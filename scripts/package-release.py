#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import io
import json
import os
import re
import subprocess
import tarfile
from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn


REPO_ROOT = Path(__file__).resolve().parent.parent
COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")


@dataclass(frozen=True)
class ReleaseContract:
    product_version: str
    targets: tuple[str, ...]
    binaries: tuple[str, ...]
    client_version: str


def fail(message: str) -> NoReturn:
    raise SystemExit(f"error: {message}")


def cargo_metadata() -> dict[str, object]:
    result = subprocess.run(
        [
            "cargo",
            "metadata",
            "--manifest-path",
            str(REPO_ROOT / "Cargo.toml"),
            "--locked",
            "--no-deps",
            "--format-version",
            "1",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    value = json.loads(result.stdout)
    if not isinstance(value, dict):
        fail("cargo metadata did not return an object")
    return value


def release_contract(metadata: dict[str, object]) -> ReleaseContract:
    workspace_metadata = metadata.get("metadata")
    packages = metadata.get("packages")
    if not isinstance(workspace_metadata, dict) or not isinstance(packages, list):
        fail("cargo metadata omitted workspace metadata or packages")
    release = workspace_metadata.get("platonic-release")
    if not isinstance(release, dict):
        fail("cargo metadata omitted workspace.metadata.platonic-release")

    product_version = release.get("product-version")
    targets = release.get("bundle-targets")
    binaries = release.get("bundle-binaries")
    if not isinstance(product_version, str):
        fail("product-version must be a string")
    if not isinstance(targets, list) or not all(isinstance(item, str) for item in targets):
        fail("bundle-targets must be a string array")
    if not isinstance(binaries, list) or not all(isinstance(item, str) for item in binaries):
        fail("bundle-binaries must be a string array")

    client_versions = {
        package.get("version")
        for package in packages
        if isinstance(package, dict) and package.get("name") == "plato-agent"
    }
    if len(client_versions) != 1:
        fail("cargo metadata must contain one plato-agent package version")
    client_version = client_versions.pop()
    if not isinstance(client_version, str):
        fail("plato-agent package version must be a string")

    return ReleaseContract(
        product_version=product_version,
        targets=tuple(targets),
        binaries=tuple(binaries),
        client_version=client_version,
    )


def validate_inputs(
    contract: ReleaseContract, target: str, source_commit: str, build_date: str
) -> None:
    if target not in contract.targets:
        fail(f"unsupported release target: {target}")
    if COMMIT_PATTERN.fullmatch(source_commit) is None:
        fail(f"source commit must be a full lowercase Git object id: {source_commit}")
    try:
        parsed_date = dt.date.fromisoformat(build_date)
    except ValueError:
        fail(f"build date must be a UTC calendar date in YYYY-MM-DD form: {build_date}")
    if parsed_date.isoformat() != build_date:
        fail(f"build date must be a UTC calendar date in YYYY-MM-DD form: {build_date}")

    head = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "rev-parse", "--verify", "HEAD^{commit}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if head != source_commit:
        fail(f"source commit does not match HEAD (source={source_commit} HEAD={head})")


def validate_binary(
    path: Path,
    name: str,
    contract: ReleaseContract,
    source_commit: str,
    build_date: str,
) -> None:
    if not path.is_file() or path.is_symlink():
        fail(f"release binary is not a regular file: {path}")
    if not os.access(path, os.X_OK):
        fail(f"release binary is not executable: {path}")
    if name == "platonic":
        expected = f"platonic {contract.product_version} ({source_commit}, {build_date})"
    else:
        expected = f"{name} {contract.client_version} {source_commit} {build_date}"
    result = subprocess.run([str(path), "--version"], capture_output=True, text=True)
    actual = result.stdout.strip()
    if result.returncode != 0 or actual != expected or result.stderr:
        fail(f"{name} provenance mismatch (expected={expected!r} actual={actual!r})")


def git_source_epoch(source_commit: str) -> int:
    result = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "show", "-s", "--format=%ct", source_commit],
        check=True,
        capture_output=True,
        text=True,
    )
    return int(result.stdout.strip())


def add_directory(archive: tarfile.TarFile, name: str, source_epoch: int) -> None:
    info = tarfile.TarInfo(name.rstrip("/") + "/")
    info.type = tarfile.DIRTYPE
    info.mode = 0o755
    info.mtime = source_epoch
    info.uid = 0
    info.gid = 0
    archive.addfile(info)


def add_file(
    archive: tarfile.TarFile, name: str, source: Path, mode: int, source_epoch: int
) -> None:
    data = source.read_bytes()
    info = tarfile.TarInfo(name)
    info.size = len(data)
    info.mode = mode
    info.mtime = source_epoch
    info.uid = 0
    info.gid = 0
    archive.addfile(info, io.BytesIO(data))


def write_bundle(
    contract: ReleaseContract,
    target: str,
    binary_dir: Path,
    output_dir: Path,
    source_commit: str,
    build_date: str,
) -> tuple[Path, Path, Path]:
    files: dict[str, tuple[Path, int]] = {
        "CHANGELOG.md": (REPO_ROOT / "CHANGELOG.md", 0o644),
        "LICENSE-APACHE": (REPO_ROOT / "LICENSE-APACHE", 0o644),
        "LICENSE-MIT": (REPO_ROOT / "LICENSE-MIT", 0o644),
    }
    for name in contract.binaries:
        binary = binary_dir / name
        validate_binary(binary, name, contract, source_commit, build_date)
        files[f"bin/{name}"] = (binary, 0o755)

    ordered_files = sorted(files)
    bundle_name = f"platonic-{contract.product_version}-{target}"
    output_dir.mkdir(parents=True, exist_ok=True)
    archive_path = output_dir / f"{bundle_name}.tar.gz"
    files_path = output_dir / f"{bundle_name}.files"
    checksum_path = output_dir / f"{bundle_name}.sha256"
    source_epoch = git_source_epoch(source_commit)

    with archive_path.open("wb") as raw_archive:
        with gzip.GzipFile(
            filename="", fileobj=raw_archive, mode="wb", compresslevel=9, mtime=source_epoch
        ) as compressed:
            with tarfile.open(
                fileobj=compressed, mode="w", format=tarfile.USTAR_FORMAT
            ) as archive:
                add_directory(archive, bundle_name, source_epoch)
                add_directory(archive, f"{bundle_name}/bin", source_epoch)
                for relative_path in ordered_files:
                    source, mode = files[relative_path]
                    add_file(
                        archive,
                        f"{bundle_name}/{relative_path}",
                        source,
                        mode,
                        source_epoch,
                    )

    files_path.write_text("".join(f"{name}\n" for name in ordered_files), encoding="utf-8")
    digest = hashlib.sha256(archive_path.read_bytes()).hexdigest()
    checksum_path.write_text(f"{digest}  {archive_path.name}\n", encoding="ascii")
    return archive_path, files_path, checksum_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build one locked Platonic command bundle")
    parser.add_argument("--target", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--build-date", required=True)
    parser.add_argument("--binary-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    contract = release_contract(cargo_metadata())
    validate_inputs(contract, args.target, args.source_commit, args.build_date)
    outputs = write_bundle(
        contract,
        args.target,
        args.binary_dir.resolve(),
        args.output_dir.resolve(),
        args.source_commit,
        args.build_date,
    )
    for output in outputs:
        print(output)


if __name__ == "__main__":
    main()
