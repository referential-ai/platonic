use crate::{AppError, AppResult};
use std::{path::PathBuf, process::Command};

const CHILD_CONFINEMENT_ENV: &str = "PLATONIC_CHILD_CONFINEMENT";
const CHILD_READABLE_PATHS_ENV: &str = "PLATONIC_CHILD_READABLE_PATHS";
const CHILD_WRITABLE_PATHS_ENV: &str = "PLATONIC_CHILD_WRITABLE_PATHS";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConfinementSupport {
    #[cfg(any(target_os = "linux", test))]
    Landlock,
    None,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ChildConfinement {
    None,
    Landlock {
        readable_paths: Vec<PathBuf>,
        writable_paths: Vec<PathBuf>,
        scratch: PathBuf,
    },
}

pub(crate) fn detect_support() -> ConfinementSupport {
    #[cfg(target_os = "linux")]
    if linux::ruleset().is_ok() {
        return ConfinementSupport::Landlock;
    }
    ConfinementSupport::None
}

pub(crate) fn configure_child(
    command: &mut Command,
    confinement: &ChildConfinement,
) -> AppResult<()> {
    match confinement {
        ChildConfinement::None => {
            command
                .env_remove(CHILD_CONFINEMENT_ENV)
                .env_remove(CHILD_READABLE_PATHS_ENV)
                .env_remove(CHILD_WRITABLE_PATHS_ENV);
        }
        ChildConfinement::Landlock {
            readable_paths,
            writable_paths,
            scratch,
        } => {
            let xdg_config_home = scratch.join("xdg-config");
            std::fs::create_dir_all(&xdg_config_home)?;
            let readable_paths = readable_paths
                .iter()
                .map(|path| path.to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            let writable_paths = writable_paths
                .iter()
                .map(|path| path.to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            command
                .env(CHILD_CONFINEMENT_ENV, "landlock")
                .env(
                    CHILD_READABLE_PATHS_ENV,
                    serde_json::to_string(&readable_paths)?,
                )
                .env(
                    CHILD_WRITABLE_PATHS_ENV,
                    serde_json::to_string(&writable_paths)?,
                )
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("XDG_CONFIG_HOME", xdg_config_home)
                .env("TMPDIR", scratch);
        }
    }
    Ok(())
}

pub(crate) fn apply_child() -> AppResult<()> {
    let Some(backend) = std::env::var_os(CHILD_CONFINEMENT_ENV) else {
        return Ok(());
    };
    if backend != "landlock" {
        return Err(AppError::SupervisedRun(
            "run child received an unknown confinement backend".into(),
        ));
    }
    let readable = std::env::var(CHILD_READABLE_PATHS_ENV)
        .map_err(|_| AppError::SupervisedRun("run child omitted Landlock readable paths".into()))?;
    let readable_paths = serde_json::from_str::<Vec<String>>(&readable)?;
    let writable = std::env::var(CHILD_WRITABLE_PATHS_ENV)
        .map_err(|_| AppError::SupervisedRun("run child omitted Landlock writable paths".into()))?;
    let writable_paths = serde_json::from_str::<Vec<String>>(&writable)?;
    #[cfg(target_os = "linux")]
    {
        linux::restrict(
            readable_paths.iter().map(PathBuf::from).collect(),
            writable_paths.iter().map(PathBuf::from).collect(),
        )
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (readable_paths, writable_paths);
        Err(AppError::SupervisedRun(
            "Landlock confinement is unavailable on this platform".into(),
        ))
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use landlock::{
        ABI, Access, AccessFs, BitFlags, CompatLevel, Compatible, PathBeneath, PathFd, Ruleset,
        RulesetAttr, RulesetCreated, RulesetCreatedAttr, RulesetStatus,
    };
    use std::{collections::BTreeSet, ffi::OsStr, path::Path};

    // ABI 5 is the first version that mediates device ioctls as write access.
    const ABI: ABI = ABI::V5;

    pub(super) fn ruleset() -> Result<RulesetCreated, landlock::RulesetError> {
        Ruleset::default()
            .set_compatibility(CompatLevel::HardRequirement)
            .handle_access(AccessFs::from_all(ABI))?
            .create()
    }

    pub(super) fn restrict(
        readable_paths: Vec<PathBuf>,
        writable_paths: Vec<PathBuf>,
    ) -> AppResult<()> {
        let read_access = AccessFs::from_read(ABI);
        let all_access = AccessFs::from_all(ABI);
        let mut ruleset = ruleset().map_err(landlock_error)?;
        for path in system_read_paths()
            .into_iter()
            .chain(host_toolchain_read_paths())
            .chain(readable_paths)
        {
            ruleset = add_path_rule(ruleset, &path, read_access)?;
        }
        for path in writable_paths {
            ruleset = add_path_rule(ruleset, &path, all_access)?;
        }
        ruleset = add_path_rule(ruleset, std::path::Path::new("/dev/null"), all_access)?;
        let status = ruleset.restrict_self().map_err(landlock_error)?;
        if status.ruleset != RulesetStatus::FullyEnforced || !status.no_new_privs {
            return Err(AppError::SupervisedRun(
                "Landlock did not fully enforce the child write policy".into(),
            ));
        }
        Ok(())
    }

    fn add_path_rule(
        ruleset: RulesetCreated,
        path: &std::path::Path,
        access: BitFlags<AccessFs>,
    ) -> AppResult<RulesetCreated> {
        let metadata = std::fs::metadata(path).map_err(landlock_error)?;
        let access = if metadata.is_dir() {
            access
        } else {
            access & AccessFs::from_file(ABI)
        };
        ruleset
            .add_rule(PathBeneath::new(
                PathFd::new(path).map_err(landlock_error)?,
                access,
            ))
            .map_err(landlock_error)
    }

    fn system_read_paths() -> Vec<PathBuf> {
        [
            "/usr",
            "/bin",
            "/sbin",
            "/lib",
            "/lib64",
            "/etc",
            "/nix/store",
            "/run/systemd/resolve",
            "/dev/null",
            "/dev/random",
            "/dev/urandom",
        ]
        .into_iter()
        .map(PathBuf::from)
        .filter(|path| path.exists())
        .collect()
    }

    fn host_toolchain_read_paths() -> Vec<PathBuf> {
        let path = std::env::var_os("PATH");
        let path_dirs = path
            .as_deref()
            .into_iter()
            .flat_map(std::env::split_paths)
            .filter(|path| path.is_absolute() && path.is_dir())
            .collect::<Vec<_>>();
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .and_then(|path| path.canonicalize().ok());
        let mut paths = BTreeSet::new();
        for path in &path_dirs {
            insert_existing(&mut paths, path, home.as_deref());
            if path.file_name() == Some(OsStr::new("shims")) {
                if let Some(tool_manager_root) = path.parent() {
                    insert_existing(
                        &mut paths,
                        &tool_manager_root.join("installs"),
                        home.as_deref(),
                    );
                }
            }
            if let Some(root) = managed_install_root(path) {
                insert_existing(&mut paths, root, home.as_deref());
            }
        }

        let has_cargo = path_dirs.iter().any(|path| path.join("cargo").is_file());
        if let Some(cargo_home) = std::env::var_os("CARGO_HOME")
            .map(PathBuf::from)
            .or_else(|| {
                has_cargo
                    .then(|| std::env::var_os("HOME").map(PathBuf::from))
                    .flatten()
                    .map(|home| home.join(".cargo"))
            })
        {
            for path in ["bin", "registry", "git"] {
                insert_existing(&mut paths, &cargo_home.join(path), home.as_deref());
            }
        }

        let has_rustup = has_cargo
            || path_dirs
                .iter()
                .any(|path| path.join("rustc").is_file() || path.join("rustup").is_file());
        if let Some(rustup_home) =
            std::env::var_os("RUSTUP_HOME")
                .map(PathBuf::from)
                .or_else(|| {
                    has_rustup
                        .then(|| std::env::var_os("HOME").map(PathBuf::from))
                        .flatten()
                        .map(|home| home.join(".rustup"))
                })
        {
            for path in ["settings.toml", "toolchains"] {
                insert_existing(&mut paths, &rustup_home.join(path), home.as_deref());
            }
        }
        paths.into_iter().collect()
    }

    fn managed_install_root(bin_dir: &Path) -> Option<&Path> {
        if bin_dir.file_name() != Some(OsStr::new("bin")) {
            return None;
        }
        let version = bin_dir.parent()?;
        let tool = version.parent()?;
        (tool.parent()?.file_name() == Some(OsStr::new("installs"))).then_some(version)
    }

    fn insert_existing(paths: &mut BTreeSet<PathBuf>, path: &Path, home: Option<&Path>) {
        if !path.is_absolute() {
            return;
        }
        if let Ok(path) = path.canonicalize() {
            if path == Path::new("/") || home.is_some_and(|home| home.starts_with(&path)) {
                return;
            }
            paths.insert(path);
        }
    }

    fn landlock_error(error: impl std::fmt::Display) -> AppError {
        AppError::SupervisedRun(format!("Landlock confinement failed: {error}"))
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::{fs, os::unix::fs::PermissionsExt, path::Path, process::Stdio};

    const FIXTURE_ENV: &str = "PLATONIC_LANDLOCK_TEST_FIXTURE";
    const ONE_SHOT_FIXTURE_ENV: &str = "PLATONIC_ONE_SHOT_LANDLOCK_TEST_FIXTURE";

    fn git(path: &Path, args: &[&str]) {
        let output = Command::new("git")
            .current_dir(path)
            .args(args)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn landlock_child_fixture() {
        let Some(encoded) = std::env::var_os(FIXTURE_ENV) else {
            return;
        };
        let paths: Vec<String> = serde_json::from_str(&encoded.to_string_lossy()).unwrap();
        apply_child().unwrap();
        let private = Path::new(&paths[0]);
        let scratch = Path::new(&paths[1]);
        let shared = Path::new(&paths[2]);
        let sibling = Path::new(&paths[3]);
        let other_thread = Path::new(&paths[4]);
        let outside = Path::new(&paths[5]);
        let server_db = Path::new(&paths[6]);
        let profile_home_secret = Path::new(&paths[7]);
        let raw_transcript = Path::new(&paths[8]);
        let toolchain_payload = Path::new(&paths[9]);
        let unrelated_home_secret = Path::new(&paths[10]);

        assert_eq!(
            fs::read_to_string(private.join("seed.txt")).unwrap(),
            "private\n"
        );
        fs::write(private.join("allowed.txt"), "allowed\n").unwrap();
        fs::write(scratch.join("scratch.txt"), "scratch\n").unwrap();
        git(private, &["add", "allowed.txt"]);
        git(private, &["commit", "--quiet", "-m", "confined commit"]);
        assert!(
            Command::new("git")
                .arg("--version")
                .output()
                .unwrap()
                .status
                .success()
        );
        assert!(fs::read("/etc/os-release").is_ok());
        let tool = Command::new("p578-toolchain").output().unwrap();
        assert!(
            tool.status.success(),
            "toolchain failed: {}",
            String::from_utf8_lossy(&tool.stderr)
        );
        assert_eq!(tool.stdout, b"toolchain payload\n");

        for denied in [
            shared.join("secret"),
            sibling.join("secret"),
            other_thread.join("private.txt"),
            outside.join("secret"),
            server_db.to_path_buf(),
            profile_home_secret.to_path_buf(),
            raw_transcript.to_path_buf(),
            unrelated_home_secret.to_path_buf(),
        ] {
            let error = fs::read(&denied).unwrap_err();
            assert_eq!(
                error.kind(),
                std::io::ErrorKind::PermissionDenied,
                "read {denied:?}"
            );
        }

        for denied in [
            shared.join("object"),
            sibling.join(".git/refs/heads/sibling"),
            other_thread.join("private.txt"),
            outside.join("outside.txt"),
            toolchain_payload.to_path_buf(),
        ] {
            let error = fs::write(&denied, "denied\n").unwrap_err();
            assert_eq!(
                error.kind(),
                std::io::ErrorKind::PermissionDenied,
                "{denied:?}"
            );
        }
    }

    #[test]
    fn one_shot_landlock_child_fixture() {
        let Some(encoded) = std::env::var_os(ONE_SHOT_FIXTURE_ENV) else {
            return;
        };
        let paths: Vec<String> = serde_json::from_str(&encoded.to_string_lossy()).unwrap();
        apply_child().unwrap();
        let workspace = Path::new(&paths[0]);
        let scratch = Path::new(&paths[1]);
        let sibling_workspace = Path::new(&paths[2]);
        let server_state = Path::new(&paths[3]);
        let shared_objects = Path::new(&paths[4]);
        let outside = Path::new(&paths[5]);

        assert_eq!(
            std::env::var_os("TMPDIR").as_deref(),
            Some(scratch.as_os_str())
        );
        let allowed = Command::new("sh")
            .arg("-c")
            .arg(
                "printf workspace > allowed.txt && \
                 printf scratch > \"$TMPDIR/allowed.txt\" && \
                 test -r /etc/os-release && git --version >/dev/null",
            )
            .current_dir(workspace)
            .output()
            .unwrap();
        assert!(
            allowed.status.success(),
            "allowed one-shot shell failed: {}",
            String::from_utf8_lossy(&allowed.stderr)
        );

        for denied in [
            sibling_workspace.join("secret"),
            server_state.join("server.db"),
            shared_objects.join("secret"),
            outside.join("secret"),
        ] {
            let error = fs::read(&denied).unwrap_err();
            assert_eq!(
                error.kind(),
                std::io::ErrorKind::PermissionDenied,
                "read {denied:?}"
            );
        }

        for denied in [
            sibling_workspace.join("denied.txt"),
            server_state.join("server.db"),
            shared_objects.join("denied-object"),
            outside.join("denied.txt"),
        ] {
            let shell = Command::new("sh")
                .arg("-c")
                .arg("printf denied > \"$DENIED\"")
                .env("DENIED", &denied)
                .output()
                .unwrap();
            assert!(!shell.status.success(), "shell wrote {denied:?}");
            let error = fs::write(&denied, "denied\n").unwrap_err();
            assert_eq!(
                error.kind(),
                std::io::ErrorKind::PermissionDenied,
                "{denied:?}"
            );
        }
    }

    #[test]
    fn landlock_allows_read_only_host_toolchain_but_denies_state_home_and_sibling_reads() {
        assert_eq!(detect_support(), ConfinementSupport::Landlock);
        let root = tempfile::tempdir().unwrap();
        let private = root.path().join("private");
        let scratch = root.path().join("scratch");
        let shared = root.path().join("shared-objects");
        let sibling = root.path().join("sibling");
        let other_thread = root.path().join("other-thread");
        let outside = root.path().join("outside");
        let server_state = root.path().join("server-state");
        let profile_home = root.path().join("profile-home");
        let home = root.path().join("home");
        let toolchain_shims = home.join(".local/share/mise/shims");
        let toolchain_install = home.join(".local/share/mise/installs/test/1.0");
        for path in [
            &private,
            &scratch,
            &shared,
            &sibling,
            &other_thread,
            &outside,
            &server_state,
            &profile_home,
            &toolchain_shims,
            &toolchain_install,
        ] {
            fs::create_dir_all(path).unwrap();
        }
        fs::create_dir_all(server_state.join("transcripts")).unwrap();
        fs::write(private.join("seed.txt"), "private\n").unwrap();
        fs::write(shared.join("secret"), "shared\n").unwrap();
        fs::write(sibling.join("secret"), "sibling\n").unwrap();
        fs::write(other_thread.join("private.txt"), "other\n").unwrap();
        fs::write(outside.join("secret"), "outside\n").unwrap();
        fs::write(server_state.join("server.db"), "state\n").unwrap();
        fs::write(profile_home.join("secret"), "home\n").unwrap();
        fs::write(server_state.join("transcripts/raw.jsonl"), "raw\n").unwrap();
        fs::write(home.join("unrelated-secret"), "unrelated home\n").unwrap();
        let toolchain_payload = toolchain_install.join("payload");
        fs::write(&toolchain_payload, "toolchain payload\n").unwrap();
        let toolchain = toolchain_shims.join("p578-toolchain");
        fs::write(
            &toolchain,
            "#!/bin/sh\nexec cat \"$P578_TOOLCHAIN_PAYLOAD\"\n",
        )
        .unwrap();
        let mut permissions = fs::metadata(&toolchain).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&toolchain, permissions).unwrap();
        git(&private, &["init", "--quiet", "--initial-branch", "main"]);
        git(&private, &["config", "user.name", "Platonic Test"]);
        git(
            &private,
            &["config", "user.email", "platonic@example.invalid"],
        );
        git(
            &sibling,
            &["init", "--quiet", "--initial-branch", "sibling"],
        );

        let fixture = vec![
            private.to_string_lossy().into_owned(),
            scratch.to_string_lossy().into_owned(),
            shared.to_string_lossy().into_owned(),
            sibling.to_string_lossy().into_owned(),
            other_thread.to_string_lossy().into_owned(),
            outside.to_string_lossy().into_owned(),
            server_state
                .join("server.db")
                .to_string_lossy()
                .into_owned(),
            profile_home.join("secret").to_string_lossy().into_owned(),
            server_state
                .join("transcripts/raw.jsonl")
                .to_string_lossy()
                .into_owned(),
            toolchain_payload.to_string_lossy().into_owned(),
            home.join("unrelated-secret").to_string_lossy().into_owned(),
        ];
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .arg("--exact")
            .arg("confinement::tests::landlock_child_fixture")
            .arg("--nocapture")
            .env_clear()
            .env(
                "PATH",
                std::env::join_paths([
                    home.as_path(),
                    toolchain_shims.as_path(),
                    Path::new("/usr/bin"),
                    Path::new("/bin"),
                ])
                .unwrap(),
            )
            .env("HOME", &home)
            .env("P578_TOOLCHAIN_PAYLOAD", &toolchain_payload)
            .env(FIXTURE_ENV, serde_json::to_string(&fixture).unwrap())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        configure_child(
            &mut command,
            &ChildConfinement::Landlock {
                readable_paths: vec![private.clone(), scratch.clone()],
                writable_paths: vec![private.clone(), scratch.clone()],
                scratch,
            },
        )
        .unwrap();
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "Landlock fixture failed\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(private.join("allowed.txt").is_file());
        assert!(root.path().join("scratch/scratch.txt").is_file());
        assert!(!shared.join("object").exists());
        assert_eq!(
            fs::read_to_string(other_thread.join("private.txt")).unwrap(),
            "other\n"
        );
        assert_eq!(
            fs::read_to_string(toolchain_payload).unwrap(),
            "toolchain payload\n"
        );
        assert!(!outside.join("outside.txt").exists());
    }

    #[test]
    fn landlock_one_shot_shell_inherits_workspace_and_tmpdir_only_write_policy() {
        assert_eq!(detect_support(), ConfinementSupport::Landlock);
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        let sibling_workspace = root.path().join("sibling-workspace");
        let server_state = root.path().join("server-state");
        let scratch = server_state.join("one-shot-runs/run_test/scratch");
        let shared_objects = server_state.join("git/repo.git/objects");
        let outside = root.path().join("outside");
        for path in [
            &workspace,
            &sibling_workspace,
            &scratch,
            &shared_objects,
            &outside,
        ] {
            fs::create_dir_all(path).unwrap();
        }
        fs::write(server_state.join("server.db"), "server state\n").unwrap();
        fs::write(sibling_workspace.join("secret"), "sibling\n").unwrap();
        fs::write(shared_objects.join("secret"), "shared\n").unwrap();
        fs::write(outside.join("secret"), "outside\n").unwrap();

        let fixture = vec![
            workspace.to_string_lossy().into_owned(),
            scratch.to_string_lossy().into_owned(),
            sibling_workspace.to_string_lossy().into_owned(),
            server_state.to_string_lossy().into_owned(),
            shared_objects.to_string_lossy().into_owned(),
            outside.to_string_lossy().into_owned(),
        ];
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .arg("--exact")
            .arg("confinement::tests::one_shot_landlock_child_fixture")
            .arg("--nocapture")
            .env(
                ONE_SHOT_FIXTURE_ENV,
                serde_json::to_string(&fixture).unwrap(),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        configure_child(
            &mut command,
            &ChildConfinement::Landlock {
                readable_paths: vec![workspace.clone(), scratch.clone()],
                writable_paths: vec![workspace.clone(), scratch.clone()],
                scratch: scratch.clone(),
            },
        )
        .unwrap();
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "one-shot Landlock fixture failed\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            fs::read_to_string(workspace.join("allowed.txt")).unwrap(),
            "workspace"
        );
        assert_eq!(
            fs::read_to_string(scratch.join("allowed.txt")).unwrap(),
            "scratch"
        );
        assert_eq!(
            fs::read_to_string(server_state.join("server.db")).unwrap(),
            "server state\n"
        );
        assert!(!sibling_workspace.join("denied.txt").exists());
        assert!(!shared_objects.join("denied-object").exists());
        assert!(!outside.join("denied.txt").exists());
    }
}
