use std::{env, fs, path::PathBuf};

fn main() {
    let manifest_path = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap())
        .join("../..")
        .join("Cargo.toml");
    println!("cargo:rerun-if-changed={}", manifest_path.display());
    println!("cargo:rerun-if-env-changed=PLATONIC_BUILD_COMMIT");
    println!("cargo:rerun-if-env-changed=PLATONIC_BUILD_DATE");

    let manifest = fs::read_to_string(&manifest_path).expect("read workspace Cargo.toml");
    let manifest: toml::Value = toml::from_str(&manifest).expect("parse workspace Cargo.toml");
    let product_version = manifest
        .get("workspace")
        .and_then(|workspace| workspace.get("metadata"))
        .and_then(|metadata| metadata.get("platonic-release"))
        .and_then(|release| release.get("product-version"))
        .and_then(toml::Value::as_str)
        .expect("workspace.metadata.platonic-release.product-version must be a string");

    let commit = env::var("PLATONIC_BUILD_COMMIT").unwrap_or_else(|_| "unknown".into());
    let date = env::var("PLATONIC_BUILD_DATE").unwrap_or_else(|_| "unknown".into());
    let provenance_is_complete = commit != "unknown" && date != "unknown";
    let provenance_is_absent = commit == "unknown" && date == "unknown";
    assert!(
        provenance_is_complete || provenance_is_absent,
        "PLATONIC_BUILD_COMMIT and PLATONIC_BUILD_DATE must be set together"
    );
    if provenance_is_complete {
        assert!(
            commit.len() == 40
                && commit
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "PLATONIC_BUILD_COMMIT must be a full lowercase Git commit"
        );
        assert!(
            valid_utc_date(&date),
            "PLATONIC_BUILD_DATE must be YYYY-MM-DD"
        );
    }

    let build_identity = format!("{product_version} ({commit}, {date})");
    let diagnostic_identity = format!("platonic {build_identity}");
    println!("cargo:rustc-env=PLATONIC_PRODUCT_VERSION={product_version}");
    println!("cargo:rustc-env=PLATONIC_BUILD_COMMIT={commit}");
    println!("cargo:rustc-env=PLATONIC_BUILD_DATE={date}");
    println!("cargo:rustc-env=PLATONIC_BUILD_IDENTITY={build_identity}");
    println!("cargo:rustc-env=PLATONIC_DIAGNOSTIC_IDENTITY={diagnostic_identity}");
}

fn valid_utc_date(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 10
        && bytes[0..4].iter().all(u8::is_ascii_digit)
        && bytes[4] == b'-'
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[7] == b'-'
        && bytes[8..10].iter().all(u8::is_ascii_digit)
}
