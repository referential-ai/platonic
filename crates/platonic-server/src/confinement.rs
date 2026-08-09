use crate::{AppError, AppResult};
use std::{path::PathBuf, process::Command};

const CHILD_CONFINEMENT_ENV: &str = "PLATONIC_CHILD_CONFINEMENT";
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
                .env_remove(CHILD_WRITABLE_PATHS_ENV);
        }
        ChildConfinement::Landlock {
            writable_paths,
            scratch,
        } => {
            let writable_paths = writable_paths
                .iter()
                .map(|path| path.to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            command
                .env(CHILD_CONFINEMENT_ENV, "landlock")
                .env(
                    CHILD_WRITABLE_PATHS_ENV,
                    serde_json::to_string(&writable_paths)?,
                )
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
    let encoded = std::env::var(CHILD_WRITABLE_PATHS_ENV)
        .map_err(|_| AppError::SupervisedRun("run child omitted Landlock writable paths".into()))?;
    let writable_paths = serde_json::from_str::<Vec<String>>(&encoded)?;
    #[cfg(target_os = "linux")]
    {
        linux::restrict(writable_paths.iter().map(PathBuf::from).collect())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = writable_paths;
        Err(AppError::SupervisedRun(
            "Landlock confinement is unavailable on this platform".into(),
        ))
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use landlock::{
        ABI, AccessFs, CompatLevel, Compatible, PathBeneath, PathFd, Ruleset, RulesetAttr,
        RulesetCreated, RulesetCreatedAttr, RulesetStatus,
    };

    // ABI 5 is the first version that mediates device ioctls as write access.
    const ABI: ABI = ABI::V5;

    pub(super) fn ruleset() -> Result<RulesetCreated, landlock::RulesetError> {
        Ruleset::default()
            .set_compatibility(CompatLevel::HardRequirement)
            .handle_access(AccessFs::from_write(ABI))?
            .create()
    }

    pub(super) fn restrict(writable_paths: Vec<PathBuf>) -> AppResult<()> {
        let directory_access = AccessFs::from_write(ABI);
        let file_access = directory_access & AccessFs::from_file(ABI);
        let mut ruleset = ruleset().map_err(landlock_error)?;
        for path in writable_paths {
            ruleset = ruleset
                .add_rule(PathBeneath::new(
                    PathFd::new(&path).map_err(landlock_error)?,
                    directory_access,
                ))
                .map_err(landlock_error)?;
        }
        // Git requires an O_RDWR descriptor for this non-persistent pseudo-device.
        ruleset = ruleset
            .add_rule(PathBeneath::new(
                PathFd::new("/dev/null").map_err(landlock_error)?,
                file_access,
            ))
            .map_err(landlock_error)?;
        let status = ruleset.restrict_self().map_err(landlock_error)?;
        if status.ruleset != RulesetStatus::FullyEnforced || !status.no_new_privs {
            return Err(AppError::SupervisedRun(
                "Landlock did not fully enforce the child write policy".into(),
            ));
        }
        Ok(())
    }

    fn landlock_error(error: impl std::fmt::Display) -> AppError {
        AppError::SupervisedRun(format!("Landlock confinement failed: {error}"))
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::{fs, path::Path, process::Stdio};

    const FIXTURE_ENV: &str = "PLATONIC_LANDLOCK_TEST_FIXTURE";

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

        for denied in [
            shared.join("object"),
            sibling.join(".git/refs/heads/sibling"),
            other_thread.join("private.txt"),
            outside.join("outside.txt"),
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
    fn landlock_allows_private_git_and_scratch_but_denies_every_other_write_root() {
        assert_eq!(detect_support(), ConfinementSupport::Landlock);
        let root = tempfile::tempdir().unwrap();
        let private = root.path().join("private");
        let scratch = root.path().join("scratch");
        let shared = root.path().join("shared-objects");
        let sibling = root.path().join("sibling");
        let other_thread = root.path().join("other-thread");
        let outside = root.path().join("outside");
        for path in [
            &private,
            &scratch,
            &shared,
            &sibling,
            &other_thread,
            &outside,
        ] {
            fs::create_dir_all(path).unwrap();
        }
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
        ];
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .arg("--exact")
            .arg("confinement::tests::landlock_child_fixture")
            .arg("--nocapture")
            .env(FIXTURE_ENV, serde_json::to_string(&fixture).unwrap())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        configure_child(
            &mut command,
            &ChildConfinement::Landlock {
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
        assert!(!other_thread.join("private.txt").exists());
        assert!(!outside.join("outside.txt").exists());
    }
}
