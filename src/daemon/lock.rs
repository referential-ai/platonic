use crate::{AppError, AppResult, paths};
pub use platonic_client::lock::{LOCK_VERSION, LockMetadata};
#[cfg(unix)]
use std::io::{Error, Read};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::{
    fs::{self, File},
    io::{ErrorKind, Seek, Write},
    path::{Path, PathBuf},
};

#[cfg(unix)]
const LOCK_FILE_MODE: u32 = 0o600;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LockConflict {
    pub path: PathBuf,
    pub metadata: Option<LockMetadata>,
    pub metadata_error: Option<String>,
}

impl LockConflict {
    pub fn owner_summary(&self) -> String {
        if let Some(metadata) = &self.metadata {
            metadata.owner_summary()
        } else if let Some(error) = &self.metadata_error {
            format!("metadata unreadable: {error}")
        } else {
            "metadata missing".into()
        }
    }
}

#[derive(Debug)]
pub struct WorkspaceLock {
    _file: File,
}

impl WorkspaceLock {
    pub fn acquire(path: PathBuf, metadata: LockMetadata) -> Result<Self, Box<LockConflict>> {
        if let Some(parent) = path.parent()
            && let Err(error) = fs::create_dir_all(parent)
        {
            return Err(Box::new(LockConflict {
                path,
                metadata: None,
                metadata_error: Some(error.to_string()),
            }));
        }

        #[cfg(unix)]
        let mut file = prepare_unix_lock_file(&path)?;
        #[cfg(windows)]
        let mut file = match create_lock_file(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                return Err(Box::new(read_conflict(path)));
            }
            Err(error) => {
                return Err(Box::new(LockConflict {
                    path,
                    metadata: None,
                    metadata_error: Some(error.to_string()),
                }));
            }
        };

        if let Err(error) = file
            .set_len(0)
            .and_then(|()| file.rewind())
            .map_err(serde_json::Error::io)
            .and_then(|()| serde_json::to_writer(&mut file, &metadata))
            .and_then(|()| file.write_all(b"\n").map_err(serde_json::Error::io))
        {
            drop(file);
            return Err(Box::new(LockConflict {
                path,
                metadata: None,
                metadata_error: Some(error.to_string()),
            }));
        }

        #[cfg(unix)]
        if let Err(error) =
            validate_unix_lock_file(&path, &file, rustix::process::geteuid().as_raw())
        {
            return Err(Box::new(LockConflict {
                path,
                metadata: None,
                metadata_error: Some(error.to_string()),
            }));
        }

        Ok(Self { _file: file })
    }

    pub fn acquire_for_workspace(workspace_root: &Path, socket_path: &Path) -> AppResult<Self> {
        let lock_path = paths::default_lock_path(workspace_root)?;
        let metadata = LockMetadata::for_workspace(workspace_root, socket_path)?;
        Self::acquire(lock_path, metadata).map_err(|conflict| lock_conflict_error(*conflict))
    }

    pub fn acquire_for_host(lock_path: PathBuf, socket_path: &Path) -> AppResult<Self> {
        let metadata = LockMetadata {
            v: LOCK_VERSION,
            pid: std::process::id(),
            executable: std::env::current_exe()
                .ok()
                .map(|path| path.to_string_lossy().into_owned()),
            workspace_root: "host".into(),
            workspace_id: "host".into(),
            socket_path: socket_path.to_string_lossy().into_owned(),
        };
        Self::acquire(lock_path, metadata).map_err(|conflict| lock_conflict_error(*conflict))
    }
}

#[cfg(unix)]
fn prepare_unix_lock_file(path: &Path) -> Result<File, Box<LockConflict>> {
    let mut file = open_unix_lock_file(path, true).map_err(|error| {
        Box::new(LockConflict {
            path: path.to_path_buf(),
            metadata: None,
            metadata_error: Some(error.to_string()),
        })
    })?;
    let expected_uid = rustix::process::geteuid().as_raw();
    validate_unix_lock_file(path, &file, expected_uid).map_err(|error| {
        Box::new(LockConflict {
            path: path.to_path_buf(),
            metadata: None,
            metadata_error: Some(error.to_string()),
        })
    })?;

    match lock_unix_file(&file) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::WouldBlock => {
            return Err(Box::new(read_conflict_from_file(
                path.to_path_buf(),
                &mut file,
            )));
        }
        Err(error) => {
            return Err(Box::new(LockConflict {
                path: path.to_path_buf(),
                metadata: None,
                metadata_error: Some(format!("advisory lock unavailable: {error}")),
            }));
        }
    }

    validate_unix_lock_file(path, &file, expected_uid).map_err(|error| {
        Box::new(LockConflict {
            path: path.to_path_buf(),
            metadata: None,
            metadata_error: Some(error.to_string()),
        })
    })?;
    Ok(file)
}

#[cfg(unix)]
fn open_unix_lock_file(path: &Path, create: bool) -> std::io::Result<File> {
    use rustix::fs::{Mode, OFlags};

    let mut flags = OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK;
    if create {
        flags |= OFlags::CREATE;
    }
    rustix::fs::open(path, flags, Mode::RUSR | Mode::WUSR)
        .map(File::from)
        .map_err(Into::into)
}

#[cfg(unix)]
fn validate_unix_lock_file(path: &Path, file: &File, expected_uid: u32) -> std::io::Result<()> {
    let file_metadata = file.metadata()?;
    if !file_metadata.file_type().is_file() {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            format!("lock path is not a regular file: {}", path.display()),
        ));
    }
    if file_metadata.uid() != expected_uid {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            format!(
                "lock file {} is owned by uid {}, expected {expected_uid}",
                path.display(),
                file_metadata.uid()
            ),
        ));
    }
    let mode = file_metadata.mode() & 0o7777;
    if mode != LOCK_FILE_MODE {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            format!(
                "lock file {} has mode {mode:04o}, expected {LOCK_FILE_MODE:04o}",
                path.display()
            ),
        ));
    }

    let path_metadata = fs::symlink_metadata(path)?;
    if path_metadata.file_type().is_symlink()
        || path_metadata.dev() != file_metadata.dev()
        || path_metadata.ino() != file_metadata.ino()
    {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            format!("lock path changed while being opened: {}", path.display()),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn lock_unix_file(file: &File) -> std::io::Result<()> {
    rustix::fs::flock(file, rustix::fs::FlockOperation::NonBlockingLockExclusive)
        .map_err(Into::into)
}

#[cfg(windows)]
fn create_lock_file(path: &Path) -> std::io::Result<File> {
    crate::windows_security::create_current_user_file(path)
}

#[cfg(unix)]
pub fn ensure_workspace_unlocked(workspace_root: &Path) -> AppResult<()> {
    let lock_path = paths::default_lock_path(workspace_root)?;
    ensure_unix_lock_unheld(lock_path)
}

#[cfg(windows)]
pub fn ensure_workspace_unlocked(workspace_root: &Path) -> AppResult<()> {
    let lock_path = paths::default_lock_path(workspace_root)?;
    if lock_path.exists() {
        return Err(lock_conflict_error(read_conflict(lock_path)));
    }
    Ok(())
}

#[cfg(unix)]
fn ensure_unix_lock_unheld(lock_path: PathBuf) -> AppResult<()> {
    let mut file = match open_unix_lock_file(&lock_path, false) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(lock_conflict_error(LockConflict {
                path: lock_path,
                metadata: None,
                metadata_error: Some(error.to_string()),
            }));
        }
    };
    if let Err(error) =
        validate_unix_lock_file(&lock_path, &file, rustix::process::geteuid().as_raw())
    {
        return Err(lock_conflict_error(LockConflict {
            path: lock_path,
            metadata: None,
            metadata_error: Some(error.to_string()),
        }));
    }

    match lock_unix_file(&file) {
        Ok(()) => {
            if let Err(error) =
                validate_unix_lock_file(&lock_path, &file, rustix::process::geteuid().as_raw())
            {
                return Err(lock_conflict_error(LockConflict {
                    path: lock_path,
                    metadata: None,
                    metadata_error: Some(error.to_string()),
                }));
            }
            Ok(())
        }
        Err(error) if error.kind() == ErrorKind::WouldBlock => Err(lock_conflict_error(
            read_conflict_from_file(lock_path, &mut file),
        )),
        Err(error) => Err(lock_conflict_error(LockConflict {
            path: lock_path,
            metadata: None,
            metadata_error: Some(format!("advisory lock unavailable: {error}")),
        })),
    }
}

#[cfg(windows)]
fn read_conflict(path: PathBuf) -> LockConflict {
    match fs::read_to_string(&path) {
        Ok(raw) => match serde_json::from_str::<LockMetadata>(raw.trim()) {
            Ok(metadata) => LockConflict {
                path,
                metadata: Some(metadata),
                metadata_error: None,
            },
            Err(error) => LockConflict {
                path,
                metadata: None,
                metadata_error: Some(error.to_string()),
            },
        },
        Err(error) => LockConflict {
            path,
            metadata: None,
            metadata_error: Some(error.to_string()),
        },
    }
}

#[cfg(unix)]
fn read_conflict_from_file(path: PathBuf, file: &mut File) -> LockConflict {
    let mut raw = String::new();
    let result = file.rewind().and_then(|()| file.read_to_string(&mut raw));
    match result {
        Ok(_) => match serde_json::from_str::<LockMetadata>(raw.trim()) {
            Ok(metadata) => LockConflict {
                path,
                metadata: Some(metadata),
                metadata_error: None,
            },
            Err(error) => LockConflict {
                path,
                metadata: None,
                metadata_error: Some(error.to_string()),
            },
        },
        Err(error) => LockConflict {
            path,
            metadata: None,
            metadata_error: Some(error.to_string()),
        },
    }
}

fn lock_conflict_error(conflict: LockConflict) -> AppError {
    let owner = conflict.owner_summary();
    AppError::DaemonLockHeld {
        path: conflict.path,
        owner,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::fs::{PermissionsExt, symlink};

    #[test]
    fn lock_conflict_reports_owner_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("agent.lock");
        let workspace = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("agent.sock");
        let metadata = LockMetadata::for_workspace(workspace.path(), &socket_path).unwrap();
        let _lock = WorkspaceLock::acquire(lock_path.clone(), metadata.clone()).unwrap();

        let conflict =
            WorkspaceLock::acquire(lock_path, metadata).expect_err("second lock must conflict");

        assert!(conflict.owner_summary().contains("pid="));
        assert!(conflict.owner_summary().contains("workspace_id="));
        assert!(conflict.owner_summary().contains("socket_path="));
    }

    #[test]
    fn dropping_lock_releases_ownership() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("agent.lock");
        let workspace = tempfile::tempdir().unwrap();
        let metadata =
            LockMetadata::for_workspace(workspace.path(), &dir.path().join("agent.sock")).unwrap();

        {
            let _lock = WorkspaceLock::acquire(lock_path.clone(), metadata.clone()).unwrap();
            assert!(lock_path.exists());
            #[cfg(windows)]
            assert!(
                fs::remove_file(&lock_path).is_err(),
                "a live daemon lock must not be replaceable"
            );
        }

        #[cfg(unix)]
        {
            assert!(lock_path.exists());
            drop(WorkspaceLock::acquire(lock_path, metadata).unwrap());
        }
        #[cfg(windows)]
        assert!(!lock_path.exists());
    }

    #[cfg(windows)]
    #[test]
    fn lock_conflict_reports_unreadable_metadata_without_stealing() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("agent.lock");
        std::fs::write(&lock_path, "not json").unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let metadata =
            LockMetadata::for_workspace(workspace.path(), &dir.path().join("agent.sock")).unwrap();

        let conflict = WorkspaceLock::acquire(lock_path.clone(), metadata)
            .expect_err("corrupt existing lock still conflicts");

        assert!(conflict.owner_summary().contains("metadata unreadable"));
        assert_eq!(std::fs::read_to_string(lock_path).unwrap(), "not json");
    }

    #[cfg(unix)]
    #[test]
    fn unlocked_stale_metadata_is_rewritten_in_place() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("agent.lock");
        fs::write(&lock_path, "not json").unwrap();
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(LOCK_FILE_MODE)).unwrap();
        let before = fs::symlink_metadata(&lock_path).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let metadata =
            LockMetadata::for_workspace(workspace.path(), &dir.path().join("agent.sock")).unwrap();

        drop(WorkspaceLock::acquire(lock_path.clone(), metadata.clone()).unwrap());

        let after = fs::symlink_metadata(&lock_path).unwrap();
        assert_eq!((before.dev(), before.ino()), (after.dev(), after.ino()));
        let stored: LockMetadata =
            serde_json::from_str(fs::read_to_string(lock_path).unwrap().trim()).unwrap();
        assert_eq!(stored, metadata);
    }

    #[cfg(unix)]
    #[test]
    fn kernel_probe_distinguishes_a_live_lock_from_a_persistent_path() {
        let workspace = tempfile::tempdir().unwrap();
        paths::with_test_xdg(workspace.path(), || {
            let socket_path = workspace.path().join("agent.sock");
            let lock =
                WorkspaceLock::acquire_for_workspace(workspace.path(), &socket_path).unwrap();
            let lock_path = paths::default_lock_path(workspace.path()).unwrap();

            assert!(matches!(
                ensure_workspace_unlocked(workspace.path()),
                Err(AppError::DaemonLockHeld { .. })
            ));
            drop(lock);
            assert!(lock_path.exists());
            ensure_workspace_unlocked(workspace.path()).unwrap();
        });
    }

    #[cfg(unix)]
    #[test]
    fn symlink_lock_path_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        let lock_path = dir.path().join("agent.lock");
        fs::write(&target, "target contents").unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(LOCK_FILE_MODE)).unwrap();
        symlink(&target, &lock_path).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let metadata =
            LockMetadata::for_workspace(workspace.path(), &dir.path().join("agent.sock")).unwrap();

        let conflict = WorkspaceLock::acquire(lock_path, metadata)
            .expect_err("a symlink lock path must fail closed");

        assert!(conflict.metadata_error.is_some());
        assert_eq!(fs::read_to_string(target).unwrap(), "target contents");
    }

    #[cfg(unix)]
    #[test]
    fn non_regular_lock_path_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("agent.lock");
        let _listener = std::os::unix::net::UnixListener::bind(&lock_path).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let metadata =
            LockMetadata::for_workspace(workspace.path(), &dir.path().join("agent.sock")).unwrap();

        let conflict = WorkspaceLock::acquire(lock_path.clone(), metadata)
            .expect_err("a non-regular lock path must fail closed");

        assert!(conflict.metadata_error.is_some());
        assert!(
            !fs::symlink_metadata(lock_path)
                .unwrap()
                .file_type()
                .is_file()
        );
    }

    #[cfg(unix)]
    #[test]
    fn unsafe_lock_mode_fails_closed_without_rewriting() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("agent.lock");
        fs::write(&lock_path, "unsafe contents").unwrap();
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o644)).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let metadata =
            LockMetadata::for_workspace(workspace.path(), &dir.path().join("agent.sock")).unwrap();

        let conflict = WorkspaceLock::acquire(lock_path.clone(), metadata)
            .expect_err("an unsafe lock mode must fail closed");

        assert!(conflict.owner_summary().contains("mode 0644"));
        assert_eq!(fs::read_to_string(lock_path).unwrap(), "unsafe contents");
    }

    #[cfg(unix)]
    #[test]
    fn foreign_owner_validation_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("agent.lock");
        fs::write(&lock_path, "").unwrap();
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(LOCK_FILE_MODE)).unwrap();
        let file = open_unix_lock_file(&lock_path, false).unwrap();
        let actual_uid = rustix::process::geteuid().as_raw();
        let different_uid = actual_uid.checked_add(1).unwrap_or(actual_uid - 1);

        let error = validate_unix_lock_file(&lock_path, &file, different_uid)
            .expect_err("a foreign owner must fail closed");

        assert!(error.to_string().contains("owned by uid"));
    }

    #[cfg(unix)]
    #[test]
    fn replaced_lock_path_fails_identity_validation() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("agent.lock");
        let moved_path = dir.path().join("moved.lock");
        fs::write(&lock_path, "").unwrap();
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(LOCK_FILE_MODE)).unwrap();
        let file = open_unix_lock_file(&lock_path, false).unwrap();
        fs::rename(&lock_path, moved_path).unwrap();
        fs::write(&lock_path, "").unwrap();
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(LOCK_FILE_MODE)).unwrap();

        let error = validate_unix_lock_file(&lock_path, &file, rustix::process::geteuid().as_raw())
            .expect_err("a replaced path must fail identity validation");

        assert!(error.to_string().contains("changed while being opened"));
    }
}
