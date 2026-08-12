use crate::{AppResult, daemon::transport};
#[cfg(unix)]
use std::path::Path;
use std::path::PathBuf;

#[cfg(unix)]
use std::{
    fs::{self, DirBuilder, Permissions},
    io::{Error, ErrorKind},
    os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, PermissionsExt},
};

#[cfg(unix)]
pub(super) const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
#[cfg(unix)]
pub(super) const SOCKET_MODE: u32 = 0o600;

#[derive(Debug)]
pub(super) struct BoundEndpoint {
    pub(super) listener: transport::Listener,
    pub(super) socket_path: PathBuf,
    #[cfg(unix)]
    pub(super) socket_device: u64,
    #[cfg(unix)]
    pub(super) socket_inode: u64,
}

impl BoundEndpoint {
    pub(super) fn bind(socket_path: PathBuf, reclaim_default_socket: bool) -> AppResult<Self> {
        #[cfg(unix)]
        prepare_socket_for_bind(&socket_path, reclaim_default_socket)?;
        let listener = transport::bind(&socket_path)?;
        #[cfg(unix)]
        let (socket_device, socket_inode) = bound_socket_identity(&socket_path)?;
        #[cfg(unix)]
        if let Err(error) = restrict_socket(&socket_path) {
            drop(listener);
            let _ = remove_socket_if_matches(&socket_path, socket_device, socket_inode);
            return Err(error.into());
        }
        Ok(Self {
            listener,
            socket_path,
            #[cfg(unix)]
            socket_device,
            #[cfg(unix)]
            socket_inode,
        })
    }
}

impl Drop for BoundEndpoint {
    fn drop(&mut self) {
        #[cfg(unix)]
        let _ = remove_socket_if_matches(&self.socket_path, self.socket_device, self.socket_inode);
    }
}

#[cfg(unix)]
fn prepare_socket_for_bind(path: &Path, reclaim_default_socket: bool) -> std::io::Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    let path_in_use = || {
        Error::new(
            ErrorKind::AddrInUse,
            format!("daemon socket path already exists: {}", path.display()),
        )
    };
    if !reclaim_default_socket
        || !metadata.file_type().is_socket()
        || metadata.uid() != rustix::process::geteuid().as_raw()
    {
        return Err(path_in_use());
    }

    remove_socket_if_matches(path, metadata.dev(), metadata.ino())?;
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(path_in_use()),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn bound_socket_identity(path: &Path) -> std::io::Result<(u64, u64)> {
    let metadata = fs::symlink_metadata(path)?;
    let expected_uid = rustix::process::geteuid().as_raw();
    if !metadata.file_type().is_socket() || metadata.uid() != expected_uid {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            format!(
                "daemon socket path is not a current-user socket: {}",
                path.display()
            ),
        ));
    }
    Ok((metadata.dev(), metadata.ino()))
}

#[cfg(unix)]
fn remove_socket_if_matches(
    path: &Path,
    expected_device: u64,
    expected_inode: u64,
) -> std::io::Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if metadata.dev() == expected_device && metadata.ino() == expected_inode {
        fs::remove_file(path)?;
    }
    Ok(())
}

#[cfg(unix)]
pub(super) fn prepare_temp_runtime_home(path: &Path) -> std::io::Result<()> {
    match DirBuilder::new().mode(PRIVATE_DIRECTORY_MODE).create(path) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    restrict_owned_runtime_home(path, rustix::process::geteuid().as_raw())
}

#[cfg(unix)]
pub(super) fn restrict_owned_runtime_home(path: &Path, expected_uid: u32) -> std::io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            format!(
                "temporary runtime home is not a real directory: {}",
                path.display()
            ),
        ));
    }
    if metadata.uid() != expected_uid {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            format!(
                "temporary runtime home {} is owned by uid {}, expected {expected_uid}",
                path.display(),
                metadata.uid()
            ),
        ));
    }
    fs::set_permissions(path, Permissions::from_mode(PRIVATE_DIRECTORY_MODE))?;
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir()
        || metadata.file_type().is_symlink()
        || metadata.uid() != expected_uid
    {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            format!(
                "temporary runtime home changed while securing it: {}",
                path.display()
            ),
        ));
    }
    verify_mode(path, PRIVATE_DIRECTORY_MODE)
}

#[cfg(unix)]
pub(super) fn prepare_runtime_path(runtime_home: &Path, path: &Path) -> std::io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "runtime path has no parent"))?;
    prepare_private_directory(parent, Some(runtime_home))
}

#[cfg(unix)]
pub(super) fn prepare_socket_parent(
    runtime_home: &Path,
    socket_path: &Path,
) -> std::io::Result<()> {
    let parent = socket_path
        .parent()
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "socket path has no parent"))?;
    let root = parent.starts_with(runtime_home).then_some(runtime_home);
    prepare_private_directory(parent, root)
}

#[cfg(unix)]
pub(super) fn prepare_private_directory(parent: &Path, root: Option<&Path>) -> std::io::Result<()> {
    if root.is_some_and(|root| !parent.starts_with(root)) {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "private directory is outside its runtime root",
        ));
    }
    DirBuilder::new()
        .recursive(true)
        .mode(PRIVATE_DIRECTORY_MODE)
        .create(parent)?;

    if let Some(root) = root {
        for directory in parent
            .ancestors()
            .take_while(|directory| directory.starts_with(root))
        {
            restrict_private_directory(directory)?;
        }
    } else {
        restrict_private_directory(parent)?;
    }
    Ok(())
}

#[cfg(unix)]
fn restrict_private_directory(path: &Path) -> std::io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            format!(
                "private runtime path is not a directory: {}",
                path.display()
            ),
        ));
    }
    fs::set_permissions(path, Permissions::from_mode(PRIVATE_DIRECTORY_MODE))?;
    verify_mode(path, PRIVATE_DIRECTORY_MODE)
}

#[cfg(unix)]
fn restrict_socket(path: &Path) -> std::io::Result<()> {
    fs::set_permissions(path, Permissions::from_mode(SOCKET_MODE))?;
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_socket() {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            format!("daemon socket path is not a socket: {}", path.display()),
        ));
    }
    verify_mode(path, SOCKET_MODE)
}

#[cfg(unix)]
pub(super) fn verify_mode(path: &Path, expected: u32) -> std::io::Result<()> {
    let actual = fs::symlink_metadata(path)?.permissions().mode() & 0o777;
    if actual == expected {
        return Ok(());
    }
    Err(Error::new(
        ErrorKind::PermissionDenied,
        format!(
            "unsafe permissions on {}: expected {expected:04o}, got {actual:04o}",
            path.display()
        ),
    ))
}

#[cfg(all(test, unix))]
pub(super) fn mode(path: &Path) -> u32 {
    fs::symlink_metadata(path).unwrap().permissions().mode() & 0o777
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn temp_runtime_home_rejects_foreign_owner_before_chmod() {
        let root = tempfile::tempdir().unwrap();
        let runtime_home = root.path().join("runtime");
        fs::create_dir(&runtime_home).unwrap();
        fs::set_permissions(&runtime_home, Permissions::from_mode(0o755)).unwrap();
        let owner = fs::symlink_metadata(&runtime_home).unwrap().uid();
        let foreign_uid = if owner == u32::MAX {
            owner - 1
        } else {
            owner + 1
        };

        let error = restrict_owned_runtime_home(&runtime_home, foreign_uid).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::PermissionDenied);
        assert!(error.to_string().contains("is owned by uid"));
        assert_eq!(mode(&runtime_home), 0o755);
    }

    #[test]
    fn mode_verification_rejects_wide_permissions() {
        let parent = tempfile::tempdir().unwrap();
        fs::set_permissions(parent.path(), Permissions::from_mode(0o755)).unwrap();

        let error = verify_mode(parent.path(), PRIVATE_DIRECTORY_MODE).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::PermissionDenied);
        assert!(error.to_string().contains("expected 0700, got 0755"));
    }

    #[test]
    fn runtime_permission_hardening_covers_the_private_chain() {
        let root_parent = tempfile::tempdir().unwrap();
        let root = root_parent.path().join("user");
        let middle = root.join("platonic");
        let leaf = middle.join("workspaces").join("workspace-1");
        fs::create_dir_all(&leaf).unwrap();
        for path in [&root, &middle, &leaf] {
            fs::set_permissions(path, Permissions::from_mode(0o755)).unwrap();
        }

        prepare_private_directory(&leaf, Some(&root)).unwrap();

        assert_eq!(mode(&root), PRIVATE_DIRECTORY_MODE);
        assert_eq!(mode(&middle), PRIVATE_DIRECTORY_MODE);
        assert_eq!(mode(&middle.join("workspaces")), PRIVATE_DIRECTORY_MODE);
        assert_eq!(mode(&leaf), PRIVATE_DIRECTORY_MODE);
    }
}
