use super::sqlite::{
    SQLITE_SCHEMA_VERSION, SqliteLedger, configure_sqlite_connection,
    configure_sqlite_journal_mode, migrate_sqlite, read_sqlite_schema_version,
};
use crate::{AppResult, paths::DefaultSqlitePath};
use rusqlite::Connection;
use std::{
    fs::{self, File, OpenOptions},
    io::{Error, ErrorKind},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

pub(super) const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
pub(super) const PRIVATE_FILE_MODE: u32 = 0o600;
const SIDECAR_HARDEN_ATTEMPTS: usize = 8;

#[cfg(unix)]
pub(super) fn open_private_default_sqlite(
    location: &DefaultSqlitePath,
    create: bool,
) -> AppResult<SqliteLedger> {
    prepare_private_directories(location)?;
    let database = restrict_private_file(location.as_path(), create)?;
    restrict_existing_sidecars(location.as_path())?;

    let flags = if create {
        rusqlite::OpenFlags::default() | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW
    } else {
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW
    };
    let mut connection = Connection::open_with_flags(location.as_path(), flags)?;
    verify_open_file(
        location.as_path(),
        &database,
        PRIVATE_FILE_MODE,
        current_uid(),
    )?;
    let schema_version = if create {
        configure_sqlite_connection(&connection)?;
        migrate_sqlite(&mut connection)?;
        configure_sqlite_journal_mode(&connection)?;
        SQLITE_SCHEMA_VERSION
    } else {
        configure_sqlite_connection(&connection)?;
        read_sqlite_schema_version(&connection)?
    };
    restrict_existing_sidecars(location.as_path())?;
    verify_open_file(
        location.as_path(),
        &database,
        PRIVATE_FILE_MODE,
        current_uid(),
    )?;
    Ok(SqliteLedger {
        connection,
        path: location.as_path().to_path_buf(),
        schema_version,
        #[cfg(test)]
        terminal_fault: None,
        #[cfg(test)]
        voice_fault: None,
    })
}

#[cfg(unix)]
pub(super) fn prepare_private_directories(location: &DefaultSqlitePath) -> std::io::Result<()> {
    let workspace_directory = location
        .as_path()
        .parent()
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "default ledger has no parent"))?;
    let workspaces_directory = workspace_directory.parent().ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidInput,
            "default ledger workspace directory has no parent",
        )
    })?;
    let state_root = workspaces_directory.parent().ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidInput,
            "default ledger workspaces directory has no parent",
        )
    })?;
    if location.as_path().file_name() != Some(std::ffi::OsStr::new("ledger.db"))
        || workspaces_directory.file_name() != Some(std::ffi::OsStr::new("workspaces"))
        || state_root.file_name() != Some(std::ffi::OsStr::new("platonic"))
    {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "default ledger path does not match the app state layout",
        ));
    }
    let state_home = state_root
        .parent()
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "state root has no parent"))?;
    fs::create_dir_all(state_home)?;

    for directory in [state_root, workspaces_directory, workspace_directory] {
        restrict_private_directory(directory, current_uid())?;
    }
    Ok(())
}

#[cfg(unix)]
pub(super) fn restrict_private_directory(path: &Path, expected_uid: u32) -> std::io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => verify_metadata(path, &metadata, true, expected_uid)?,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            match fs::DirBuilder::new()
                .mode(PRIVATE_DIRECTORY_MODE)
                .create(path)
            {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        Err(error) => return Err(error),
    }
    fs::set_permissions(path, fs::Permissions::from_mode(PRIVATE_DIRECTORY_MODE))?;
    let metadata = fs::symlink_metadata(path)?;
    verify_metadata(path, &metadata, true, expected_uid)?;
    verify_mode(path, &metadata, PRIVATE_DIRECTORY_MODE)
}

#[cfg(unix)]
fn restrict_private_file(path: &Path, create: bool) -> std::io::Result<File> {
    let expected_uid = current_uid();
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            verify_metadata(path, &metadata, false, expected_uid)?;
            fs::set_permissions(path, fs::Permissions::from_mode(PRIVATE_FILE_MODE))?;
        }
        Err(error) if error.kind() == ErrorKind::NotFound && create => {}
        Err(error) => return Err(error),
    }

    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32);
    if create {
        options.write(true).create(true).mode(PRIVATE_FILE_MODE);
    }
    let file = options.open(path)?;
    file.set_permissions(fs::Permissions::from_mode(PRIVATE_FILE_MODE))?;
    verify_open_file(path, &file, PRIVATE_FILE_MODE, expected_uid)?;
    Ok(file)
}

#[cfg(unix)]
fn restrict_existing_sidecars(database: &Path) -> std::io::Result<()> {
    for suffix in ["-journal", "-wal", "-shm"] {
        let mut sidecar = database.as_os_str().to_os_string();
        sidecar.push(suffix);
        let path = PathBuf::from(sidecar);
        restrict_existing_sidecar(&path)?;
    }
    Ok(())
}

#[cfg(unix)]
fn restrict_existing_sidecar(path: &Path) -> std::io::Result<()> {
    for attempt in 0..SIDECAR_HARDEN_ATTEMPTS {
        match restrict_private_file(path, false) {
            Ok(_) => return Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                if sidecar_is_absent(path)? {
                    return Ok(());
                }
                if attempt + 1 == SIDECAR_HARDEN_ATTEMPTS {
                    return Err(Error::new(
                        ErrorKind::PermissionDenied,
                        format!("SQLite sidecar kept changing: {}", path.display()),
                    ));
                }
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("sidecar hardening loop always returns")
}

#[cfg(unix)]
fn sidecar_is_absent(path: &Path) -> std::io::Result<bool> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(true),
        Ok(_) => Ok(false),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
pub(super) fn verify_open_file(
    path: &Path,
    file: &File,
    expected_mode: u32,
    expected_uid: u32,
) -> std::io::Result<()> {
    let open_metadata = file.metadata()?;
    verify_metadata(path, &open_metadata, false, expected_uid)?;
    verify_mode(path, &open_metadata, expected_mode)?;
    let path_metadata = fs::symlink_metadata(path)?;
    verify_metadata(path, &path_metadata, false, expected_uid)?;
    verify_mode(path, &path_metadata, expected_mode)?;
    if open_metadata.dev() != path_metadata.dev() || open_metadata.ino() != path_metadata.ino() {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            format!("ledger path changed while opening: {}", path.display()),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn verify_metadata(
    path: &Path,
    metadata: &fs::Metadata,
    directory: bool,
    expected_uid: u32,
) -> std::io::Result<()> {
    let expected_type = if directory {
        "directory"
    } else {
        "regular file"
    };
    let actual_type_matches = if directory {
        metadata.file_type().is_dir()
    } else {
        metadata.file_type().is_file()
    };
    if !actual_type_matches || metadata.file_type().is_symlink() {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            format!(
                "private state path is not a {expected_type}: {}",
                path.display()
            ),
        ));
    }
    if metadata.uid() != expected_uid {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            format!(
                "private state path is not owned by the current user: {}",
                path.display()
            ),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn verify_mode(path: &Path, metadata: &fs::Metadata, expected: u32) -> std::io::Result<()> {
    let actual = metadata.permissions().mode() & 0o777;
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

#[cfg(unix)]
pub(super) fn current_uid() -> u32 {
    rustix::process::geteuid().as_raw()
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::{
        EventRecorder, SqliteLedger, run_jsonl_path, sqlite::tests::default_location,
    };
    use platonic_core::RunId;
    use rusqlite::Connection;
    use std::{
        fs,
        io::ErrorKind,
        os::unix::fs::PermissionsExt,
        path::{Path, PathBuf},
        process::Command,
    };

    fn mode(path: &Path) -> u32 {
        fs::symlink_metadata(path).unwrap().permissions().mode() & 0o777
    }

    fn set_mode(path: &Path, mode: u32) {
        fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn default_sqlite_creation_ignores_permissive_umask() {
        const CHILD: &str = "PLATO_TEST_LEDGER_PERMISSIVE_UMASK";
        if std::env::var_os(CHILD).is_none() {
            let output = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "ledger::unix::tests::default_sqlite_creation_ignores_permissive_umask",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "child failed:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }

        rustix::process::umask(rustix::fs::Mode::empty());
        let root = tempfile::tempdir().unwrap();
        let location = default_location(root.path());
        drop(SqliteLedger::open_or_create_default(&location).unwrap());

        let workspace_directory = location.as_path().parent().unwrap();
        let workspaces_directory = workspace_directory.parent().unwrap();
        let state_root = workspaces_directory.parent().unwrap();
        assert_eq!(mode(state_root), PRIVATE_DIRECTORY_MODE);
        assert_eq!(mode(workspaces_directory), PRIVATE_DIRECTORY_MODE);
        assert_eq!(mode(workspace_directory), PRIVATE_DIRECTORY_MODE);
        assert_eq!(mode(location.as_path()), PRIVATE_FILE_MODE);
    }

    #[cfg(unix)]
    #[test]
    fn default_run_jsonl_uses_minted_private_layout() {
        let root = tempfile::tempdir().unwrap();
        let location = default_location(root.path());
        let run_id = RunId::new("run_private").unwrap();

        drop(EventRecorder::create_default_jsonl(&location, &run_id).unwrap());

        let path = run_jsonl_path(location.as_path(), run_id.as_str()).unwrap();
        assert_eq!(
            path,
            location
                .as_path()
                .parent()
                .unwrap()
                .join("runs/run_private.jsonl")
        );
        assert_eq!(mode(path.parent().unwrap()), PRIVATE_DIRECTORY_MODE);
        assert_eq!(mode(&path), PRIVATE_FILE_MODE);
    }

    #[cfg(unix)]
    #[test]
    fn default_sqlite_tightens_existing_paths_and_preserves_content_on_reopen() {
        let root = tempfile::tempdir().unwrap();
        let location = default_location(root.path());
        fs::create_dir_all(location.as_path().parent().unwrap()).unwrap();
        let connection = Connection::open(location.as_path()).unwrap();
        connection
            .execute_batch("CREATE TABLE proof (value TEXT); INSERT INTO proof VALUES ('kept');")
            .unwrap();
        drop(connection);

        for directory in [
            location
                .as_path()
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .parent()
                .unwrap(),
            location.as_path().parent().unwrap().parent().unwrap(),
            location.as_path().parent().unwrap(),
        ] {
            set_mode(directory, 0o755);
        }
        set_mode(location.as_path(), 0o644);

        for _ in 0..2 {
            let ledger = SqliteLedger::open_or_create_default(&location).unwrap();
            let value: String = ledger
                .connection
                .query_row("SELECT value FROM proof", [], |row| row.get(0))
                .unwrap();
            assert_eq!(value, "kept");
        }
        assert_eq!(mode(location.as_path()), PRIVATE_FILE_MODE);
    }

    #[cfg(unix)]
    #[test]
    fn default_sqlite_rejects_symlinks_and_wrong_types() {
        use std::os::unix::fs::symlink;

        let symlink_root = tempfile::tempdir().unwrap();
        let location = default_location(symlink_root.path());
        fs::create_dir_all(location.as_path().parent().unwrap()).unwrap();
        let target = symlink_root.path().join("target.db");
        fs::write(&target, []).unwrap();
        symlink(&target, location.as_path()).unwrap();
        assert!(SqliteLedger::open_or_create_default(&location).is_err());
        assert!(SqliteLedger::open_default_readonly(&location).is_err());

        let directory_root = tempfile::tempdir().unwrap();
        let location = default_location(directory_root.path());
        fs::create_dir_all(location.as_path()).unwrap();
        assert!(SqliteLedger::open_or_create_default(&location).is_err());

        let sidecar_root = tempfile::tempdir().unwrap();
        let location = default_location(sidecar_root.path());
        drop(SqliteLedger::open_or_create_default(&location).unwrap());
        let mut journal = location.as_path().as_os_str().to_os_string();
        journal.push("-journal");
        symlink(&target, PathBuf::from(journal)).unwrap();
        assert!(SqliteLedger::open_or_create_default(&location).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn private_state_verifier_rejects_foreign_owner() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("agent.db");
        fs::write(&file, []).unwrap();
        let metadata = fs::symlink_metadata(&file).unwrap();
        let foreign_uid = if current_uid() == u32::MAX {
            current_uid() - 1
        } else {
            current_uid() + 1
        };

        let error = verify_metadata(&file, &metadata, false, foreign_uid).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::PermissionDenied);
        assert!(error.to_string().contains("not owned"));
    }

    #[cfg(unix)]
    #[test]
    fn default_sqlite_sidecars_are_private() {
        let root = tempfile::tempdir().unwrap();
        let location = default_location(root.path());
        let ledger = SqliteLedger::open_or_create_default(&location).unwrap();
        ledger
            .connection
            .execute_batch("CREATE TABLE proof (value INTEGER); INSERT INTO proof VALUES (1);")
            .unwrap();
        ledger
            .connection
            .execute_batch("BEGIN IMMEDIATE; UPDATE proof SET value = 2;")
            .unwrap();

        for suffix in ["-wal", "-shm"] {
            let mut sidecar = location.as_path().as_os_str().to_os_string();
            sidecar.push(suffix);
            let sidecar = PathBuf::from(sidecar);
            assert!(sidecar.is_file());
            assert_eq!(mode(&sidecar), PRIVATE_FILE_MODE);
        }
        ledger.connection.execute_batch("ROLLBACK").unwrap();
        drop(ledger);

        for suffix in ["-journal", "-wal", "-shm"] {
            let mut sidecar = location.as_path().as_os_str().to_os_string();
            sidecar.push(suffix);
            fs::write(PathBuf::from(sidecar), []).unwrap();
        }
        restrict_existing_sidecars(location.as_path()).unwrap();
        for suffix in ["-journal", "-wal", "-shm"] {
            let mut sidecar = location.as_path().as_os_str().to_os_string();
            sidecar.push(suffix);
            assert_eq!(mode(&PathBuf::from(sidecar)), PRIVATE_FILE_MODE);
        }
    }

    #[cfg(unix)]
    #[test]
    fn missing_or_reappeared_sidecars_are_rechecked() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let sidecar = root.path().join("agent.db-journal");
        assert!(sidecar_is_absent(&sidecar).unwrap());
        restrict_existing_sidecar(&sidecar).unwrap();

        fs::write(&sidecar, []).unwrap();
        set_mode(&sidecar, 0o644);
        assert!(!sidecar_is_absent(&sidecar).unwrap());
        restrict_existing_sidecar(&sidecar).unwrap();
        assert_eq!(mode(&sidecar), PRIVATE_FILE_MODE);

        fs::remove_file(&sidecar).unwrap();
        let target = root.path().join("target");
        fs::write(&target, []).unwrap();
        symlink(&target, &sidecar).unwrap();
        assert_eq!(
            restrict_existing_sidecar(&sidecar).unwrap_err().kind(),
            ErrorKind::PermissionDenied
        );

        fs::remove_file(&sidecar).unwrap();
        fs::create_dir(&sidecar).unwrap();
        assert_eq!(
            restrict_existing_sidecar(&sidecar).unwrap_err().kind(),
            ErrorKind::PermissionDenied
        );
    }

    #[cfg(unix)]
    #[test]
    fn explicit_sqlite_path_keeps_caller_managed_permissions() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("custom");
        let path = parent.join("agent.db");
        fs::create_dir(&parent).unwrap();
        drop(SqliteLedger::open_or_create(&path).unwrap());
        set_mode(&parent, 0o755);
        set_mode(&path, 0o644);

        drop(SqliteLedger::open_or_create(&path).unwrap());

        assert_eq!(mode(&parent), 0o755);
        assert_eq!(mode(&path), 0o644);
    }
}
