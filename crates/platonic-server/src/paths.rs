use crate::{AppError, AppResult};
#[cfg(windows)]
pub(crate) use platonic_client::paths::runtime_home;
#[cfg(unix)]
pub(crate) use platonic_client::paths::runtime_home_and_fallback;
pub use platonic_client::paths::{host_lock_path, host_socket_path, workspace_id};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DefaultSqlitePath {
    path: PathBuf,
}

impl DefaultSqlitePath {
    pub fn as_path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn from_path(path: PathBuf) -> Self {
        Self { path }
    }
}

/// Absolute path to the server-wide state root, independent of any workspace.
///
/// Holds the workspace registry and every table that spans workspaces. A
/// per-workspace ledger lives under `workspaces/<id>/` beneath this root.
pub fn server_state_root() -> AppResult<PathBuf> {
    let root = state_home()?.join("platonic");
    adopt_legacy_state_root(&root)?;
    Ok(root)
}

/// Move state written under the old `plato-agent` root to the current one.
///
/// The server was renamed to Platonic, and the state root followed. Without
/// this, every ledger a user already has would become invisible rather than
/// merely misfiled. Renaming the directory is atomic, and it happens only when
/// the old root exists and the new one does not, so it runs at most once and
/// never overwrites current state.
fn adopt_legacy_state_root(root: &Path) -> AppResult<()> {
    let Some(parent) = root.parent() else {
        return Ok(());
    };
    let legacy = parent.join("plato-agent");
    if !legacy.is_dir() || root.exists() {
        return Ok(());
    }
    match std::fs::rename(&legacy, root) {
        Ok(()) => Ok(()),
        // Another process may have adopted it first; that is success, not failure.
        Err(_) if root.exists() => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Absolute path to the server-wide database.
///
/// D005 requires every thread to be enumerable, including clientless threads
/// and orphans. Thread authority therefore cannot live in a per-workspace
/// ledger: an orphan in a workspace nobody has opened would be invisible.
pub fn server_db_path() -> AppResult<PathBuf> {
    Ok(server_state_root()?.join("server.db"))
}

/// Server-owned private repositories, grouped by immutable thread id.
pub(crate) fn thread_repositories_root(server_db_path: &Path) -> AppResult<PathBuf> {
    Ok(server_state_parent(server_db_path)?.join("worktrees"))
}

pub(crate) fn thread_repository_root(server_db_path: &Path, thread_id: &str) -> AppResult<PathBuf> {
    Ok(thread_repositories_root(server_db_path)?.join(thread_id))
}

pub(crate) fn one_shot_runs_root(server_db_path: &Path) -> AppResult<PathBuf> {
    Ok(server_state_parent(server_db_path)?.join("one-shot-runs"))
}

pub(crate) fn one_shot_run_root(server_db_path: &Path, run_id: &str) -> AppResult<PathBuf> {
    Ok(one_shot_runs_root(server_db_path)?.join(run_id))
}

/// Server-owned shared Git storage. Thread processes receive read-only alternates into it.
pub(crate) fn shared_git_root(server_db_path: &Path) -> AppResult<PathBuf> {
    Ok(server_state_parent(server_db_path)?.join("git"))
}

pub fn default_sqlite_path(workspace_id: &str) -> AppResult<PathBuf> {
    Ok(default_sqlite(workspace_id)?.path)
}

pub fn default_sqlite(workspace_id: &str) -> AppResult<DefaultSqlitePath> {
    let path = server_state_root()?
        .join("workspaces")
        .join(workspace_id)
        .join("ledger.db");
    Ok(DefaultSqlitePath { path })
}

pub(crate) fn workspace_sqlite_path(
    server_db_path: &Path,
    workspace_id: &str,
) -> AppResult<PathBuf> {
    let state_root = server_state_parent(server_db_path)?;
    Ok(state_root
        .join("workspaces")
        .join(workspace_id)
        .join("ledger.db"))
}

fn server_state_parent(server_db_path: &Path) -> AppResult<&Path> {
    server_db_path.parent().ok_or_else(|| {
        AppError::Config(format!(
            "server database has no state root: {}",
            server_db_path.display()
        ))
    })
}

/// Path used by the pre-registry, path-derived ledger layout.
#[cfg(test)]
pub(crate) fn legacy_sqlite_path(workspace_root: &Path) -> AppResult<PathBuf> {
    legacy_sqlite_path_at(&server_db_path()?, workspace_root)
}

pub(crate) fn legacy_sqlite_path_at(
    server_db_path: &Path,
    workspace_root: &Path,
) -> AppResult<PathBuf> {
    let state_root = server_db_path.parent().ok_or_else(|| {
        AppError::Config(format!(
            "server database has no state root: {}",
            server_db_path.display()
        ))
    })?;
    Ok(state_root
        .join("workspaces")
        .join(workspace_id(workspace_root)?)
        .join("agent.db"))
}

/// Move a legacy SQLite database and any sidecars to its minted-id location.
pub(crate) fn adopt_legacy_sqlite(
    server_db_path: &Path,
    workspace_root: &Path,
    destination: &Path,
) -> AppResult<bool> {
    move_sqlite_files(
        &legacy_sqlite_path_at(server_db_path, workspace_root)?,
        destination,
    )
}

pub(crate) fn move_sqlite_files(source: &Path, destination: &Path) -> AppResult<bool> {
    // Companions move before the main database, which acts as the completion
    // marker. A retry can therefore finish or roll back an interrupted move.
    let mut files = Vec::new();
    for suffix in ["-journal", "-wal", "-shm", ""] {
        let source = sqlite_companion(source, suffix);
        let destination = sqlite_companion(destination, suffix);
        let at_source = regular_sqlite_file(&source)?;
        let at_destination = regular_sqlite_file(&destination)?;
        if at_source && at_destination {
            return Err(AppError::Config(format!(
                "workspace ledger exists at both migration paths: {} and {}",
                source.display(),
                destination.display()
            )));
        }
        files.push((source, destination, at_source, at_destination));
    }

    let (_, _, source_main, destination_main) = files
        .last()
        .expect("SQLite migration always includes the main database");
    if !source_main && !destination_main {
        if files
            .iter()
            .any(|(_, _, at_source, at_destination)| *at_source || *at_destination)
        {
            return Err(AppError::Config(
                "workspace ledger companions exist without a main database".into(),
            ));
        }
        return Ok(false);
    }

    let parent = destination.parent().ok_or_else(|| {
        AppError::Config(format!(
            "workspace ledger destination has no parent: {}",
            destination.display()
        ))
    })?;
    if files.iter().any(|(_, _, at_source, _)| *at_source) {
        std::fs::create_dir_all(parent)?;
    }
    let mut moved = files
        .iter()
        .filter(|(_, _, _, at_destination)| *at_destination)
        .map(|(source, destination, _, _)| (source.clone(), destination.clone()))
        .collect::<Vec<_>>();
    for (source, destination, at_source, _) in files {
        if !at_source {
            continue;
        }
        if let Err(error) = std::fs::rename(&source, &destination) {
            for (source, destination) in moved.into_iter().rev() {
                if let Err(rollback) = std::fs::rename(&destination, &source) {
                    return Err(AppError::Config(format!(
                        "ledger adoption failed ({error}) and rollback failed ({rollback})"
                    )));
                }
            }
            return Err(error.into());
        }
        moved.push((source, destination));
    }
    Ok(true)
}

fn regular_sqlite_file(path: &Path) -> AppResult<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(true),
        Ok(_) => Err(AppError::Config(format!(
            "workspace ledger path is not a regular file: {}",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn sqlite_companion(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

#[cfg(unix)]
fn state_home() -> AppResult<PathBuf> {
    if let Some(value) = std::env::var_os("XDG_STATE_HOME")
        && !value.is_empty()
    {
        return Ok(PathBuf::from(value));
    }
    let home = std::env::var_os("HOME")
        .ok_or_else(|| AppError::Config("HOME is required for default --db path".into()))?;
    Ok(PathBuf::from(home).join(".local").join("state"))
}

#[cfg(windows)]
fn state_home() -> AppResult<PathBuf> {
    local_app_data("default --db path")
}

#[cfg(windows)]
fn local_app_data(purpose: &str) -> AppResult<PathBuf> {
    let value = std::env::var_os("LOCALAPPDATA")
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AppError::Config(format!("LOCALAPPDATA is required for {purpose}")))?;
    Ok(PathBuf::from(value))
}

#[cfg(test)]
pub(crate) fn with_test_xdg<T>(root: &Path, run: impl FnOnce() -> T) -> T {
    #[cfg(unix)]
    {
        let state_home = root.join("xdg-state");
        let runtime_home = root.join("xdg-runtime");
        temp_env::with_vars(
            [
                ("XDG_STATE_HOME", Some(state_home.as_os_str())),
                ("XDG_RUNTIME_DIR", Some(runtime_home.as_os_str())),
            ],
            run,
        )
    }
    #[cfg(windows)]
    {
        let local_app_data = root.join("local-app-data");
        temp_env::with_var("LOCALAPPDATA", Some(local_app_data.as_os_str()), run)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A user who already has ledgers under the old root keeps them. The
    /// rename must not be a silent data loss.
    #[cfg(unix)]
    #[test]
    fn existing_state_under_the_legacy_root_is_adopted_once_and_never_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        with_test_xdg(dir.path(), || {
            let state_home = dir.path().join("xdg-state");
            let legacy_ledger = state_home
                .join("plato-agent")
                .join("workspaces")
                .join("workspace-abc")
                .join("agent.db");
            std::fs::create_dir_all(legacy_ledger.parent().unwrap()).unwrap();
            std::fs::write(&legacy_ledger, b"original ledger bytes").unwrap();

            let root = server_state_root().unwrap();
            assert_eq!(root, state_home.join("platonic"));
            let adopted = root
                .join("workspaces")
                .join("workspace-abc")
                .join("agent.db");
            assert_eq!(std::fs::read(&adopted).unwrap(), b"original ledger bytes");
            assert!(!state_home.join("plato-agent").exists());

            // A second legacy root appearing later must not clobber current state.
            std::fs::create_dir_all(state_home.join("plato-agent")).unwrap();
            std::fs::write(state_home.join("plato-agent").join("stray"), b"stray").unwrap();
            assert_eq!(server_state_root().unwrap(), root);
            assert_eq!(std::fs::read(&adopted).unwrap(), b"original ledger bytes");
        });
    }

    #[test]
    fn default_sqlite_path_uses_minted_workspace_id() {
        let dir = tempfile::tempdir().unwrap();
        with_test_xdg(dir.path(), || {
            let path = default_sqlite_path("ws-1234").unwrap();

            assert_eq!(
                path,
                dir.path()
                    .join("xdg-state/platonic/workspaces/ws-1234/ledger.db")
            );
        });
    }

    #[test]
    fn legacy_sqlite_is_adopted_with_sidecars_and_can_be_restored() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        with_test_xdg(dir.path(), || {
            let legacy = legacy_sqlite_path(&workspace).unwrap();
            std::fs::create_dir_all(legacy.parent().unwrap()).unwrap();
            std::fs::write(&legacy, b"ledger").unwrap();
            std::fs::write(sqlite_companion(&legacy, "-wal"), b"wal").unwrap();
            let destination = default_sqlite_path("ws-1234").unwrap();

            let server_db = server_db_path().unwrap();
            assert!(adopt_legacy_sqlite(&server_db, &workspace, &destination).unwrap());
            assert_eq!(std::fs::read(&destination).unwrap(), b"ledger");
            assert_eq!(
                std::fs::read(sqlite_companion(&destination, "-wal")).unwrap(),
                b"wal"
            );
            assert!(!legacy.exists());

            assert!(move_sqlite_files(&destination, &legacy).unwrap());
            assert_eq!(std::fs::read(&legacy).unwrap(), b"ledger");
            assert_eq!(
                std::fs::read(sqlite_companion(&legacy, "-wal")).unwrap(),
                b"wal"
            );
            assert!(!destination.exists());

            std::fs::write(&destination, b"occupied").unwrap();
            assert!(adopt_legacy_sqlite(&server_db, &workspace, &destination).is_err());
            assert_eq!(std::fs::read(&legacy).unwrap(), b"ledger");
            assert_eq!(std::fs::read(&destination).unwrap(), b"occupied");
        });
    }

    #[test]
    fn interrupted_sidecar_move_resumes_before_the_main_database_moves() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        with_test_xdg(dir.path(), || {
            let legacy = legacy_sqlite_path(&workspace).unwrap();
            let destination = default_sqlite_path("ws-1234").unwrap();
            std::fs::create_dir_all(legacy.parent().unwrap()).unwrap();
            std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
            std::fs::write(&legacy, b"ledger").unwrap();
            let legacy_wal = sqlite_companion(&legacy, "-wal");
            let destination_wal = sqlite_companion(&destination, "-wal");
            std::fs::write(&legacy_wal, b"wal").unwrap();

            // Simulate termination after a companion rename but before the
            // main database, then prove the next adoption completes it.
            std::fs::rename(&legacy_wal, &destination_wal).unwrap();
            assert!(
                adopt_legacy_sqlite(&server_db_path().unwrap(), &workspace, &destination).unwrap()
            );

            assert_eq!(std::fs::read(&destination).unwrap(), b"ledger");
            assert_eq!(std::fs::read(&destination_wal).unwrap(), b"wal");
            assert!(!legacy.exists());
            assert!(!legacy_wal.exists());
        });
    }
}
