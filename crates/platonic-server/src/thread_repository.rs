use crate::{AppError, AppResult, paths};
use platonic_protocol::{
    ThreadAuthorityRecord, ThreadGrantedPath, ThreadRepositoryRequest, ThreadWorktree,
};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    fs,
    path::{Component, Path, PathBuf},
    process::{Command, Output},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ThreadRepositoryDraft {
    pub(crate) repo: String,
    pub(crate) source_path: PathBuf,
    pub(crate) branch: String,
    source_ref: String,
}

#[derive(Debug)]
pub(crate) struct PreparedThreadRepositories {
    pub(crate) worktrees: Vec<ThreadWorktree>,
    pub(crate) granted_paths: Vec<ThreadGrantedPath>,
}

pub(crate) fn resolve(
    workspace_root: &Path,
    thread_id: &str,
    cwd: &Path,
    parent: Option<&ThreadAuthorityRecord>,
    requests: &[ThreadRepositoryRequest],
) -> AppResult<Vec<ThreadRepositoryDraft>> {
    let resolved = if requests.is_empty() {
        infer_repository(workspace_root, cwd, parent)?
            .into_iter()
            .collect::<Vec<_>>()
    } else {
        requests
            .iter()
            .map(|request| resolve_request(workspace_root, parent, request))
            .collect::<AppResult<Vec<_>>>()?
    };
    if resolved.is_empty() {
        return Err(AppError::Config(
            "thread spawn requires a named Git repository and claimed branch".into(),
        ));
    }
    let mut seen = HashSet::new();
    resolved
        .into_iter()
        .map(|(repo, source_path, requested_branch)| {
            if !seen.insert(repo.clone()) {
                return Err(AppError::Config(format!(
                    "thread spawn names repository {repo} more than once"
                )));
            }
            let (branch, source_ref) = match requested_branch {
                Some(branch) => {
                    validate_branch(&branch)?;
                    let source_ref = format!("refs/heads/{branch}");
                    git_stdout(
                        &source_path,
                        &["rev-parse", "--verify", &format!("{source_ref}^{{commit}}")],
                    )?;
                    (branch, source_ref)
                }
                None => {
                    let branch = format!("thread/{thread_id}");
                    validate_branch(&branch)?;
                    git_stdout(&source_path, &["rev-parse", "--verify", "HEAD^{commit}"])?;
                    (branch, "HEAD".into())
                }
            };
            Ok(ThreadRepositoryDraft {
                repo,
                source_path,
                branch,
                source_ref,
            })
        })
        .collect()
}

fn infer_repository(
    workspace_root: &Path,
    cwd: &Path,
    parent: Option<&ThreadAuthorityRecord>,
) -> AppResult<Option<(String, PathBuf, Option<String>)>> {
    if let Some(parent) = parent {
        return Ok(parent
            .worktrees
            .iter()
            .find(|worktree| cwd.starts_with(&worktree.path))
            .map(|worktree| (worktree.repo.clone(), PathBuf::from(&worktree.path), None)));
    }
    let Some(source_path) = git_toplevel(cwd)? else {
        return Ok(None);
    };
    if !source_path.starts_with(workspace_root) {
        return Err(AppError::Config(format!(
            "thread repository is outside its workspace: {}",
            source_path.display()
        )));
    }
    Ok(Some((
        repository_name(workspace_root, &source_path)?,
        source_path,
        None,
    )))
}

fn resolve_request(
    workspace_root: &Path,
    parent: Option<&ThreadAuthorityRecord>,
    request: &ThreadRepositoryRequest,
) -> AppResult<(String, PathBuf, Option<String>)> {
    validate_repository_name(&request.repo)?;
    if let Some(parent) = parent {
        let worktree = parent
            .worktrees
            .iter()
            .find(|worktree| worktree.repo == request.repo)
            .ok_or_else(|| {
                AppError::Config(format!(
                    "child repository {} exceeds parent repository authority",
                    request.repo
                ))
            })?;
        return Ok((
            request.repo.clone(),
            PathBuf::from(&worktree.path),
            request.branch.clone(),
        ));
    }
    let source_path = workspace_root.join(&request.repo).canonicalize()?;
    if !source_path.starts_with(workspace_root) {
        return Err(AppError::Config(format!(
            "thread repository escapes its workspace: {}",
            request.repo
        )));
    }
    let toplevel = git_toplevel(&source_path)?.ok_or_else(|| {
        AppError::Config(format!(
            "thread repository is not a Git repository: {}",
            source_path.display()
        ))
    })?;
    if toplevel != source_path {
        return Err(AppError::Config(format!(
            "thread repository must name a Git top level: {}",
            request.repo
        )));
    }
    if repository_name(workspace_root, &source_path)? != request.repo {
        return Err(AppError::Config(format!(
            "thread repository must use its canonical workspace-relative name: {}",
            request.repo
        )));
    }
    Ok((request.repo.clone(), source_path, request.branch.clone()))
}

pub(crate) fn prepare(
    server_db_path: &Path,
    workspace_id: &str,
    thread_id: &str,
    repositories: &[ThreadRepositoryDraft],
) -> AppResult<PreparedThreadRepositories> {
    let thread_root = paths::thread_repository_root(server_db_path, thread_id)?;
    if thread_root.exists() {
        return Err(AppError::Config(format!(
            "thread repository root already exists: {}",
            thread_root.display()
        )));
    }
    create_private_directory(&thread_root)?;
    let scratch = thread_root.join("scratch");
    create_private_directory(&scratch)?;
    let repos_root = thread_root.join("repos");
    create_private_directory(&repos_root)?;

    let result = (|| {
        let mut worktrees = Vec::with_capacity(repositories.len());
        for (index, repository) in repositories.iter().enumerate() {
            let shared = shared_repository_path(server_db_path, workspace_id, &repository.repo)?;
            ensure_shared_repository(&shared)?;
            let base_ref = base_ref(thread_id, &repository.repo);
            let claimed_ref = format!("refs/heads/{}", repository.branch);
            let oid = match git_dir_reference(&shared, &claimed_ref)? {
                Some(oid) => oid,
                None => {
                    git_dir(
                        &shared,
                        &[
                            "fetch",
                            "--force",
                            "--no-tags",
                            "--no-write-fetch-head",
                            &repository.source_path.to_string_lossy(),
                            &format!("{}:{base_ref}", repository.source_ref),
                        ],
                    )?;
                    git_dir_stdout(
                        &shared,
                        &["rev-parse", "--verify", &format!("{base_ref}^{{commit}}")],
                    )?
                }
            };
            let private =
                repos_root.join(format!("{index:03}-{}", repository_key(&repository.repo)));
            git_command(&[
                "init",
                "--quiet",
                "--initial-branch",
                &repository.branch,
                &private.to_string_lossy(),
            ])?;
            let alternates = private.join(".git/objects/info/alternates");
            fs::create_dir_all(
                alternates
                    .parent()
                    .expect("alternates path has an info directory"),
            )?;
            fs::write(
                &alternates,
                format!("{}\n", shared.join("objects").display()),
            )?;
            git(&private, &["config", "gc.auto", "0"])?;
            git(&private, &["config", "maintenance.auto", "false"])?;
            git(
                &private,
                &[
                    "update-ref",
                    &format!("refs/heads/{}", repository.branch),
                    &oid,
                ],
            )?;
            git(
                &private,
                &[
                    "symbolic-ref",
                    "HEAD",
                    &format!("refs/heads/{}", repository.branch),
                ],
            )?;
            git(&private, &["reset", "--quiet", "--hard", &oid])?;
            let private = private.canonicalize()?;
            worktrees.push(ThreadWorktree {
                repo: repository.repo.clone(),
                branch: repository.branch.clone(),
                path: private.to_string_lossy().into_owned(),
            });
        }
        let scratch = scratch.canonicalize()?;
        Ok(PreparedThreadRepositories {
            worktrees,
            granted_paths: vec![ThreadGrantedPath {
                path: scratch.to_string_lossy().into_owned(),
                writable: true,
            }],
        })
    })();
    if result.is_err() {
        let _ = discard(server_db_path, workspace_id, thread_id, repositories);
    }
    result
}

pub(crate) fn integrate_and_discard(
    server_db_path: &Path,
    workspace_id: &str,
    authority: &ThreadAuthorityRecord,
) -> AppResult<()> {
    for worktree in &authority.worktrees {
        let private = Path::new(&worktree.path);
        let shared = shared_repository_path(server_db_path, workspace_id, &worktree.repo)?;
        if !private.is_dir() {
            if git_dir_reference(&shared, &format!("refs/heads/{}", worktree.branch))?.is_none() {
                return Err(AppError::Config(format!(
                    "thread private repository is missing before integration: {}",
                    private.display()
                )));
            }
            delete_base_ref(&shared, &base_ref(&authority.thread_id, &worktree.repo))?;
            git_dir(&shared, &["gc", "--auto"])?;
            continue;
        }
        git_dir(
            &shared,
            &[
                "fetch",
                "--force",
                "--no-tags",
                "--no-write-fetch-head",
                &private.to_string_lossy(),
                &format!("refs/heads/{0}:refs/heads/{0}", worktree.branch),
            ],
        )?;
        delete_base_ref(&shared, &base_ref(&authority.thread_id, &worktree.repo))?;
        git_dir(&shared, &["gc", "--auto"])?;
    }
    remove_thread_root(server_db_path, &authority.thread_id)
}

pub(crate) fn reconcile_and_discard(
    server_db_path: &Path,
    workspace_id: &str,
    authority: &ThreadAuthorityRecord,
) -> AppResult<()> {
    if authority
        .worktrees
        .iter()
        .all(|worktree| Path::new(&worktree.path).is_dir())
    {
        integrate_and_discard(server_db_path, workspace_id, authority)
    } else {
        for worktree in &authority.worktrees {
            let shared = shared_repository_path(server_db_path, workspace_id, &worktree.repo)?;
            if shared.is_dir() {
                delete_base_ref(&shared, &base_ref(&authority.thread_id, &worktree.repo))?;
            }
        }
        remove_thread_root(server_db_path, &authority.thread_id)
    }
}

pub(crate) fn discard(
    server_db_path: &Path,
    workspace_id: &str,
    thread_id: &str,
    repositories: &[ThreadRepositoryDraft],
) -> AppResult<()> {
    for repository in repositories {
        let shared = shared_repository_path(server_db_path, workspace_id, &repository.repo)?;
        if shared.is_dir() {
            delete_base_ref(&shared, &base_ref(thread_id, &repository.repo))?;
        }
    }
    remove_thread_root(server_db_path, thread_id)
}

pub(crate) fn discard_claims(
    server_db_path: &Path,
    workspace_id: &str,
    thread_id: &str,
    repos: &[String],
) -> AppResult<()> {
    for repo in repos {
        let shared = shared_repository_path(server_db_path, workspace_id, repo)?;
        if shared.is_dir() {
            delete_base_ref(&shared, &base_ref(thread_id, repo))?;
        }
    }
    remove_thread_root(server_db_path, thread_id)
}

pub(crate) fn remove_thread_root(server_db_path: &Path, thread_id: &str) -> AppResult<()> {
    validate_single_component(thread_id, "thread id")?;
    let root = paths::thread_repository_root(server_db_path, thread_id)?;
    match fs::symlink_metadata(&root) {
        Ok(metadata) if metadata.file_type().is_dir() => {
            fs::remove_dir_all(root).map_err(Into::into)
        }
        Ok(_) => Err(AppError::Config(format!(
            "thread repository root is not a directory: {}",
            root.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn remove_all_thread_roots(server_db_path: &Path) -> AppResult<()> {
    let root = paths::thread_repositories_root(server_db_path)?;
    let entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    for entry in entries {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            fs::remove_dir_all(entry.path())?;
        } else {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

pub(crate) fn shared_repository_path(
    server_db_path: &Path,
    workspace_id: &str,
    repo: &str,
) -> AppResult<PathBuf> {
    validate_repository_name(repo)?;
    Ok(paths::shared_git_root(server_db_path)?.join(format!(
        "{}.git",
        repository_key(&format!("{workspace_id}\0{repo}"))
    )))
}

fn ensure_shared_repository(path: &Path) -> AppResult<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.file_type().is_dir() {
                return Err(AppError::Config(format!(
                    "shared Git store is not a directory: {}",
                    path.display()
                )));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let parent = path.parent().ok_or_else(|| {
        AppError::Config(format!(
            "shared Git store has no parent: {}",
            path.display()
        ))
    })?;
    create_private_directory(parent)?;
    git_command(&["init", "--quiet", "--bare", &path.to_string_lossy()])
}

fn delete_base_ref(shared: &Path, reference: &str) -> AppResult<()> {
    git_dir(shared, &["update-ref", "-d", reference])
}

fn base_ref(thread_id: &str, repo: &str) -> String {
    format!("refs/platonic/bases/{thread_id}/{}", repository_key(repo))
}

fn repository_key(repo: &str) -> String {
    let digest = Sha256::digest(repo.as_bytes());
    digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn repository_name(workspace_root: &Path, source_path: &Path) -> AppResult<String> {
    let relative = source_path.strip_prefix(workspace_root).map_err(|_| {
        AppError::Config(format!(
            "thread repository is outside its workspace: {}",
            source_path.display()
        ))
    })?;
    if relative.as_os_str().is_empty() {
        Ok(".".into())
    } else {
        let name = relative.to_string_lossy().into_owned();
        validate_repository_name(&name)?;
        Ok(name)
    }
}

fn validate_repository_name(repo: &str) -> AppResult<()> {
    if repo.is_empty() || Path::new(repo).is_absolute() {
        return Err(AppError::Config(
            "thread repository name must be a non-empty relative path".into(),
        ));
    }
    if repo != "."
        && Path::new(repo)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(AppError::Config(format!(
            "thread repository name is invalid: {repo}"
        )));
    }
    Ok(())
}

fn validate_single_component(value: &str, name: &str) -> AppResult<()> {
    let mut components = Path::new(value).components();
    if !matches!(components.next(), Some(Component::Normal(_))) || components.next().is_some() {
        return Err(AppError::Config(format!("invalid {name}: {value}")));
    }
    Ok(())
}

fn validate_branch(branch: &str) -> AppResult<()> {
    git_command(&["check-ref-format", "--branch", branch])
}

fn git_toplevel(path: &Path) -> AppResult<Option<PathBuf>> {
    let output = git_output(Some(path), &["rev-parse", "--show-toplevel"])?;
    if !output.status.success() {
        return Ok(None);
    }
    let path = String::from_utf8(output.stdout)
        .map_err(|_| AppError::Config("Git returned a non-UTF-8 repository path".into()))?;
    Ok(Some(PathBuf::from(path.trim()).canonicalize()?))
}

fn git(path: &Path, args: &[&str]) -> AppResult<()> {
    git_output_success(Some(path), args).map(|_| ())
}

fn git_stdout(path: &Path, args: &[&str]) -> AppResult<String> {
    let output = git_output_success(Some(path), args)?;
    String::from_utf8(output.stdout)
        .map(|value| value.trim().to_owned())
        .map_err(|_| AppError::Config("Git returned non-UTF-8 output".into()))
}

fn git_dir(path: &Path, args: &[&str]) -> AppResult<()> {
    let path = path.to_string_lossy();
    let mut all = vec!["--git-dir", path.as_ref()];
    all.extend_from_slice(args);
    git_command(&all)
}

fn git_dir_stdout(path: &Path, args: &[&str]) -> AppResult<String> {
    let path = path.to_string_lossy();
    let mut all = vec!["--git-dir", path.as_ref()];
    all.extend_from_slice(args);
    let output = git_output_success(None, &all)?;
    String::from_utf8(output.stdout)
        .map(|value| value.trim().to_owned())
        .map_err(|_| AppError::Config("Git returned non-UTF-8 output".into()))
}

fn git_dir_reference(path: &Path, reference: &str) -> AppResult<Option<String>> {
    let path = path.to_string_lossy();
    let output = git_output(
        None,
        &[
            "--git-dir",
            path.as_ref(),
            "rev-parse",
            "--verify",
            &format!("{reference}^{{commit}}"),
        ],
    )?;
    if !output.status.success() {
        return Ok(None);
    }
    String::from_utf8(output.stdout)
        .map(|value| Some(value.trim().to_owned()))
        .map_err(|_| AppError::Config("Git returned non-UTF-8 output".into()))
}

fn git_command(args: &[&str]) -> AppResult<()> {
    git_output_success(None, args).map(|_| ())
}

fn git_output_success(cwd: Option<&Path>, args: &[&str]) -> AppResult<Output> {
    let output = git_output(cwd, args)?;
    if output.status.success() {
        return Ok(output);
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(AppError::Config(format!(
        "Git command failed ({}): {}",
        args.join(" "),
        stderr.trim()
    )))
}

fn git_output(cwd: Option<&Path>, args: &[&str]) -> AppResult<Output> {
    let mut command = Command::new("git");
    command
        .args(args)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_OPTIONAL_LOCKS", "0");
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    Ok(command.output()?)
}

pub(crate) fn create_private_directory(path: &Path) -> AppResult<()> {
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use platonic_core::AgentId;
    use platonic_protocol::{ReasoningEffort, ThreadApprovalPolicy};
    use std::sync::{Arc, Barrier};

    fn init_repository(path: &Path, marker: &str) -> String {
        fs::create_dir_all(path).unwrap();
        git_command(&[
            "init",
            "--quiet",
            "--initial-branch",
            "main",
            &path.to_string_lossy(),
        ])
        .unwrap();
        git(path, &["config", "user.name", "Platonic Test"]).unwrap();
        git(path, &["config", "user.email", "platonic@example.invalid"]).unwrap();
        fs::write(path.join("tracked.txt"), marker).unwrap();
        git(path, &["add", "tracked.txt"]).unwrap();
        git(path, &["commit", "--quiet", "-m", "initial"]).unwrap();
        git_stdout(path, &["rev-parse", "HEAD"]).unwrap()
    }

    #[test]
    fn private_repositories_commit_and_integrate_without_mutating_user_repositories() {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        let repo_a = workspace.join("repo-a");
        let repo_b = workspace.join("repo-b");
        let original_a = init_repository(&repo_a, "a\n");
        let original_b = init_repository(&repo_b, "b\n");
        let server_db = root.path().join("state/server.db");
        let requests = vec![
            ThreadRepositoryRequest {
                repo: "repo-a".into(),
                branch: None,
            },
            ThreadRepositoryRequest {
                repo: "repo-b".into(),
                branch: Some("main".into()),
            },
        ];
        let drafts = resolve(&workspace, "thread_test", &repo_a, None, &requests).unwrap();

        let prepared = prepare(&server_db, "workspace-test", "thread_test", &drafts).unwrap();

        assert_eq!(prepared.worktrees.len(), 2);
        assert_eq!(prepared.worktrees[0].branch, "thread/thread_test");
        assert_eq!(prepared.worktrees[1].branch, "main");
        for worktree in &prepared.worktrees {
            let private = Path::new(&worktree.path);
            assert!(private.join(".git/refs/heads").is_dir());
            assert!(private.join(".git/index").is_file());
            assert_eq!(git_stdout(private, &["config", "gc.auto"]).unwrap(), "0");
            assert_eq!(
                git_stdout(private, &["config", "maintenance.auto"]).unwrap(),
                "false"
            );
            let shared = shared_repository_path(&server_db, "workspace-test", &worktree.repo)
                .unwrap()
                .join("objects")
                .canonicalize()
                .unwrap();
            assert_eq!(
                fs::read_to_string(private.join(".git/objects/info/alternates"))
                    .unwrap()
                    .trim(),
                shared.to_string_lossy()
            );
        }

        let private_a = Path::new(&prepared.worktrees[0].path);
        git(private_a, &["config", "user.name", "Platonic Test"]).unwrap();
        git(
            private_a,
            &["config", "user.email", "platonic@example.invalid"],
        )
        .unwrap();
        fs::write(private_a.join("thread.txt"), "private\n").unwrap();
        git(private_a, &["add", "thread.txt"]).unwrap();
        git(private_a, &["commit", "--quiet", "-m", "thread commit"]).unwrap();
        let private_commit = git_stdout(private_a, &["rev-parse", "HEAD"]).unwrap();
        assert_ne!(private_commit, original_a);
        assert_eq!(
            git_stdout(&repo_a, &["rev-parse", "HEAD"]).unwrap(),
            original_a
        );
        assert!(!repo_a.join("thread.txt").exists());
        let private_b = Path::new(&prepared.worktrees[1].path);
        git(private_b, &["config", "user.name", "Platonic Test"]).unwrap();
        git(
            private_b,
            &["config", "user.email", "platonic@example.invalid"],
        )
        .unwrap();
        fs::write(private_b.join("thread-b.txt"), "private b\n").unwrap();
        git(private_b, &["add", "thread-b.txt"]).unwrap();
        git(private_b, &["commit", "--quiet", "-m", "thread b commit"]).unwrap();
        let private_b_commit = git_stdout(private_b, &["rev-parse", "HEAD"]).unwrap();

        let authority = ThreadAuthorityRecord {
            thread_id: "thread_test".into(),
            parent_thread_id: None,
            spawning_actor: "test".into(),
            agent_id: Some(AgentId::new("plato").unwrap()),
            model: "gpt-test".into(),
            reasoning_effort: ReasoningEffort::None,
            approval_policy: ThreadApprovalPolicy::Prompt,
            toolset: vec!["file.read".into()],
            worktrees: prepared.worktrees,
            granted_paths: prepared.granted_paths,
            network: false,
            created_at_ms: 1,
        };
        integrate_and_discard(&server_db, "workspace-test", &authority).unwrap();

        assert!(
            !paths::thread_repository_root(&server_db, "thread_test")
                .unwrap()
                .exists()
        );
        let shared_a = shared_repository_path(&server_db, "workspace-test", "repo-a").unwrap();
        assert_eq!(
            git_dir_stdout(&shared_a, &["rev-parse", "refs/heads/thread/thread_test"]).unwrap(),
            private_commit
        );
        git_dir(
            &shared_a,
            &["cat-file", "-e", &format!("{private_commit}^{{commit}}")],
        )
        .unwrap();
        assert_eq!(
            git_stdout(&repo_a, &["rev-parse", "HEAD"]).unwrap(),
            original_a
        );
        assert_eq!(
            git_stdout(&repo_b, &["rev-parse", "HEAD"]).unwrap(),
            original_b
        );

        let next_drafts = resolve(
            &workspace,
            "thread_next",
            &repo_b,
            None,
            &[ThreadRepositoryRequest {
                repo: "repo-b".into(),
                branch: Some("main".into()),
            }],
        )
        .unwrap();
        let next = prepare(&server_db, "workspace-test", "thread_next", &next_drafts).unwrap();
        assert_eq!(
            git_stdout(Path::new(&next.worktrees[0].path), &["rev-parse", "HEAD"]).unwrap(),
            private_b_commit
        );
        discard(&server_db, "workspace-test", "thread_next", &next_drafts).unwrap();
    }

    #[test]
    fn repository_claims_require_one_canonical_workspace_relative_identity() {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        let repository = workspace.join("repo");
        init_repository(&repository, "repo\n");

        for alias in ["./repo", "repo/."] {
            assert!(
                resolve(
                    &workspace,
                    "thread_alias",
                    &repository,
                    None,
                    &[ThreadRepositoryRequest {
                        repo: alias.into(),
                        branch: None,
                    }],
                )
                .is_err()
            );
        }
    }

    #[test]
    fn server_integrates_two_disjoint_live_thread_repositories_concurrently() {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        init_repository(&workspace.join("repo-a"), "a\n");
        init_repository(&workspace.join("repo-b"), "b\n");
        let server_db = root.path().join("state/server.db");
        let barrier = Arc::new(Barrier::new(3));
        let workers = [("thread_a", "repo-a"), ("thread_b", "repo-b")].map(|(thread_id, repo)| {
            let workspace = workspace.clone();
            let server_db = server_db.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let drafts = resolve(
                    &workspace,
                    thread_id,
                    &workspace.join(repo),
                    None,
                    &[ThreadRepositoryRequest {
                        repo: repo.into(),
                        branch: None,
                    }],
                )
                .unwrap();
                let prepared = prepare(&server_db, "workspace-test", thread_id, &drafts).unwrap();
                let private = Path::new(&prepared.worktrees[0].path);
                git(private, &["config", "user.name", "Platonic Test"]).unwrap();
                git(
                    private,
                    &["config", "user.email", "platonic@example.invalid"],
                )
                .unwrap();
                fs::write(private.join("thread.txt"), format!("{thread_id}\n")).unwrap();
                git(private, &["add", "thread.txt"]).unwrap();
                git(private, &["commit", "--quiet", "-m", thread_id]).unwrap();
                let commit = git_stdout(private, &["rev-parse", "HEAD"]).unwrap();
                let branch = prepared.worktrees[0].branch.clone();
                let authority = ThreadAuthorityRecord {
                    thread_id: thread_id.into(),
                    parent_thread_id: None,
                    spawning_actor: "test".into(),
                    agent_id: Some(AgentId::new("plato").unwrap()),
                    model: "gpt-test".into(),
                    reasoning_effort: ReasoningEffort::None,
                    approval_policy: ThreadApprovalPolicy::Prompt,
                    toolset: vec!["file.read".into()],
                    worktrees: prepared.worktrees,
                    granted_paths: prepared.granted_paths,
                    network: false,
                    created_at_ms: 1,
                };
                barrier.wait();
                integrate_and_discard(&server_db, "workspace-test", &authority).unwrap();
                (repo.to_owned(), branch, commit)
            })
        });
        barrier.wait();
        for worker in workers {
            let (repo, branch, commit) = worker.join().unwrap();
            let shared = shared_repository_path(&server_db, "workspace-test", &repo).unwrap();
            assert_eq!(
                git_dir_stdout(&shared, &["rev-parse", &format!("refs/heads/{branch}")]).unwrap(),
                commit
            );
        }
    }
}
