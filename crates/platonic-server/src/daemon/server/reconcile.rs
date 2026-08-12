use crate::{
    AppResult,
    thread_authority::{ThreadStopRecord, now_ms},
};
use std::{collections::HashMap, path::Path};

const RECONCILIATION_ACTOR: &str = "startup-reconciliation";

pub(super) fn reconcile_thread_repositories(server_db_path: &Path) -> AppResult<()> {
    let mut store = crate::server_store::ServerStore::open_or_create(server_db_path)?;
    let mut claims = HashMap::<String, (String, Vec<String>)>::new();
    for claim in store.branch_claims()? {
        let entry = claims
            .entry(claim.thread_id)
            .or_insert_with(|| (claim.workspace_id.clone(), Vec::new()));
        if entry.0 != claim.workspace_id {
            return Err(crate::AppError::Config(
                "one thread has branch claims in multiple workspaces".into(),
            ));
        }
        entry.1.push(claim.repo);
    }
    for (thread_id, (workspace_id, repos)) in claims {
        match store.thread_authority(&thread_id)? {
            Some(authority) if store.thread_stop(&thread_id)?.is_none() => {
                crate::thread_repository::reconcile_and_discard(
                    server_db_path,
                    &workspace_id,
                    &authority,
                )?;
                store.persist_thread_stop(&ThreadStopRecord::new(
                    thread_id.clone(),
                    RECONCILIATION_ACTOR.into(),
                    None,
                    now_ms(),
                )?)?;
            }
            Some(_) | None => crate::thread_repository::discard_claims(
                server_db_path,
                &workspace_id,
                &thread_id,
                &repos,
            )?,
        }
        store.release_thread_claims(&thread_id)?;
    }
    crate::thread_repository::remove_all_thread_roots(server_db_path)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use platonic_core::AgentId;
    use std::fs;

    fn git(cwd: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .current_dir(cwd)
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
        String::from_utf8(output.stdout).unwrap().trim().into()
    }

    #[test]
    fn startup_reconciliation_integrates_orphaned_private_repo_and_releases_claim() {
        use crate::thread_authority::{ThreadSpawnApprovalRecord, ThreadSpawnDecisionName};
        use platonic_protocol::{
            ReasoningEffort, ThreadApprovalPolicy, ThreadAuthorityRecord, ThreadConfinement,
            ThreadRepositoryRequest,
        };

        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        fs::create_dir(&workspace).unwrap();
        git(&workspace, &["init", "--quiet", "--initial-branch", "main"]);
        git(&workspace, &["config", "user.name", "Platonic Test"]);
        git(
            &workspace,
            &["config", "user.email", "platonic@example.invalid"],
        );
        fs::write(workspace.join("tracked.txt"), "user\n").unwrap();
        git(&workspace, &["add", "tracked.txt"]);
        git(&workspace, &["commit", "--quiet", "-m", "initial"]);
        let user_commit = git(&workspace, &["rev-parse", "HEAD"]);
        let server_db = root.path().join("state/platonic/server.db");
        let drafts = crate::thread_repository::resolve(
            &workspace,
            "thread_crashed",
            &workspace,
            None,
            &[ThreadRepositoryRequest {
                repo: ".".into(),
                branch: None,
            }],
        )
        .unwrap();
        let prepared = crate::thread_repository::prepare(
            &server_db,
            "workspace-crash",
            "thread_crashed",
            &drafts,
        )
        .unwrap();
        let private = Path::new(&prepared.worktrees[0].path);
        git(private, &["config", "user.name", "Platonic Test"]);
        git(
            private,
            &["config", "user.email", "platonic@example.invalid"],
        );
        fs::write(private.join("recovered.txt"), "recovered\n").unwrap();
        git(private, &["add", "recovered.txt"]);
        git(private, &["commit", "--quiet", "-m", "recovered"]);
        let recovered_commit = git(private, &["rev-parse", "HEAD"]);
        let authority = ThreadAuthorityRecord {
            thread_id: "thread_crashed".into(),
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
        let approval = ThreadSpawnApprovalRecord {
            spawn_id: "spawn_crashed".into(),
            thread_id: authority.thread_id.clone(),
            decision: ThreadSpawnDecisionName::Granted,
            actor: "test".into(),
            reason: None,
            occurred_at_ms: 1,
        };
        let mut store = crate::server_store::ServerStore::open_or_create(&server_db).unwrap();
        store
            .claim_thread_branches(
                "workspace-crash",
                "thread_crashed",
                &[(".".into(), "thread/thread_crashed".into())],
                1,
            )
            .unwrap();
        store
            .persist_thread_spawn(&approval, Some(&authority), Some(ThreadConfinement::None))
            .unwrap();
        drop(store);

        reconcile_thread_repositories(&server_db).unwrap();

        let store = crate::server_store::ServerStore::open_or_create(&server_db).unwrap();
        let stop = store.thread_stop("thread_crashed").unwrap().unwrap();
        assert_eq!(stop.actor, RECONCILIATION_ACTOR);
        assert!(store.branch_claims().unwrap().is_empty());
        assert!(
            !crate::paths::thread_repository_root(&server_db, "thread_crashed")
                .unwrap()
                .exists()
        );
        let shared =
            crate::thread_repository::shared_repository_path(&server_db, "workspace-crash", ".")
                .unwrap();
        assert_eq!(
            git(
                shared.parent().unwrap(),
                &[
                    "--git-dir",
                    &shared.to_string_lossy(),
                    "rev-parse",
                    "refs/heads/thread/thread_crashed",
                ],
            ),
            recovered_commit
        );
        assert_eq!(git(&workspace, &["rev-parse", "HEAD"]), user_commit);
        assert!(!workspace.join("recovered.txt").exists());
    }
}
