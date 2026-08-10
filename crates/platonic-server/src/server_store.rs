//! Server-wide state, independent of any workspace.
//!
//! D005 requires every thread to be enumerable — including clientless threads
//! and orphans. Thread authority therefore cannot live in a per-workspace
//! ledger: a thread in a workspace nobody has opened would be invisible, and a
//! dead parent would hide its children. This store lives once per host, beside
//! the socket, and holds the records that must outlive any single workspace.
//!
//! Workspace ledgers keep what is workspace-scoped: the event log, sessions,
//! and voice events. Nothing here is a log; every table is current state.

#[cfg(test)]
use crate::thread_authority::legacy_status_authority;
use crate::{
    AppError, AppResult,
    ledger::{row_u64, sqlite_i64},
    paths,
    thread_authority::{
        ThreadSpawnApprovalRecord, ThreadSpawnDecisionName, ThreadStopRecord,
        authority_working_directory, validate_child_authority, validate_complete_authority,
    },
};
use platonic_core::{AgentId, EffectClass};
use platonic_protocol::{
    ReasoningEffort, ThreadApprovalPolicy, ThreadAuthorityRecord, ThreadConfinement,
    ThreadGrantedPath, ThreadWorktree,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params, types::Type};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// A registered workspace: a named directory the server knows about.
///
/// The name is the handle operators use; the root is the directory it wears.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkspaceRecord {
    /// Minted once and never derived from the path (P021). A workspace that
    /// moves keeps its identity and its history; only `root` changes.
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) root: String,
    pub(crate) ledger_path: String,
    pub(crate) created_at_ms: u64,
}

/// One immutable configured agent profile in server-wide state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AgentRecord {
    pub(crate) id: AgentId,
    pub(crate) workspace_id: String,
    pub(crate) model: String,
    pub(crate) reasoning_effort: ReasoningEffort,
    pub(crate) approval_policy: ThreadApprovalPolicy,
    pub(crate) toolset: Vec<String>,
    pub(crate) created_at_ms: u64,
}

/// Whether a registered workspace's directory is still where the registry says.
///
/// A workspace whose directory has vanished is reported broken, never omitted
/// and never auto-removed (P021): its ledger is retained and spawning into it
/// fails at the gate rather than silently resurrecting an empty workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkspaceHealth {
    Present,
    Broken,
}

/// A tool-call approval as it exists on disk: what was asked, and — once a
/// client decides — what was answered.
///
/// An approval outlives the daemon that requested it. The run it belongs to
/// does not: its child process dies with the daemon, so the run is recorded
/// interrupted while the approval stays readable and resolvable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ToolCallApprovalRecord {
    pub(crate) run_id: String,
    pub(crate) call_id: String,
    pub(crate) session_id: String,
    pub(crate) tool_name: String,
    pub(crate) effect: EffectClass,
    pub(crate) reason: String,
    pub(crate) input_preview: Option<String>,
    pub(crate) approval_preview: Option<String>,
    pub(crate) diff_preview: Option<String>,
    pub(crate) requested_at_ms: u64,
    pub(crate) decision: Option<ToolCallApprovalDecision>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ToolCallApprovalDecision {
    pub(crate) granted: bool,
    pub(crate) actor: String,
    pub(crate) reason: Option<String>,
    pub(crate) decided_at_ms: u64,
}

/// A thread authority record proven to be durably written.
///
/// The type exists so a caller cannot mistake an in-memory record for one that
/// survived the write D005 requires before the first turn executes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DurableThreadAuthority(ThreadAuthorityRecord);

impl DurableThreadAuthority {
    pub(crate) fn record(&self) -> &ThreadAuthorityRecord {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BranchClaimRecord {
    pub(crate) workspace_id: String,
    pub(crate) repo: String,
    pub(crate) branch: String,
    pub(crate) thread_id: String,
    pub(crate) claimed_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("branch {branch} in repository {repo} is already claimed by live thread {thread_id}")]
pub(crate) struct BranchClaimConflict {
    pub(crate) repo: String,
    pub(crate) branch: String,
    pub(crate) thread_id: String,
}

pub(crate) struct ServerStore {
    connection: Connection,
    path: PathBuf,
}

impl ServerStore {
    /// Open the server-wide store, creating it and its schema if absent.
    pub(crate) fn open_or_create(path: &Path) -> AppResult<Self> {
        if path.as_os_str().is_empty() {
            return Err(AppError::EmptyLedger);
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut connection = Connection::open(path)?;
        connection.busy_timeout(SQLITE_BUSY_TIMEOUT)?;
        let journal_mode: String =
            connection.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
        if journal_mode != "wal" {
            connection.pragma_update(None, "journal_mode", "WAL")?;
        }
        connection.pragma_update(None, "synchronous", "FULL")?;
        create_thread_authority_tables(&mut connection)?;
        create_thread_stop_table(&connection)?;
        create_thread_repository_tables(&connection)?;
        create_workspace_table(&connection)?;
        create_agent_table(&connection)?;
        create_tool_call_approval_table(&connection)?;
        Ok(Self {
            connection,
            path: path.to_path_buf(),
        })
    }

    /// Open the store without the ability to write, and without creating it.
    ///
    /// Readback that cannot mutate is how the immutability of authority
    /// records is proven rather than asserted.
    #[cfg(test)]
    pub(crate) fn open_readonly(path: &Path) -> AppResult<Self> {
        let connection =
            Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        connection.busy_timeout(SQLITE_BUSY_TIMEOUT)?;
        Ok(Self {
            connection,
            path: path.to_path_buf(),
        })
    }

    /// Register a workspace while atomically excluding duplicate names and roots.
    ///
    /// The boolean is true only for the caller that inserted the returned row.
    pub(crate) fn register_workspace(
        &mut self,
        id: &str,
        name: &str,
        root: &str,
        ledger_path: &str,
        now_ms: u64,
    ) -> AppResult<(WorkspaceRecord, bool)> {
        for (field, value) in [
            ("id", id),
            ("name", name),
            ("root", root),
            ("ledger path", ledger_path),
        ] {
            if value.is_empty() {
                return Err(AppError::Config(format!(
                    "workspace {field} must not be empty"
                )));
            }
        }
        let store_path = self.path.clone();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = transaction
            .query_row(
                "SELECT id, name, root, ledger_path, created_at_ms
                   FROM workspaces WHERE name = ?1",
                params![name],
                workspace_from_row,
            )
            .optional()?
        {
            transaction.commit()?;
            return Ok((existing, false));
        }
        let root = Path::new(root).canonicalize()?;
        let root = root.to_string_lossy().into_owned();
        if let Some(existing) = transaction
            .query_row(
                "SELECT id, name, root, ledger_path, created_at_ms
                   FROM workspaces WHERE root = ?1
                   ORDER BY created_at_ms, name LIMIT 1",
                params![root],
                workspace_from_row,
            )
            .optional()?
        {
            transaction.commit()?;
            return Ok((existing, false));
        }
        let record = WorkspaceRecord {
            id: id.to_owned(),
            name: name.to_owned(),
            root,
            ledger_path: ledger_path.to_owned(),
            created_at_ms: now_ms,
        };
        let created_at_ms = sqlite_i64(now_ms, "workspace created_at_ms")?;
        let legacy_path = paths::legacy_sqlite_path_at(&store_path, Path::new(&record.root))?;
        let adopted = paths::adopt_legacy_sqlite(
            &store_path,
            Path::new(&record.root),
            Path::new(&record.ledger_path),
        )?;
        if let Err(error) = transaction.execute(
            "INSERT INTO workspaces (id, name, root, ledger_path, created_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                record.id,
                record.name,
                record.root,
                record.ledger_path,
                created_at_ms
            ],
        ) {
            drop(transaction);
            return Err(rollback_adopted_ledger(
                &legacy_path,
                Path::new(&record.ledger_path),
                adopted,
                error.into(),
            ));
        }
        if let Err(error) = transaction.commit() {
            return Err(rollback_adopted_ledger(
                &legacy_path,
                Path::new(&record.ledger_path),
                adopted,
                error.into(),
            ));
        }
        Ok((record, true))
    }

    /// Point a workspace at a new directory without disturbing its identity.
    ///
    /// Moving a workspace is a registry update, never a new workspace (P021).
    #[allow(dead_code)]
    pub(crate) fn relocate_workspace(&self, id: &str, root: &str) -> AppResult<bool> {
        let root = Path::new(root).canonicalize()?;
        let changed = self.connection.execute(
            "UPDATE workspaces SET root = ?2 WHERE id = ?1",
            params![id, root.to_string_lossy()],
        )?;
        Ok(changed == 1)
    }

    pub(crate) fn workspace(&self, id: &str) -> AppResult<Option<WorkspaceRecord>> {
        Ok(self
            .connection
            .query_row(
                "SELECT id, name, root, ledger_path, created_at_ms
                   FROM workspaces WHERE id = ?1",
                params![id],
                workspace_from_row,
            )
            .optional()?)
    }

    pub(crate) fn workspace_by_name(&self, name: &str) -> AppResult<Option<WorkspaceRecord>> {
        Ok(self
            .connection
            .query_row(
                "SELECT id, name, root, ledger_path, created_at_ms
                   FROM workspaces WHERE name = ?1",
                params![name],
                workspace_from_row,
            )
            .optional()?)
    }

    pub(crate) fn workspace_by_root(&self, root: &str) -> AppResult<Option<WorkspaceRecord>> {
        Ok(self
            .connection
            .query_row(
                "SELECT id, name, root, ledger_path, created_at_ms
                   FROM workspaces WHERE root = ?1
                   ORDER BY created_at_ms, name LIMIT 1",
                params![root],
                workspace_from_row,
            )
            .optional()?)
    }

    /// Resolve one registered workspace and migrate its pre-minted ledger path.
    ///
    /// The immediate transaction serializes the filesystem move with the row
    /// update, so concurrent attachers either observe the old complete state
    /// or the new complete state. A failed update restores the legacy files.
    pub(crate) fn workspace_for_attachment(
        &mut self,
        root: &str,
    ) -> AppResult<Option<WorkspaceRecord>> {
        let store_path = self.path.clone();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(mut record) = transaction
            .query_row(
                "SELECT id, name, root, ledger_path, created_at_ms
                   FROM workspaces WHERE root = ?1
                   ORDER BY created_at_ms, name LIMIT 1",
                params![root],
                workspace_from_row,
            )
            .optional()?
        else {
            transaction.commit()?;
            return Ok(None);
        };
        let state_root = store_path.parent().ok_or_else(|| {
            AppError::Config(format!(
                "server database has no state root: {}",
                store_path.display()
            ))
        })?;
        let legacy_path = PathBuf::from(&record.ledger_path);
        if legacy_path.file_name() != Some(std::ffi::OsStr::new("agent.db"))
            || legacy_path.parent().and_then(Path::parent)
                != Some(state_root.join("workspaces").as_path())
        {
            transaction.commit()?;
            return Ok(Some(record));
        }

        let destination = paths::workspace_sqlite_path(&store_path, &record.id)?;
        let adopted = paths::move_sqlite_files(&legacy_path, &destination)?;
        let destination = destination.to_string_lossy().into_owned();
        let changed = match transaction.execute(
            "UPDATE workspaces SET ledger_path = ?2
              WHERE id = ?1 AND ledger_path = ?3",
            params![record.id, destination, record.ledger_path],
        ) {
            Ok(changed) => changed,
            Err(error) => {
                drop(transaction);
                return Err(rollback_adopted_ledger(
                    &legacy_path,
                    Path::new(&destination),
                    adopted,
                    error.into(),
                ));
            }
        };
        if changed != 1 {
            drop(transaction);
            return Err(rollback_adopted_ledger(
                &legacy_path,
                Path::new(&destination),
                adopted,
                AppError::Config(format!(
                    "workspace ledger migration updated {changed} rows for {}",
                    record.id
                )),
            ));
        }
        if let Err(error) = transaction.commit() {
            return Err(rollback_adopted_ledger(
                &legacy_path,
                Path::new(&destination),
                adopted,
                error.into(),
            ));
        }
        record.ledger_path = destination;
        Ok(Some(record))
    }

    /// Every registered workspace, whether or not a client has it open.
    ///
    pub(crate) fn workspaces(&self) -> AppResult<Vec<WorkspaceRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT id, name, root, ledger_path, created_at_ms
               FROM workspaces ORDER BY created_at_ms, name",
        )?;
        let rows = statement.query_map([], workspace_from_row)?;
        let mut records = Vec::new();
        for row in rows {
            records.push(row?);
        }
        Ok(records)
    }

    /// Insert one complete profile after its workspace and toolset are validated.
    pub(crate) fn register_agent(&self, record: &AgentRecord) -> AppResult<bool> {
        let toolset = serde_json::to_string(&record.toolset)?;
        let changed = self.connection.execute(
            "INSERT INTO agents
               (id, workspace_id, model, reasoning_effort, approval_policy,
                toolset, created_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(id) DO NOTHING",
            params![
                record.id.as_str(),
                record.workspace_id,
                record.model,
                record.reasoning_effort.as_str(),
                record.approval_policy.as_str(),
                toolset,
                sqlite_i64(record.created_at_ms, "agent created_at_ms")?,
            ],
        )?;
        Ok(changed == 1)
    }

    pub(crate) fn agent(&self, id: &AgentId) -> AppResult<Option<AgentRecord>> {
        Ok(self
            .connection
            .query_row(
                "SELECT id, workspace_id, model, reasoning_effort,
                        approval_policy, toolset, created_at_ms
                   FROM agents WHERE id = ?1",
                params![id.as_str()],
                agent_from_row,
            )
            .optional()?)
    }

    pub(crate) fn agents(&self) -> AppResult<Vec<AgentRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT id, workspace_id, model, reasoning_effort,
                    approval_policy, toolset, created_at_ms
               FROM agents ORDER BY created_at_ms, id",
        )?;
        Ok(statement
            .query_map([], agent_from_row)?
            .collect::<Result<Vec<_>, _>>()?)
    }

    /// Record a tool-call approval before the run blocks on it.
    ///
    /// Written before the request is announced to clients, so a daemon that
    /// dies between announcing and deciding still leaves the ask on disk.
    /// Re-requesting the same call is idempotent: the original request and any
    /// decision already made both stand.
    pub(crate) fn persist_tool_call_approval(
        &self,
        record: &ToolCallApprovalRecord,
    ) -> AppResult<()> {
        self.connection.execute(
            "INSERT INTO tool_call_approvals
               (run_id, call_id, session_id, tool_name, effect, reason,
                input_preview, approval_preview, diff_preview, requested_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT(run_id, call_id) DO NOTHING",
            params![
                record.run_id,
                record.call_id,
                record.session_id,
                record.tool_name,
                effect_to_text(&record.effect)?,
                record.reason,
                record.input_preview,
                record.approval_preview,
                record.diff_preview,
                sqlite_i64(record.requested_at_ms, "approval requested_at_ms")?,
            ],
        )?;
        Ok(())
    }

    /// Record the decision for a tool-call approval.
    ///
    /// Returns false when the approval was already decided, so a late second
    /// decider learns it lost rather than silently overwriting the first.
    pub(crate) fn resolve_tool_call_approval(
        &self,
        run_id: &str,
        call_id: &str,
        decision: &ToolCallApprovalDecision,
    ) -> AppResult<bool> {
        let changed = self.connection.execute(
            "UPDATE tool_call_approvals
                SET decision = ?3, decided_by = ?4, decision_reason = ?5, decided_at_ms = ?6
              WHERE run_id = ?1 AND call_id = ?2 AND decision IS NULL",
            params![
                run_id,
                call_id,
                if decision.granted {
                    "granted"
                } else {
                    "denied"
                },
                decision.actor,
                decision.reason,
                sqlite_i64(decision.decided_at_ms, "approval decided_at_ms")?,
            ],
        )?;
        Ok(changed == 1)
    }

    /// Every approval still awaiting a decision, across all workspaces.
    ///
    /// This is the away-from-terminal question — "what is waiting on me?" —
    /// which is why approvals live in the server tier rather than in one
    /// workspace's ledger.
    pub(crate) fn pending_tool_call_approvals(&self) -> AppResult<Vec<ToolCallApprovalRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT run_id, call_id, session_id, tool_name, effect, reason,
                    input_preview, approval_preview, diff_preview, requested_at_ms,
                    decision, decided_by, decision_reason, decided_at_ms
               FROM tool_call_approvals
              WHERE decision IS NULL
              ORDER BY requested_at_ms, run_id, call_id",
        )?;
        let rows = statement.query_map([], tool_call_approval_from_row)?;
        let mut records = Vec::new();
        for row in rows {
            records.push(row??);
        }
        Ok(records)
    }

    /// Read one approval, decided or not. Tests assert the decided shape;
    /// the live restore path enumerates instead, because it has a run id and
    /// not a call id.
    #[cfg(test)]
    pub(crate) fn tool_call_approval(
        &self,
        run_id: &str,
        call_id: &str,
    ) -> AppResult<Option<ToolCallApprovalRecord>> {
        self.connection
            .query_row(
                "SELECT run_id, call_id, session_id, tool_name, effect, reason,
                        input_preview, approval_preview, diff_preview, requested_at_ms,
                        decision, decided_by, decision_reason, decided_at_ms
                   FROM tool_call_approvals
                  WHERE run_id = ?1 AND call_id = ?2",
                params![run_id, call_id],
                tool_call_approval_from_row,
            )
            .optional()?
            .transpose()
    }

    pub(crate) fn persist_thread_spawn(
        &mut self,
        approval: &ThreadSpawnApprovalRecord,
        authority: Option<&ThreadAuthorityRecord>,
        confinement: Option<ThreadConfinement>,
    ) -> AppResult<Option<DurableThreadAuthority>> {
        if authority.is_none() && confinement.is_some() {
            return Err(AppError::Config(
                "thread confinement requires a granted authority record".into(),
            ));
        }
        if let Some(authority) = authority {
            validate_complete_authority(authority)?;
            if authority.spawning_actor != approval.actor {
                return Err(AppError::Config(
                    "thread.spawn approval actor must match spawning actor".into(),
                ));
            }
        }
        match (approval.decision, authority) {
            (ThreadSpawnDecisionName::Granted, Some(authority))
                if authority.thread_id == approval.thread_id => {}
            (ThreadSpawnDecisionName::Granted, Some(_)) => {
                return Err(AppError::Config(
                    "thread.spawn approval and authority thread ids differ".into(),
                ));
            }
            (ThreadSpawnDecisionName::Granted, None) => {
                return Err(AppError::Config(
                    "granted thread.spawn approval requires an authority record".into(),
                ));
            }
            (ThreadSpawnDecisionName::Denied | ThreadSpawnDecisionName::Canceled, None) => {}
            (ThreadSpawnDecisionName::Denied | ThreadSpawnDecisionName::Canceled, Some(_)) => {
                return Err(AppError::Config(
                    "denied or canceled thread.spawn cannot create authority".into(),
                ));
            }
        }

        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = thread_spawn_approval_from(&transaction, &approval.spawn_id)? {
            if existing != *approval {
                return Err(AppError::Config(format!(
                    "thread.spawn decision conflicts with durable spawn {}",
                    approval.spawn_id
                )));
            }
            if let Some(authority) = authority {
                let existing_authority = thread_authority_from(&transaction, &authority.thread_id)?
                    .ok_or_else(|| {
                        AppError::Config(format!(
                            "granted spawn {} has no authority record",
                            approval.spawn_id
                        ))
                    })?;
                if existing_authority != *authority {
                    return Err(AppError::Config(format!(
                        "thread authority conflicts with durable thread {}",
                        authority.thread_id
                    )));
                }
            }
            let durable_confinement = thread_confinement_from(&transaction, &approval.thread_id)?;
            if confinement.is_some() && confinement != durable_confinement {
                return Err(AppError::Config(format!(
                    "thread confinement conflicts with durable thread {}",
                    approval.thread_id
                )));
            }
            transaction.commit()?;
            return Ok(authority.cloned().map(DurableThreadAuthority));
        }

        if let Some(authority) = authority
            && let Some(parent_thread_id) = authority.parent_thread_id.as_deref()
        {
            let parent =
                thread_authority_from(&transaction, parent_thread_id)?.ok_or_else(|| {
                    AppError::Config(format!(
                        "parent thread is no longer durable: {parent_thread_id}"
                    ))
                })?;
            let cwd = authority_working_directory(authority).ok_or_else(|| {
                AppError::Config("thread authority has no working directory".into())
            })?;
            let draft = crate::thread_authority::ThreadAuthorityDraft {
                thread_id: authority.thread_id.clone(),
                parent_thread_id: authority.parent_thread_id.clone(),
                cwd: cwd.to_string_lossy().into_owned(),
                agent_id: authority
                    .agent_id
                    .clone()
                    .expect("complete authority has an agent id"),
                model: authority.model.clone(),
                reasoning_effort: authority.reasoning_effort,
                approval_policy: authority.approval_policy,
                toolset: authority.toolset.clone(),
                worktrees: authority.worktrees.clone(),
                repositories: Vec::new(),
                granted_paths: authority.granted_paths.clone(),
                network: authority.network,
            };
            validate_child_authority(&parent, &draft)
                .map_err(|error| AppError::Config(error.to_string()))?;
        }

        transaction.execute(
            "INSERT INTO thread_spawn_approvals
               (spawn_id, thread_id, decision, actor, reason, occurred_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                approval.spawn_id,
                approval.thread_id,
                approval.decision.as_str(),
                approval.actor,
                approval.reason,
                sqlite_i64(approval.occurred_at_ms, "thread approval occurred_at_ms")?
            ],
        )?;
        if let Some(authority) = authority {
            let toolset = serde_json::to_string(&authority.toolset)?;
            let worktrees = serde_json::to_string(&authority.worktrees)?;
            let granted_paths = serde_json::to_string(&authority.granted_paths)?;
            transaction.execute(
                "INSERT INTO thread_authorities
                   (thread_id, parent_thread_id, spawning_actor, agent_id, model,
                    reasoning_effort, approval_policy, toolset, worktrees,
                    granted_paths, network, created_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    authority.thread_id,
                    authority.parent_thread_id,
                    authority.spawning_actor,
                    authority
                        .agent_id
                        .as_ref()
                        .map(AgentId::as_str)
                        .expect("complete authority has an agent id"),
                    authority.model,
                    authority.reasoning_effort.as_str(),
                    authority.approval_policy.as_str(),
                    toolset,
                    worktrees,
                    granted_paths,
                    authority.network,
                    sqlite_i64(authority.created_at_ms, "thread created_at_ms")?
                ],
            )?;
            if let Some(confinement) = confinement {
                transaction.execute(
                    "INSERT INTO thread_confinements (thread_id, backend, recorded_at_ms)
                     VALUES (?1, ?2, ?3)",
                    params![
                        authority.thread_id,
                        confinement.as_str(),
                        sqlite_i64(approval.occurred_at_ms, "thread confinement recorded_at_ms")?
                    ],
                )?;
            }
        }
        transaction.commit()?;
        Ok(authority.cloned().map(DurableThreadAuthority))
    }

    pub(crate) fn thread_authority(
        &self,
        thread_id: &str,
    ) -> AppResult<Option<ThreadAuthorityRecord>> {
        thread_authority_from(&self.connection, thread_id)
    }

    pub(crate) fn thread_authorities(&self) -> AppResult<Vec<ThreadAuthorityRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT thread_id, parent_thread_id, spawning_actor, cwd, agent_id,
                    model, reasoning_effort, approval_policy, toolset, worktrees,
                    granted_paths, network, created_at_ms
             FROM thread_authorities
             ORDER BY created_at_ms ASC, thread_id ASC",
        )?;
        Ok(statement
            .query_map([], thread_authority_from_row)?
            .collect::<Result<Vec<_>, _>>()?)
    }

    pub(crate) fn thread_spawn_approval(
        &self,
        spawn_id: &str,
    ) -> AppResult<Option<ThreadSpawnApprovalRecord>> {
        thread_spawn_approval_from(&self.connection, spawn_id)
    }

    pub(crate) fn thread_confinement(
        &self,
        thread_id: &str,
    ) -> AppResult<Option<ThreadConfinement>> {
        thread_confinement_from(&self.connection, thread_id)
    }

    pub(crate) fn claim_thread_branches(
        &mut self,
        workspace_id: &str,
        thread_id: &str,
        claims: &[(String, String)],
        claimed_at_ms: u64,
    ) -> AppResult<Option<BranchClaimConflict>> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        for (repo, branch) in claims {
            let existing = transaction
                .query_row(
                    "SELECT thread_id FROM thread_branch_claims
                     WHERE workspace_id = ?1 AND repo = ?2 AND branch = ?3",
                    params![workspace_id, repo, branch],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if let Some(existing) = existing {
                if existing != thread_id {
                    return Ok(Some(BranchClaimConflict {
                        repo: repo.clone(),
                        branch: branch.clone(),
                        thread_id: existing,
                    }));
                }
                continue;
            }
            transaction.execute(
                "INSERT INTO thread_branch_claims
                   (workspace_id, repo, branch, thread_id, claimed_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    workspace_id,
                    repo,
                    branch,
                    thread_id,
                    sqlite_i64(claimed_at_ms, "branch claim claimed_at_ms")?
                ],
            )?;
        }
        transaction.commit()?;
        Ok(None)
    }

    pub(crate) fn branch_claims(&self) -> AppResult<Vec<BranchClaimRecord>> {
        let mut statement = self.connection.prepare(
            "SELECT workspace_id, repo, branch, thread_id, claimed_at_ms
             FROM thread_branch_claims
             ORDER BY claimed_at_ms, workspace_id, repo, branch",
        )?;
        Ok(statement
            .query_map([], |row| {
                Ok(BranchClaimRecord {
                    workspace_id: row.get(0)?,
                    repo: row.get(1)?,
                    branch: row.get(2)?,
                    thread_id: row.get(3)?,
                    claimed_at_ms: row_u64(row, 4, "branch claim claimed_at_ms")?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?)
    }

    pub(crate) fn release_thread_claims(&mut self, thread_id: &str) -> AppResult<()> {
        self.connection.execute(
            "DELETE FROM thread_branch_claims WHERE thread_id = ?1",
            params![thread_id],
        )?;
        Ok(())
    }

    pub(crate) fn persist_thread_stop(
        &mut self,
        stop: &ThreadStopRecord,
    ) -> AppResult<(ThreadStopRecord, bool)> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = thread_stop_from(&transaction, &stop.thread_id)? {
            transaction.commit()?;
            return Ok((existing, false));
        }
        if thread_authority_from(&transaction, &stop.thread_id)?.is_none() {
            return Err(AppError::Config(format!(
                "thread stop has no durable authority: {}",
                stop.thread_id
            )));
        }
        transaction.execute(
            "INSERT INTO thread_stops (thread_id, actor, stopped_turn_id, occurred_at_ms)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                stop.thread_id,
                stop.actor,
                stop.stopped_turn_id,
                sqlite_i64(stop.occurred_at_ms, "thread stop occurred_at_ms")?
            ],
        )?;
        transaction.execute(
            "DELETE FROM thread_branch_claims WHERE thread_id = ?1",
            params![stop.thread_id],
        )?;
        transaction.commit()?;
        Ok((stop.clone(), true))
    }

    pub(crate) fn thread_stop(&self, thread_id: &str) -> AppResult<Option<ThreadStopRecord>> {
        thread_stop_from(&self.connection, thread_id)
    }
}

fn rollback_adopted_ledger(
    source: &Path,
    destination: &Path,
    adopted: bool,
    operation_error: AppError,
) -> AppError {
    if !adopted {
        return operation_error;
    }
    match paths::move_sqlite_files(destination, source) {
        Ok(true) => operation_error,
        Ok(false) => AppError::Config(format!(
            "workspace ledger operation failed ({operation_error}) and adoption rollback found no destination ledger"
        )),
        Err(rollback_error) => AppError::Config(format!(
            "workspace ledger operation failed ({operation_error}) and adoption rollback failed ({rollback_error})"
        )),
    }
}

fn thread_authority_from(
    connection: &Connection,
    thread_id: &str,
) -> AppResult<Option<ThreadAuthorityRecord>> {
    Ok(connection
        .query_row(
            "SELECT thread_id, parent_thread_id, spawning_actor, cwd, agent_id,
                    model, reasoning_effort, approval_policy, toolset, worktrees,
                    granted_paths, network, created_at_ms
             FROM thread_authorities
             WHERE thread_id = ?1",
            params![thread_id],
            thread_authority_from_row,
        )
        .optional()?)
}

fn thread_authority_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ThreadAuthorityRecord> {
    let legacy_cwd: Option<String> = row.get(3)?;
    let agent_value: Option<String> = row.get(4)?;
    let agent_id = agent_value
        .map(AgentId::new)
        .transpose()
        .map_err(|error| invalid_thread_column(4, error.to_string()))?;
    let reasoning_value: String = row.get(6)?;
    let reasoning_effort = ReasoningEffort::parse(&reasoning_value).ok_or_else(|| {
        invalid_thread_column(6, format!("unknown reasoning effort: {reasoning_value}"))
    })?;
    let policy_value: String = row.get(7)?;
    let approval_policy = ThreadApprovalPolicy::parse(&policy_value).ok_or_else(|| {
        invalid_thread_column(7, format!("unknown approval policy: {policy_value}"))
    })?;
    let toolset = row
        .get::<_, Option<String>>(8)?
        .map(|value| {
            serde_json::from_str::<Vec<String>>(&value)
                .map_err(|error| invalid_thread_column(8, error.to_string()))
        })
        .transpose()?
        .unwrap_or_default();
    let worktrees = row
        .get::<_, Option<String>>(9)?
        .map(|value| {
            serde_json::from_str::<Vec<ThreadWorktree>>(&value)
                .map_err(|error| invalid_thread_column(9, error.to_string()))
        })
        .transpose()?
        .unwrap_or_default();
    let granted_paths = row
        .get::<_, Option<String>>(10)?
        .map(|value| {
            serde_json::from_str::<Vec<ThreadGrantedPath>>(&value)
                .map_err(|error| invalid_thread_column(10, error.to_string()))
        })
        .transpose()?
        .unwrap_or_else(|| {
            legacy_cwd
                .map(|path| {
                    vec![ThreadGrantedPath {
                        path,
                        writable: true,
                    }]
                })
                .unwrap_or_default()
        });
    let network = match row.get::<_, Option<i64>>(11)? {
        None | Some(0) => false,
        Some(1) => true,
        Some(value) => {
            return Err(invalid_thread_column(
                11,
                format!("invalid network flag: {value}"),
            ));
        }
    };
    Ok(ThreadAuthorityRecord {
        thread_id: row.get(0)?,
        parent_thread_id: row.get(1)?,
        spawning_actor: row.get(2)?,
        agent_id,
        model: row.get(5)?,
        reasoning_effort,
        approval_policy,
        toolset,
        worktrees,
        granted_paths,
        network,
        created_at_ms: row_u64(row, 12, "thread created_at_ms")?,
    })
}

fn thread_spawn_approval_from(
    connection: &Connection,
    spawn_id: &str,
) -> AppResult<Option<ThreadSpawnApprovalRecord>> {
    Ok(connection
        .query_row(
            "SELECT spawn_id, thread_id, decision, actor, reason, occurred_at_ms
             FROM thread_spawn_approvals
             WHERE spawn_id = ?1",
            params![spawn_id],
            |row| {
                let decision_value: String = row.get(2)?;
                let decision =
                    ThreadSpawnDecisionName::parse(&decision_value).ok_or_else(|| {
                        invalid_thread_column(
                            2,
                            format!("unknown thread spawn decision: {decision_value}"),
                        )
                    })?;
                Ok(ThreadSpawnApprovalRecord {
                    spawn_id: row.get(0)?,
                    thread_id: row.get(1)?,
                    decision,
                    actor: row.get(3)?,
                    reason: row.get(4)?,
                    occurred_at_ms: row_u64(row, 5, "thread approval occurred_at_ms")?,
                })
            },
        )
        .optional()?)
}

fn thread_confinement_from(
    connection: &Connection,
    thread_id: &str,
) -> AppResult<Option<ThreadConfinement>> {
    let value = connection
        .query_row(
            "SELECT backend FROM thread_confinements WHERE thread_id = ?1",
            params![thread_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    value
        .map(|value| {
            ThreadConfinement::parse(&value).ok_or_else(|| {
                AppError::Config(format!("unknown thread confinement backend: {value}"))
            })
        })
        .transpose()
}

fn thread_stop_from(
    connection: &Connection,
    thread_id: &str,
) -> AppResult<Option<ThreadStopRecord>> {
    let record = connection
        .query_row(
            "SELECT thread_id, actor, stopped_turn_id, occurred_at_ms
             FROM thread_stops
             WHERE thread_id = ?1",
            params![thread_id],
            |row| {
                Ok(ThreadStopRecord {
                    thread_id: row.get(0)?,
                    actor: row.get(1)?,
                    stopped_turn_id: row.get(2)?,
                    occurred_at_ms: row_u64(row, 3, "thread stop occurred_at_ms")?,
                })
            },
        )
        .optional()?;
    match record {
        Some(record) => Ok(Some(ThreadStopRecord::new(
            record.thread_id,
            record.actor,
            record.stopped_turn_id,
            record.occurred_at_ms,
        )?)),
        None => Ok(None),
    }
}

fn invalid_thread_column(index: usize, message: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        index,
        Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            message,
        )),
    )
}

fn effect_to_text(effect: &EffectClass) -> AppResult<String> {
    match serde_json::to_value(effect) {
        Ok(serde_json::Value::String(text)) => Ok(text),
        _ => Err(AppError::Config("effect class is not a string".into())),
    }
}

fn effect_from_text(text: &str) -> AppResult<EffectClass> {
    serde_json::from_value(serde_json::Value::String(text.to_owned()))
        .map_err(|_| AppError::Config(format!("unknown effect class: {text}")))
}

/// The outer Result is the row read; the inner one is the effect class, which
/// can only be validated after the text leaves SQLite.
fn tool_call_approval_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<AppResult<ToolCallApprovalRecord>> {
    let effect_text: String = row.get(4)?;
    let decision: Option<String> = row.get(10)?;
    let decided_by: Option<String> = row.get(11)?;
    let decision_reason: Option<String> = row.get(12)?;
    let decided_at_ms: Option<i64> = row.get(13)?;
    let record = ToolCallApprovalRecord {
        run_id: row.get(0)?,
        call_id: row.get(1)?,
        session_id: row.get(2)?,
        tool_name: row.get(3)?,
        effect: EffectClass::ReadOnly,
        reason: row.get(5)?,
        input_preview: row.get(6)?,
        approval_preview: row.get(7)?,
        diff_preview: row.get(8)?,
        requested_at_ms: row_u64(row, 9, "approval requested_at_ms")?,
        decision: match (decision, decided_by, decided_at_ms) {
            (Some(decision), Some(actor), Some(at_ms)) => Some(ToolCallApprovalDecision {
                granted: decision == "granted",
                actor,
                reason: decision_reason,
                decided_at_ms: at_ms.max(0) as u64,
            }),
            _ => None,
        },
    };
    Ok(effect_from_text(&effect_text).map(|effect| ToolCallApprovalRecord { effect, ..record }))
}

fn workspace_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<WorkspaceRecord> {
    Ok(WorkspaceRecord {
        id: row.get(0)?,
        name: row.get(1)?,
        root: row.get(2)?,
        ledger_path: row.get(3)?,
        created_at_ms: row_u64(row, 4, "workspace created_at_ms")?,
    })
}

fn agent_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AgentRecord> {
    let id = AgentId::new(row.get::<_, String>(0)?)
        .map_err(|error| invalid_agent_column(0, error.to_string()))?;
    let reasoning_value: String = row.get(3)?;
    let reasoning_effort = ReasoningEffort::parse(&reasoning_value).ok_or_else(|| {
        invalid_agent_column(3, format!("unknown reasoning effort: {reasoning_value}"))
    })?;
    let policy_value: String = row.get(4)?;
    let approval_policy = ThreadApprovalPolicy::parse(&policy_value).ok_or_else(|| {
        invalid_agent_column(4, format!("unknown approval policy: {policy_value}"))
    })?;
    let toolset = serde_json::from_str::<Vec<String>>(&row.get::<_, String>(5)?)
        .map_err(|error| invalid_agent_column(5, error.to_string()))?;
    Ok(AgentRecord {
        id,
        workspace_id: row.get(1)?,
        model: row.get(2)?,
        reasoning_effort,
        approval_policy,
        toolset,
        created_at_ms: row_u64(row, 6, "agent created_at_ms")?,
    })
}

fn invalid_agent_column(index: usize, message: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(index, Type::Text, message.into())
}

/// Mint a workspace id that does not depend on where the workspace lives.
///
/// Deliberately not derived from the path, unlike `paths::workspace_id` (P021):
/// a workspace that moves must keep its identity and its history rather than
/// silently becoming a new, empty one.
pub(crate) fn mint_workspace_id(name: &str, created_at_ms: u64) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(name.as_bytes());
    hasher.update(b"\0");
    hasher.update(created_at_ms.to_be_bytes());
    let digest = hasher.finalize();
    let hex: String = digest
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("ws-{hex}")
}

impl WorkspaceRecord {
    /// A workspace is broken when the directory it points at is gone.
    ///
    /// Checked at read time rather than stored, because the filesystem can
    /// change without the server running; a cached flag would lie.
    pub(crate) fn health(&self) -> WorkspaceHealth {
        if Path::new(&self.root).is_dir() {
            WorkspaceHealth::Present
        } else {
            WorkspaceHealth::Broken
        }
    }
}

fn create_workspace_table(connection: &Connection) -> AppResult<()> {
    connection.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS workspaces (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          root TEXT NOT NULL,
          ledger_path TEXT NOT NULL,
          created_at_ms INTEGER NOT NULL
        );

        CREATE TRIGGER IF NOT EXISTS workspaces_identity_is_immutable
        BEFORE UPDATE ON workspaces
        WHEN OLD.id IS NOT NEW.id OR OLD.created_at_ms IS NOT NEW.created_at_ms
        BEGIN
          SELECT RAISE(ABORT, 'workspace identity is immutable');
        END;
        "#,
    )?;
    Ok(())
}

fn create_agent_table(connection: &Connection) -> AppResult<()> {
    connection.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS agents (
          id TEXT PRIMARY KEY,
          workspace_id TEXT NOT NULL,
          model TEXT NOT NULL,
          reasoning_effort TEXT NOT NULL,
          approval_policy TEXT NOT NULL,
          toolset TEXT NOT NULL,
          created_at_ms INTEGER NOT NULL
        );

        CREATE TRIGGER IF NOT EXISTS agents_no_update
        BEFORE UPDATE ON agents
        BEGIN
          SELECT RAISE(ABORT, 'agent records are immutable');
        END;

        CREATE TRIGGER IF NOT EXISTS agents_no_delete
        BEFORE DELETE ON agents
        BEGIN
          SELECT RAISE(ABORT, 'agent records are immutable');
        END;
        "#,
    )?;
    Ok(())
}

/// Pending tool-call approvals.
///
/// The request half is immutable, exactly like a spawn approval: what was
/// asked can never be rewritten. The decision half starts empty and is
/// written once, so an approval that outlives its daemon can still be
/// resolved and recorded afterwards.
fn create_tool_call_approval_table(connection: &Connection) -> AppResult<()> {
    connection.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS tool_call_approvals (
          run_id TEXT NOT NULL,
          call_id TEXT NOT NULL,
          session_id TEXT NOT NULL,
          tool_name TEXT NOT NULL,
          effect TEXT NOT NULL,
          reason TEXT NOT NULL,
          input_preview TEXT,
          approval_preview TEXT,
          diff_preview TEXT,
          requested_at_ms INTEGER NOT NULL,
          decision TEXT,
          decided_by TEXT,
          decision_reason TEXT,
          decided_at_ms INTEGER,
          PRIMARY KEY (run_id, call_id)
        );

        CREATE TRIGGER IF NOT EXISTS tool_call_approvals_request_is_immutable
        BEFORE UPDATE ON tool_call_approvals
        WHEN OLD.run_id IS NOT NEW.run_id
          OR OLD.call_id IS NOT NEW.call_id
          OR OLD.session_id IS NOT NEW.session_id
          OR OLD.tool_name IS NOT NEW.tool_name
          OR OLD.effect IS NOT NEW.effect
          OR OLD.reason IS NOT NEW.reason
          OR OLD.input_preview IS NOT NEW.input_preview
          OR OLD.approval_preview IS NOT NEW.approval_preview
          OR OLD.diff_preview IS NOT NEW.diff_preview
          OR OLD.requested_at_ms IS NOT NEW.requested_at_ms
        BEGIN
          SELECT RAISE(ABORT, 'tool call approval requests are immutable');
        END;

        CREATE TRIGGER IF NOT EXISTS tool_call_approvals_decide_once
        BEFORE UPDATE ON tool_call_approvals
        WHEN OLD.decision IS NOT NULL AND NEW.decision IS NOT OLD.decision
        BEGIN
          SELECT RAISE(ABORT, 'tool call approval decisions are immutable');
        END;

        CREATE TRIGGER IF NOT EXISTS tool_call_approvals_no_delete
        BEFORE DELETE ON tool_call_approvals
        BEGIN
          SELECT RAISE(ABORT, 'tool call approvals are immutable');
        END;
        "#,
    )?;
    Ok(())
}

fn create_thread_authority_tables(connection: &mut Connection) -> AppResult<()> {
    connection.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS thread_authorities (
          thread_id TEXT PRIMARY KEY,
          parent_thread_id TEXT,
          spawning_actor TEXT NOT NULL,
          cwd TEXT,
          agent_id TEXT,
          model TEXT NOT NULL,
          reasoning_effort TEXT NOT NULL,
          approval_policy TEXT NOT NULL,
          toolset TEXT,
          worktrees TEXT,
          granted_paths TEXT,
          network INTEGER,
          created_at_ms INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS thread_spawn_approvals (
          spawn_id TEXT PRIMARY KEY,
          thread_id TEXT NOT NULL,
          decision TEXT NOT NULL,
          actor TEXT NOT NULL,
          reason TEXT,
          occurred_at_ms INTEGER NOT NULL
        );
        "#,
    )?;

    let columns = connection
        .prepare("PRAGMA table_info(thread_authorities)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    if !columns.iter().any(|column| column == "agent_id") {
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(
            r#"
            DROP TRIGGER IF EXISTS thread_authorities_no_update;
            DROP TRIGGER IF EXISTS thread_authorities_no_delete;
            ALTER TABLE thread_authorities RENAME TO thread_authorities_legacy;

            CREATE TABLE thread_authorities (
              thread_id TEXT PRIMARY KEY,
              parent_thread_id TEXT,
              spawning_actor TEXT NOT NULL,
              cwd TEXT,
              agent_id TEXT,
              model TEXT NOT NULL,
              reasoning_effort TEXT NOT NULL,
              approval_policy TEXT NOT NULL,
              toolset TEXT,
              worktrees TEXT,
              granted_paths TEXT,
              network INTEGER,
              created_at_ms INTEGER NOT NULL
            );

            INSERT INTO thread_authorities
              (thread_id, parent_thread_id, spawning_actor, cwd, model,
               reasoning_effort, approval_policy, created_at_ms)
            SELECT thread_id, parent_thread_id, spawning_actor, cwd, model,
                   reasoning_effort, approval_policy, created_at_ms
              FROM thread_authorities_legacy;

            DROP TABLE thread_authorities_legacy;
            "#,
        )?;
        transaction.commit()?;
    }

    connection.execute_batch(
        r#"
        CREATE TRIGGER IF NOT EXISTS thread_authorities_no_update
        BEFORE UPDATE ON thread_authorities
        BEGIN
          SELECT RAISE(ABORT, 'thread authority records are immutable');
        END;

        CREATE TRIGGER IF NOT EXISTS thread_authorities_no_delete
        BEFORE DELETE ON thread_authorities
        BEGIN
          SELECT RAISE(ABORT, 'thread authority records are immutable');
        END;

        CREATE TRIGGER IF NOT EXISTS thread_spawn_approvals_no_update
        BEFORE UPDATE ON thread_spawn_approvals
        BEGIN
          SELECT RAISE(ABORT, 'thread spawn approvals are immutable');
        END;

        CREATE TRIGGER IF NOT EXISTS thread_spawn_approvals_no_delete
        BEFORE DELETE ON thread_spawn_approvals
        BEGIN
          SELECT RAISE(ABORT, 'thread spawn approvals are immutable');
        END;
        "#,
    )?;
    Ok(())
}

fn create_thread_stop_table(connection: &Connection) -> AppResult<()> {
    connection.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS thread_stops (
          thread_id TEXT PRIMARY KEY,
          actor TEXT NOT NULL,
          stopped_turn_id TEXT,
          occurred_at_ms INTEGER NOT NULL
        );

        CREATE TRIGGER IF NOT EXISTS thread_stops_no_update
        BEFORE UPDATE ON thread_stops
        BEGIN
          SELECT RAISE(ABORT, 'thread stop records are immutable');
        END;

        CREATE TRIGGER IF NOT EXISTS thread_stops_no_delete
        BEFORE DELETE ON thread_stops
        BEGIN
          SELECT RAISE(ABORT, 'thread stop records are immutable');
        END;
        "#,
    )?;
    Ok(())
}

fn create_thread_repository_tables(connection: &Connection) -> AppResult<()> {
    connection.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS thread_branch_claims (
          workspace_id TEXT NOT NULL,
          repo TEXT NOT NULL,
          branch TEXT NOT NULL,
          thread_id TEXT NOT NULL,
          claimed_at_ms INTEGER NOT NULL,
          PRIMARY KEY (workspace_id, repo, branch),
          UNIQUE (thread_id, repo)
        );

        CREATE TRIGGER IF NOT EXISTS thread_branch_claims_no_update
        BEFORE UPDATE ON thread_branch_claims
        BEGIN
          SELECT RAISE(ABORT, 'thread branch claims cannot be reassigned');
        END;

        CREATE TABLE IF NOT EXISTS thread_confinements (
          thread_id TEXT PRIMARY KEY,
          backend TEXT NOT NULL,
          recorded_at_ms INTEGER NOT NULL
        );

        CREATE TRIGGER IF NOT EXISTS thread_confinements_no_update
        BEFORE UPDATE ON thread_confinements
        BEGIN
          SELECT RAISE(ABORT, 'thread confinement facts are immutable');
        END;

        CREATE TRIGGER IF NOT EXISTS thread_confinements_no_delete
        BEFORE DELETE ON thread_confinements
        BEGIN
          SELECT RAISE(ABORT, 'thread confinement facts are immutable');
        END;
        "#,
    )?;
    Ok(())
}

/// Read one thread's authority without holding a store open.
///
/// These mirror the store's methods for callers that read a single record.
/// The store is created on first open, so an absent file reads as empty
/// rather than as an error.
pub(crate) fn thread_authorities(path: &Path) -> AppResult<Vec<ThreadAuthorityRecord>> {
    ServerStore::open_or_create(path)?.thread_authorities()
}

pub(crate) fn thread_authority(
    path: &Path,
    thread_id: &str,
) -> AppResult<Option<ThreadAuthorityRecord>> {
    ServerStore::open_or_create(path)?.thread_authority(thread_id)
}

pub(crate) fn thread_confinement(
    path: &Path,
    thread_id: &str,
) -> AppResult<Option<ThreadConfinement>> {
    ServerStore::open_or_create(path)?.thread_confinement(thread_id)
}

pub(crate) fn thread_stop(path: &Path, thread_id: &str) -> AppResult<Option<ThreadStopRecord>> {
    ServerStore::open_or_create(path)?.thread_stop(thread_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use platonic_protocol::ReasoningEffort;
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::{Arc, Barrier},
        thread,
    };

    #[test]
    fn opens_server_store_with_wal_full_and_default_autocheckpoint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("server.db");
        let store = ServerStore::open_or_create(&path).unwrap();

        let journal_mode: String = store
            .connection
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();
        let synchronous: u32 = store
            .connection
            .pragma_query_value(None, "synchronous", |row| row.get(0))
            .unwrap();
        let autocheckpoint: u32 = store
            .connection
            .pragma_query_value(None, "wal_autocheckpoint", |row| row.get(0))
            .unwrap();

        assert_eq!(journal_mode, "wal");
        assert_eq!(synchronous, 2);
        assert_eq!(autocheckpoint, 1_000);
    }

    fn thread_authority(
        thread_id: &str,
        parent_thread_id: Option<&str>,
        actor: &str,
        cwd: &Path,
        policy: ThreadApprovalPolicy,
        created_at_ms: u64,
    ) -> ThreadAuthorityRecord {
        ThreadAuthorityRecord {
            thread_id: thread_id.into(),
            parent_thread_id: parent_thread_id.map(str::to_owned),
            spawning_actor: actor.into(),
            agent_id: Some(AgentId::new("plato").unwrap()),
            model: "gpt-5.6-sol".into(),
            reasoning_effort: ReasoningEffort::Xhigh,
            approval_policy: policy,
            toolset: vec!["file.read".into(), "file.write".into()],
            worktrees: Vec::new(),
            granted_paths: vec![ThreadGrantedPath {
                path: cwd.canonicalize().unwrap().to_string_lossy().into_owned(),
                writable: true,
            }],
            network: false,
            created_at_ms,
        }
    }

    fn thread_approval(
        spawn_id: &str,
        thread_id: &str,
        decision: ThreadSpawnDecisionName,
        actor: &str,
        reason: Option<&str>,
        occurred_at_ms: u64,
    ) -> ThreadSpawnApprovalRecord {
        ThreadSpawnApprovalRecord {
            spawn_id: spawn_id.into(),
            thread_id: thread_id.into(),
            decision,
            actor: actor.into(),
            reason: reason.map(str::to_owned),
            occurred_at_ms,
        }
    }

    #[test]
    fn thread_authority_persists_all_twelve_fields_and_is_immutable_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("threads.db");
        let mut ledger = ServerStore::open_or_create(&path).unwrap();
        let columns = ledger
            .connection
            .prepare("PRAGMA table_info(thread_authorities)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            columns,
            [
                "thread_id",
                "parent_thread_id",
                "spawning_actor",
                "cwd",
                "agent_id",
                "model",
                "reasoning_effort",
                "approval_policy",
                "toolset",
                "worktrees",
                "granted_paths",
                "network",
                "created_at_ms",
            ]
        );

        let authority = thread_authority(
            "thread_root",
            None,
            "stdin",
            dir.path(),
            ThreadApprovalPolicy::Prompt,
            42,
        );
        let approval = thread_approval(
            "spawn_root",
            "thread_root",
            ThreadSpawnDecisionName::Granted,
            "stdin",
            None,
            42,
        );
        let durable = ledger
            .persist_thread_spawn(&approval, Some(&authority), None)
            .unwrap()
            .unwrap();
        assert_eq!(durable.record(), &authority);
        let stored = ledger
            .connection
            .query_row(
                "SELECT cwd, agent_id, toolset, worktrees, granted_paths, network
                   FROM thread_authorities WHERE thread_id = 'thread_root'",
                [],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            stored,
            (
                None,
                "plato".into(),
                r#"["file.read","file.write"]"#.into(),
                "[]".into(),
                format!(
                    r#"[{{"path":{},"writable":true}}]"#,
                    serde_json::to_string(&dir.path().canonicalize().unwrap().to_string_lossy())
                        .unwrap()
                ),
                0,
            )
        );
        assert_eq!(
            ledger.thread_spawn_approval("spawn_root").unwrap(),
            Some(approval.clone())
        );

        for statement in [
            "UPDATE thread_authorities SET model = 'changed' WHERE thread_id = 'thread_root'",
            "DELETE FROM thread_authorities WHERE thread_id = 'thread_root'",
            "UPDATE thread_spawn_approvals SET actor = 'changed' WHERE spawn_id = 'spawn_root'",
            "DELETE FROM thread_spawn_approvals WHERE spawn_id = 'spawn_root'",
        ] {
            let error = ledger.connection.execute(statement, []).unwrap_err();
            assert!(error.to_string().contains("immutable"));
        }
        drop(ledger);

        let reopened = ServerStore::open_readonly(&path).unwrap();
        assert_eq!(
            reopened.thread_authorities().unwrap(),
            vec![authority.clone()]
        );
        assert_eq!(
            reopened.thread_authority("thread_root").unwrap(),
            Some(authority)
        );
        assert_eq!(
            reopened.thread_spawn_approval("spawn_root").unwrap(),
            Some(approval)
        );
    }

    #[test]
    fn thread_stop_is_immutable_idempotent_and_separate_from_authority() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("thread-stop.db");
        let mut ledger = ServerStore::open_or_create(&path).unwrap();
        let authority = thread_authority(
            "thread_stop_target",
            None,
            "stdin",
            dir.path(),
            ThreadApprovalPolicy::Prompt,
            42,
        );
        let approval = thread_approval(
            "spawn_stop_target",
            "thread_stop_target",
            ThreadSpawnDecisionName::Granted,
            "stdin",
            None,
            42,
        );
        ledger
            .persist_thread_spawn(&approval, Some(&authority), None)
            .unwrap();
        let stop = ThreadStopRecord::new(
            "thread_stop_target".into(),
            "operator".into(),
            Some("turn_active".into()),
            52,
        )
        .unwrap();
        assert_eq!(
            ledger.persist_thread_stop(&stop).unwrap(),
            (stop.clone(), true)
        );
        let conflicting_retry = ThreadStopRecord::new(
            "thread_stop_target".into(),
            "other_operator".into(),
            None,
            99,
        )
        .unwrap();
        assert_eq!(
            ledger.persist_thread_stop(&conflicting_retry).unwrap(),
            (stop.clone(), false)
        );
        assert_eq!(
            ledger.thread_authority("thread_stop_target").unwrap(),
            Some(authority.clone())
        );
        for statement in [
            "UPDATE thread_stops SET actor = 'changed' WHERE thread_id = 'thread_stop_target'",
            "DELETE FROM thread_stops WHERE thread_id = 'thread_stop_target'",
        ] {
            assert!(
                ledger
                    .connection
                    .execute(statement, [])
                    .unwrap_err()
                    .to_string()
                    .contains("immutable")
            );
        }
        drop(ledger);

        let reopened = ServerStore::open_readonly(&path).unwrap();
        assert_eq!(
            reopened.thread_stop("thread_stop_target").unwrap(),
            Some(stop)
        );
        assert_eq!(
            reopened.thread_authority("thread_stop_target").unwrap(),
            Some(authority)
        );
    }

    fn approval_record(run_id: &str, call_id: &str) -> ToolCallApprovalRecord {
        ToolCallApprovalRecord {
            run_id: run_id.into(),
            call_id: call_id.into(),
            session_id: "session_1".into(),
            tool_name: "shell_exec".into(),
            effect: EffectClass::ExternalSideEffect,
            reason: "writes outside the workspace".into(),
            input_preview: Some("rm -rf /tmp/x".into()),
            approval_preview: Some("shell_exec: rm -rf /tmp/x".into()),
            diff_preview: None,
            requested_at_ms: 1_700,
            decision: None,
        }
    }

    /// The #435 proof: an approval outlives the daemon that asked it. The ask
    /// survives a restart with every field intact, can be answered afterwards,
    /// and can be answered only once.
    #[test]
    fn pending_approval_survives_restart_and_is_decided_exactly_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("server.db");
        let requested = approval_record("run_1", "call_1");

        let store = ServerStore::open_or_create(&path).unwrap();
        store.persist_tool_call_approval(&requested).unwrap();
        // Re-asking the same call is idempotent, not a duplicate row.
        store.persist_tool_call_approval(&requested).unwrap();
        assert_eq!(store.pending_tool_call_approvals().unwrap().len(), 1);
        drop(store);

        // The daemon is gone. The question is not.
        let restarted = ServerStore::open_or_create(&path).unwrap();
        let pending = restarted.pending_tool_call_approvals().unwrap();
        assert_eq!(pending, vec![requested.clone()]);

        let decision = ToolCallApprovalDecision {
            granted: true,
            actor: "stdin".into(),
            reason: None,
            decided_at_ms: 1_900,
        };
        assert!(
            restarted
                .resolve_tool_call_approval("run_1", "call_1", &decision)
                .unwrap()
        );
        assert!(restarted.pending_tool_call_approvals().unwrap().is_empty());
        assert_eq!(
            restarted
                .tool_call_approval("run_1", "call_1")
                .unwrap()
                .unwrap(),
            ToolCallApprovalRecord {
                decision: Some(decision),
                ..requested.clone()
            }
        );

        // A second decider learns it lost rather than overwriting the first.
        assert!(
            !restarted
                .resolve_tool_call_approval(
                    "run_1",
                    "call_1",
                    &ToolCallApprovalDecision {
                        granted: false,
                        actor: "late".into(),
                        reason: Some("too slow".into()),
                        decided_at_ms: 2_000,
                    }
                )
                .unwrap()
        );
        assert_eq!(
            restarted
                .tool_call_approval("run_1", "call_1")
                .unwrap()
                .unwrap()
                .decision
                .unwrap()
                .actor,
            "stdin"
        );
    }

    /// What was asked can never be rewritten, and no approval can be erased.
    #[test]
    fn approval_requests_are_immutable_and_undeletable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("server.db");
        let store = ServerStore::open_or_create(&path).unwrap();
        store
            .persist_tool_call_approval(&approval_record("run_1", "call_1"))
            .unwrap();

        for statement in [
            "UPDATE tool_call_approvals SET tool_name = 'read_file' WHERE call_id = 'call_1'",
            "UPDATE tool_call_approvals SET reason = 'rewritten' WHERE call_id = 'call_1'",
            "DELETE FROM tool_call_approvals WHERE call_id = 'call_1'",
        ] {
            assert!(store.connection.execute_batch(statement).is_err());
        }
        assert_eq!(store.pending_tool_call_approvals().unwrap().len(), 1);
    }

    /// The D005 proof: a thread stays enumerable from the server tier no
    /// matter which workspace it belongs to, or whether that workspace is
    /// still open. Two workspaces are registered, each spawns a thread, one
    /// workspace is closed, and both threads still enumerate.
    #[test]
    fn threads_in_every_registered_workspace_enumerate_after_one_closes() {
        let dir = tempfile::tempdir().unwrap();
        let alpha_root = dir.path().join("alpha");
        let beta_root = dir.path().join("beta");
        fs::create_dir(&alpha_root).unwrap();
        fs::create_dir(&beta_root).unwrap();
        let server_db = dir.path().join("state/platonic/server.db");

        let mut store = ServerStore::open_or_create(&server_db).unwrap();
        store
            .register_workspace(
                "ws-alpha",
                "alpha",
                &alpha_root.to_string_lossy(),
                "alpha.db",
                10,
            )
            .unwrap();
        store
            .register_workspace(
                "ws-beta",
                "beta",
                &beta_root.to_string_lossy(),
                "beta.db",
                20,
            )
            .unwrap();

        for (index, (thread_id, spawn_id, root)) in [
            ("thread_alpha", "spawn_alpha", &alpha_root),
            ("thread_beta", "spawn_beta", &beta_root),
        ]
        .iter()
        .enumerate()
        {
            let authority = thread_authority(
                thread_id,
                None,
                "stdin",
                root,
                ThreadApprovalPolicy::Prompt,
                30 + index as u64,
            );
            let approval = thread_approval(
                spawn_id,
                thread_id,
                ThreadSpawnDecisionName::Granted,
                "stdin",
                None,
                30 + index as u64,
            );
            store
                .persist_thread_spawn(&approval, Some(&authority), None)
                .unwrap()
                .unwrap();
        }

        // Close the workspace holding thread_alpha. Nothing about the server
        // tier depends on a workspace being open.
        drop(store);
        fs::remove_dir_all(&alpha_root).unwrap();

        let mut reopened = ServerStore::open_or_create(&server_db).unwrap();
        let workspaces = reopened.workspaces().unwrap();
        assert_eq!(
            workspaces
                .iter()
                .map(|workspace| workspace.name.as_str())
                .collect::<Vec<_>>(),
            ["alpha", "beta"]
        );

        let threads = reopened.thread_authorities().unwrap();
        assert_eq!(
            threads
                .iter()
                .map(|authority| authority.thread_id.as_str())
                .collect::<Vec<_>>(),
            ["thread_alpha", "thread_beta"]
        );
        assert!(
            reopened
                .thread_authority("thread_alpha")
                .unwrap()
                .is_some_and(|authority| authority.granted_paths[0].path.ends_with("alpha"))
        );

        // Rebinding reports the existing record without changing it.
        let (rebound, inserted) = reopened
            .register_workspace(
                "ws-alpha",
                "alpha",
                &alpha_root.to_string_lossy(),
                "alpha.db",
                999,
            )
            .unwrap();
        assert!(!inserted);
        assert_eq!(rebound.created_at_ms, 10);
        assert_eq!(reopened.workspaces().unwrap().len(), 2);
    }

    fn run_workspace_registration_race(
        server_db: PathBuf,
        registrations: [(String, String, String, u64); 2],
    ) -> Vec<(WorkspaceRecord, bool)> {
        drop(ServerStore::open_or_create(&server_db).unwrap());
        let barrier = Arc::new(Barrier::new(3));
        let handles = registrations.map(|(id, name, root, created_at_ms)| {
            let server_db = server_db.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let mut store = ServerStore::open_or_create(&server_db).unwrap();
                let ledger_path = paths::workspace_sqlite_path(&server_db, &id).unwrap();
                barrier.wait();
                store
                    .register_workspace(
                        &id,
                        &name,
                        &root,
                        &ledger_path.to_string_lossy(),
                        created_at_ms,
                    )
                    .unwrap()
            })
        });
        barrier.wait();
        handles.map(|handle| handle.join().unwrap()).into()
    }

    fn seed_legacy_workspace_record(store: &ServerStore, id: &str, root: &Path) -> PathBuf {
        let root = root.canonicalize().unwrap();
        let legacy = paths::legacy_sqlite_path_at(&store.path, &root).unwrap();
        store
            .connection
            .execute(
                "INSERT INTO workspaces (id, name, root, ledger_path, created_at_ms)
                 VALUES (?1, 'legacy', ?2, ?3, 10)",
                params![id, root.to_string_lossy(), legacy.to_string_lossy()],
            )
            .unwrap();
        legacy
    }

    fn sqlite_test_companion(path: &Path, suffix: &str) -> PathBuf {
        let mut value = path.as_os_str().to_os_string();
        value.push(suffix);
        PathBuf::from(value)
    }

    fn run_workspace_attachment_race(server_db: &Path, root: &Path) -> Vec<WorkspaceRecord> {
        let root = root.canonicalize().unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let handles = [(), ()].map(|()| {
            let server_db = server_db.to_path_buf();
            let root = root.to_path_buf();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let mut store = ServerStore::open_or_create(&server_db).unwrap();
                barrier.wait();
                store
                    .workspace_for_attachment(&root.to_string_lossy())
                    .unwrap()
                    .unwrap()
            })
        });
        barrier.wait();
        handles.map(|handle| handle.join().unwrap()).into()
    }

    #[test]
    fn concurrent_workspace_registration_atomically_rejects_duplicate_name_and_root() {
        let dir = tempfile::tempdir().unwrap();
        let alpha_root = dir.path().join("alpha");
        let beta_root = dir.path().join("beta");
        fs::create_dir(&alpha_root).unwrap();
        fs::create_dir(&beta_root).unwrap();
        let alpha_root = alpha_root
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let beta_root = beta_root
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .into_owned();

        let same_root = run_workspace_registration_race(
            dir.path().join("same-root.db"),
            [
                ("ws-alpha".into(), "alpha".into(), alpha_root.clone(), 10),
                ("ws-beta".into(), "beta".into(), alpha_root.clone(), 20),
            ],
        );
        assert_eq!(
            same_root.iter().filter(|(_, inserted)| *inserted).count(),
            1
        );
        assert_eq!(same_root[0].0, same_root[1].0);
        assert_eq!(
            ServerStore::open_or_create(&dir.path().join("same-root.db"))
                .unwrap()
                .workspaces()
                .unwrap()
                .len(),
            1
        );

        let same_name = run_workspace_registration_race(
            dir.path().join("same-name.db"),
            [
                ("ws-first".into(), "shared".into(), alpha_root, 30),
                ("ws-second".into(), "shared".into(), beta_root, 40),
            ],
        );
        assert_eq!(
            same_name.iter().filter(|(_, inserted)| *inserted).count(),
            1
        );
        assert_eq!(same_name[0].0, same_name[1].0);
        assert_eq!(
            ServerStore::open_or_create(&dir.path().join("same-name.db"))
                .unwrap()
                .workspaces()
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn concurrent_workspace_registration_adopts_legacy_ledger_once() {
        let dir = tempfile::tempdir().unwrap();
        let workspace_root = dir.path().join("workspace");
        fs::create_dir(&workspace_root).unwrap();
        let workspace_root = workspace_root
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let server_db = dir.path().join("state/platonic/server.db");
        let legacy = paths::legacy_sqlite_path_at(&server_db, Path::new(&workspace_root)).unwrap();
        fs::create_dir_all(legacy.parent().unwrap()).unwrap();
        fs::write(&legacy, b"legacy history").unwrap();

        let registrations = run_workspace_registration_race(
            server_db.clone(),
            [
                (
                    "ws-alpha".into(),
                    "alpha".into(),
                    workspace_root.clone(),
                    10,
                ),
                ("ws-beta".into(), "beta".into(), workspace_root, 20),
            ],
        );

        assert_eq!(
            registrations
                .iter()
                .filter(|(_, inserted)| *inserted)
                .count(),
            1
        );
        assert_eq!(registrations[0].0, registrations[1].0);
        let record = &registrations[0].0;
        assert_eq!(fs::read(&record.ledger_path).unwrap(), b"legacy history");
        assert!(!legacy.exists());
        let losing_id = if record.id == "ws-alpha" {
            "ws-beta"
        } else {
            "ws-alpha"
        };
        assert!(
            !paths::workspace_sqlite_path(&server_db, losing_id)
                .unwrap()
                .exists()
        );
    }

    #[test]
    fn concurrent_attachment_migrates_one_existing_registry_row_once() {
        let dir = tempfile::tempdir().unwrap();
        let workspace_root = dir.path().join("workspace");
        fs::create_dir(&workspace_root).unwrap();
        let server_db = dir.path().join("state/platonic/server.db");
        let store = ServerStore::open_or_create(&server_db).unwrap();
        let legacy = seed_legacy_workspace_record(&store, "ws-0123456789abcdef", &workspace_root);
        fs::create_dir_all(legacy.parent().unwrap()).unwrap();
        fs::write(&legacy, b"registered legacy history").unwrap();
        drop(store);

        let records = run_workspace_attachment_race(&server_db, &workspace_root);
        assert_eq!(records[0], records[1]);
        assert_eq!(records[0].id, "ws-0123456789abcdef");
        let destination = paths::workspace_sqlite_path(&server_db, &records[0].id).unwrap();
        assert_eq!(Path::new(&records[0].ledger_path), destination);
        assert_eq!(
            fs::read(&destination).unwrap(),
            b"registered legacy history"
        );
        assert!(!legacy.exists());

        let reopened = ServerStore::open_or_create(&server_db).unwrap();
        assert_eq!(
            reopened
                .workspace("ws-0123456789abcdef")
                .unwrap()
                .unwrap()
                .ledger_path,
            destination.to_string_lossy()
        );
    }

    #[test]
    fn existing_registry_migration_restores_legacy_files_when_update_fails() {
        let dir = tempfile::tempdir().unwrap();
        let workspace_root = dir.path().join("workspace");
        fs::create_dir(&workspace_root).unwrap();
        let server_db = dir.path().join("state/platonic/server.db");
        let mut store = ServerStore::open_or_create(&server_db).unwrap();
        let legacy = seed_legacy_workspace_record(&store, "ws-0123456789abcdef", &workspace_root);
        fs::create_dir_all(legacy.parent().unwrap()).unwrap();
        fs::write(&legacy, b"registered legacy history").unwrap();
        fs::write(sqlite_test_companion(&legacy, "-wal"), b"legacy wal").unwrap();
        store
            .connection
            .execute_batch(
                "CREATE TRIGGER fail_workspace_ledger_migration
                 BEFORE UPDATE OF ledger_path ON workspaces
                 BEGIN
                   SELECT RAISE(FAIL, 'injected ledger migration failure');
                 END;",
            )
            .unwrap();

        let error = store
            .workspace_for_attachment(&workspace_root.canonicalize().unwrap().to_string_lossy())
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("injected ledger migration failure")
        );
        let destination = paths::workspace_sqlite_path(&server_db, "ws-0123456789abcdef").unwrap();
        assert_eq!(fs::read(&legacy).unwrap(), b"registered legacy history");
        assert_eq!(
            fs::read(sqlite_test_companion(&legacy, "-wal")).unwrap(),
            b"legacy wal"
        );
        assert!(!destination.exists());
        assert_eq!(
            store
                .workspace("ws-0123456789abcdef")
                .unwrap()
                .unwrap()
                .ledger_path,
            legacy.to_string_lossy()
        );
    }

    #[test]
    fn existing_registry_migration_recovers_a_move_completed_before_row_update() {
        let dir = tempfile::tempdir().unwrap();
        let workspace_root = dir.path().join("workspace");
        fs::create_dir(&workspace_root).unwrap();
        let server_db = dir.path().join("state/platonic/server.db");
        let mut store = ServerStore::open_or_create(&server_db).unwrap();
        let legacy = seed_legacy_workspace_record(&store, "ws-0123456789abcdef", &workspace_root);
        fs::create_dir_all(legacy.parent().unwrap()).unwrap();
        fs::write(&legacy, b"registered legacy history").unwrap();
        let destination = paths::workspace_sqlite_path(&server_db, "ws-0123456789abcdef").unwrap();
        assert!(paths::move_sqlite_files(&legacy, &destination).unwrap());
        assert_eq!(
            store
                .workspace("ws-0123456789abcdef")
                .unwrap()
                .unwrap()
                .ledger_path,
            legacy.to_string_lossy()
        );

        let record = store
            .workspace_for_attachment(&workspace_root.canonicalize().unwrap().to_string_lossy())
            .unwrap()
            .unwrap();

        assert_eq!(Path::new(&record.ledger_path), destination);
        assert_eq!(
            fs::read(&destination).unwrap(),
            b"registered legacy history"
        );
        assert!(!legacy.exists());
    }

    #[test]
    fn agents_round_trip_from_server_db_without_partial_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        let workspace_root = dir.path().join("workspace");
        fs::create_dir(&workspace_root).unwrap();
        let server_db = dir.path().join("server.db");
        let mut store = ServerStore::open_or_create(&server_db).unwrap();
        store
            .register_workspace(
                "ws-alpha",
                "alpha",
                &workspace_root.to_string_lossy(),
                "alpha.db",
                10,
            )
            .unwrap();
        assert_eq!(
            store
                .workspace_by_root(&workspace_root.to_string_lossy())
                .unwrap()
                .unwrap()
                .id,
            "ws-alpha"
        );
        let record = AgentRecord {
            id: AgentId::new("builder").unwrap(),
            workspace_id: "ws-alpha".into(),
            model: "gpt-5.6-sol".into(),
            reasoning_effort: ReasoningEffort::Xhigh,
            approval_policy: ThreadApprovalPolicy::Prompt,
            toolset: vec!["file.read".into(), "file.write".into()],
            created_at_ms: 20,
        };

        assert!(store.register_agent(&record).unwrap());
        assert!(!store.register_agent(&record).unwrap());
        assert_eq!(store.agent(&record.id).unwrap(), Some(record.clone()));
        assert_eq!(store.agents().unwrap(), [record]);
        assert!(
            store
                .connection
                .execute(
                    "UPDATE agents SET model = 'changed' WHERE id = 'builder'",
                    []
                )
                .is_err()
        );
        assert!(
            store
                .connection
                .execute("DELETE FROM agents WHERE id = 'builder'", [])
                .is_err()
        );
    }

    #[test]
    fn pre_migration_authority_rows_enumerate_with_conservative_typed_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("literal-authority.db");
        let cwd = dir.path().canonicalize().unwrap();
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                r#"
                CREATE TABLE thread_authorities (
                  thread_id TEXT PRIMARY KEY,
                  parent_thread_id TEXT,
                  spawning_actor TEXT NOT NULL,
                  cwd TEXT NOT NULL,
                  model TEXT NOT NULL,
                  reasoning_effort TEXT NOT NULL,
                  approval_policy TEXT NOT NULL,
                  created_at_ms INTEGER NOT NULL
                );
                CREATE TRIGGER thread_authorities_no_update
                BEFORE UPDATE ON thread_authorities
                BEGIN
                  SELECT RAISE(ABORT, 'thread authority records are immutable');
                END;
                CREATE TRIGGER thread_authorities_no_delete
                BEFORE DELETE ON thread_authorities
                BEGIN
                  SELECT RAISE(ABORT, 'thread authority records are immutable');
                END;
                CREATE TABLE thread_stops (
                  thread_id TEXT PRIMARY KEY,
                  actor TEXT NOT NULL,
                  stopped_turn_id TEXT,
                  occurred_at_ms INTEGER NOT NULL
                );
                "#,
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO thread_authorities
                   (thread_id, parent_thread_id, spawning_actor, cwd, model,
                    reasoning_effort, approval_policy, created_at_ms)
                 VALUES ('thread_literal', 'thread_parent', 'fixture_actor', ?1,
                         'gpt-5.6-sol', 'xhigh', 'prompt', 42)",
                params![cwd.to_string_lossy()],
            )
            .unwrap();
        drop(connection);

        let ledger = ServerStore::open_or_create(&path).unwrap();
        let authority = ledger.thread_authority("thread_literal").unwrap().unwrap();
        assert_eq!(
            authority,
            ThreadAuthorityRecord {
                thread_id: "thread_literal".into(),
                parent_thread_id: Some("thread_parent".into()),
                spawning_actor: "fixture_actor".into(),
                agent_id: None,
                model: "gpt-5.6-sol".into(),
                reasoning_effort: ReasoningEffort::Xhigh,
                approval_policy: ThreadApprovalPolicy::Prompt,
                toolset: Vec::new(),
                worktrees: Vec::new(),
                granted_paths: vec![ThreadGrantedPath {
                    path: cwd.to_string_lossy().into_owned(),
                    writable: true,
                }],
                network: false,
                created_at_ms: 42,
            }
        );
        assert_eq!(
            legacy_status_authority(&authority).unwrap().cwd,
            cwd.to_string_lossy()
        );
        assert_eq!(ledger.thread_authorities().unwrap().len(), 1);
        let legacy_columns = ledger
            .connection
            .query_row(
                "SELECT cwd, agent_id, toolset, worktrees, granted_paths, network
                   FROM thread_authorities WHERE thread_id = 'thread_literal'",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            legacy_columns,
            (
                cwd.to_string_lossy().into_owned(),
                None,
                None,
                None,
                None,
                None,
            )
        );
        for statement in [
            "UPDATE thread_authorities SET model = 'changed' WHERE thread_id = 'thread_literal'",
            "DELETE FROM thread_authorities WHERE thread_id = 'thread_literal'",
        ] {
            assert!(
                ledger
                    .connection
                    .execute(statement, [])
                    .unwrap_err()
                    .to_string()
                    .contains("immutable")
            );
        }
        assert!(ledger.thread_stop("thread_literal").unwrap().is_none());
    }

    #[test]
    fn malformed_durable_thread_authority_fails_closed_on_readback() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("malformed-v4.db");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                r#"
                CREATE TABLE thread_authorities (
                  thread_id TEXT PRIMARY KEY,
                  parent_thread_id TEXT,
                  spawning_actor TEXT NOT NULL,
                  cwd TEXT NOT NULL,
                  model TEXT NOT NULL,
                  reasoning_effort TEXT NOT NULL,
                  approval_policy TEXT NOT NULL,
                  created_at_ms INTEGER NOT NULL
                );
                INSERT INTO thread_authorities VALUES
                  ('thread_bad', NULL, 'fixture_actor', '/tmp', 'gpt-5.6-sol',
                   'xhigh', 'expanded', 42);
                PRAGMA user_version = 4;
                "#,
            )
            .unwrap();
        drop(connection);

        let ledger = ServerStore::open_or_create(&path).unwrap();
        let error = ledger.thread_authority("thread_bad").unwrap_err();
        assert!(error.to_string().contains("unknown approval policy"));
    }

    #[test]
    fn denied_and_canceled_thread_spawns_record_actor_without_authority() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("threads.db");
        let mut ledger = ServerStore::open_or_create(&path).unwrap();
        for approval in [
            thread_approval(
                "spawn_denied",
                "thread_denied",
                ThreadSpawnDecisionName::Denied,
                "reviewer",
                Some("not admitted"),
                10,
            ),
            thread_approval(
                "spawn_canceled",
                "thread_canceled",
                ThreadSpawnDecisionName::Canceled,
                "stdin",
                None,
                11,
            ),
        ] {
            assert!(
                ledger
                    .persist_thread_spawn(&approval, None, None)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                ledger.thread_spawn_approval(&approval.spawn_id).unwrap(),
                Some(approval.clone())
            );
            assert!(
                ledger
                    .thread_authority(&approval.thread_id)
                    .unwrap()
                    .is_none()
            );
        }
        assert!(ledger.thread_authorities().unwrap().is_empty());
    }

    #[test]
    fn thread_spawn_persistence_failure_rolls_back_decision_and_authority() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("threads.db");
        let mut ledger = ServerStore::open_or_create(&path).unwrap();
        ledger
            .connection
            .execute_batch(
                "CREATE TRIGGER fail_thread_authority_insert
                 BEFORE INSERT ON thread_authorities
                 BEGIN SELECT RAISE(ABORT, 'injected authority failure'); END;",
            )
            .unwrap();
        let authority = thread_authority(
            "thread_failed",
            None,
            "stdin",
            dir.path(),
            ThreadApprovalPolicy::Prompt,
            20,
        );
        let approval = thread_approval(
            "spawn_failed",
            "thread_failed",
            ThreadSpawnDecisionName::Granted,
            "stdin",
            None,
            20,
        );

        assert!(
            ledger
                .persist_thread_spawn(&approval, Some(&authority), None)
                .is_err()
        );
        assert!(
            ledger
                .thread_spawn_approval("spawn_failed")
                .unwrap()
                .is_none()
        );
        assert!(ledger.thread_authority("thread_failed").unwrap().is_none());
    }

    #[test]
    fn duplicate_thread_spawn_is_idempotent_only_for_identical_durable_facts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("threads.db");
        let mut ledger = ServerStore::open_or_create(&path).unwrap();
        let authority = thread_authority(
            "thread_root",
            None,
            "stdin",
            dir.path(),
            ThreadApprovalPolicy::Yolo,
            30,
        );
        let approval = thread_approval(
            "spawn_root",
            "thread_root",
            ThreadSpawnDecisionName::Granted,
            "stdin",
            None,
            30,
        );
        ledger
            .persist_thread_spawn(&approval, Some(&authority), None)
            .unwrap();
        assert_eq!(
            ledger
                .persist_thread_spawn(&approval, Some(&authority), None)
                .unwrap()
                .unwrap()
                .record(),
            &authority
        );

        let conflicting_approval = thread_approval(
            "spawn_root",
            "thread_root",
            ThreadSpawnDecisionName::Granted,
            "different_actor",
            None,
            30,
        );
        let conflicting_authority = ThreadAuthorityRecord {
            spawning_actor: "different_actor".into(),
            ..authority.clone()
        };
        assert!(matches!(
            ledger.persist_thread_spawn(
                &conflicting_approval,
                Some(&conflicting_authority),
                None,
            ),
            Err(AppError::Config(message)) if message.contains("conflicts")
        ));

        let mismatched_actor = ThreadAuthorityRecord {
            spawning_actor: "reviewer".into(),
            thread_id: "thread_other".into(),
            ..authority
        };
        let other_approval = thread_approval(
            "spawn_other",
            "thread_other",
            ThreadSpawnDecisionName::Granted,
            "stdin",
            None,
            31,
        );
        assert!(matches!(
            ledger.persist_thread_spawn(&other_approval, Some(&mismatched_actor), None),
            Err(AppError::Config(message)) if message.contains("actor")
        ));
    }

    #[test]
    fn durable_parent_gate_rejects_policy_cwd_and_toolset_expansion_atomically() {
        let root = tempfile::tempdir().unwrap();
        let child_dir = root.path().join("child");
        fs::create_dir(&child_dir).unwrap();
        let outside = tempfile::tempdir().unwrap();
        let path = root.path().join("threads.db");
        let mut ledger = ServerStore::open_or_create(&path).unwrap();
        let parent = thread_authority(
            "thread_parent",
            None,
            "stdin",
            root.path(),
            ThreadApprovalPolicy::Prompt,
            1,
        );
        let parent_approval = thread_approval(
            "spawn_parent",
            "thread_parent",
            ThreadSpawnDecisionName::Granted,
            "stdin",
            None,
            1,
        );
        ledger
            .persist_thread_spawn(&parent_approval, Some(&parent), None)
            .unwrap();

        for (spawn_id, thread_id, cwd, policy) in [
            (
                "spawn_policy_expansion",
                "thread_policy_expansion",
                child_dir.as_path(),
                ThreadApprovalPolicy::Yolo,
            ),
            (
                "spawn_cwd_expansion",
                "thread_cwd_expansion",
                outside.path(),
                ThreadApprovalPolicy::Prompt,
            ),
        ] {
            let authority =
                thread_authority(thread_id, Some("thread_parent"), "stdin", cwd, policy, 2);
            let approval = thread_approval(
                spawn_id,
                thread_id,
                ThreadSpawnDecisionName::Granted,
                "stdin",
                None,
                2,
            );
            assert!(
                ledger
                    .persist_thread_spawn(&approval, Some(&authority), None)
                    .is_err()
            );
            assert!(ledger.thread_spawn_approval(spawn_id).unwrap().is_none());
            assert!(ledger.thread_authority(thread_id).unwrap().is_none());
        }

        let mut authority = thread_authority(
            "thread_toolset_expansion",
            Some("thread_parent"),
            "stdin",
            &child_dir,
            ThreadApprovalPolicy::Prompt,
            2,
        );
        authority.toolset.push("web.fetch".into());
        let approval = thread_approval(
            "spawn_toolset_expansion",
            "thread_toolset_expansion",
            ThreadSpawnDecisionName::Granted,
            "stdin",
            None,
            2,
        );
        assert!(matches!(
            ledger.persist_thread_spawn(&approval, Some(&authority), None),
            Err(AppError::Config(message)) if message.contains("toolset")
        ));
        assert!(
            ledger
                .thread_spawn_approval("spawn_toolset_expansion")
                .unwrap()
                .is_none()
        );
        assert!(
            ledger
                .thread_authority("thread_toolset_expansion")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn concurrent_branch_claims_conflict_once_and_disjoint_repositories_both_succeed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("claims.db");
        ServerStore::open_or_create(&path).unwrap();

        let barrier = Arc::new(Barrier::new(3));
        let contenders = ["thread_a", "thread_b"].map(|thread_id| {
            let path = path.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let mut store = ServerStore::open_or_create(&path).unwrap();
                barrier.wait();
                store
                    .claim_thread_branches(
                        "workspace",
                        thread_id,
                        &[("repo".into(), "main".into())],
                        1,
                    )
                    .unwrap()
            })
        });
        barrier.wait();
        let results = contenders.map(|worker| worker.join().unwrap());
        assert_eq!(results.iter().filter(|result| result.is_none()).count(), 1);
        assert_eq!(results.iter().filter(|result| result.is_some()).count(), 1);

        let barrier = Arc::new(Barrier::new(3));
        let disjoint = [("thread_c", "repo-c"), ("thread_d", "repo-d")].map(|(thread_id, repo)| {
            let path = path.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let mut store = ServerStore::open_or_create(&path).unwrap();
                barrier.wait();
                store
                    .claim_thread_branches(
                        "workspace",
                        thread_id,
                        &[(repo.into(), "main".into())],
                        2,
                    )
                    .unwrap()
            })
        });
        barrier.wait();
        for worker in disjoint {
            assert!(worker.join().unwrap().is_none());
        }
    }

    #[test]
    fn confinement_fact_is_immutable_and_stop_releases_the_live_branch_claim() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("confinement.db");
        let mut store = ServerStore::open_or_create(&path).unwrap();
        let authority = thread_authority(
            "thread_confined",
            None,
            "stdin",
            dir.path(),
            ThreadApprovalPolicy::Prompt,
            42,
        );
        let approval = thread_approval(
            "spawn_confined",
            "thread_confined",
            ThreadSpawnDecisionName::Granted,
            "stdin",
            None,
            42,
        );
        store
            .claim_thread_branches(
                "workspace",
                "thread_confined",
                &[("repo".into(), "main".into())],
                42,
            )
            .unwrap();
        store
            .persist_thread_spawn(&approval, Some(&authority), Some(ThreadConfinement::None))
            .unwrap();
        assert_eq!(
            store.thread_confinement("thread_confined").unwrap(),
            Some(ThreadConfinement::None)
        );
        for statement in [
            "UPDATE thread_confinements SET backend = 'landlock' WHERE thread_id = 'thread_confined'",
            "DELETE FROM thread_confinements WHERE thread_id = 'thread_confined'",
        ] {
            let error = store.connection.execute(statement, []).unwrap_err();
            assert!(error.to_string().contains("immutable"));
        }

        store
            .persist_thread_stop(
                &ThreadStopRecord::new("thread_confined".into(), "test".into(), None, 43).unwrap(),
            )
            .unwrap();
        assert!(store.branch_claims().unwrap().is_empty());
        assert_eq!(
            store.thread_confinement("thread_confined").unwrap(),
            Some(ThreadConfinement::None)
        );
    }
}
