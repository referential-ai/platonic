use super::types::ProfileRevisionContent;
use crate::{AppError, AppResult, thread_authority::now_ms};
use platonic_core::{ModelName, ProfileId, ToolName};
use platonic_protocol::{ReasoningEffort, ThreadApprovalPolicy};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

pub(super) const SERVER_SCHEMA_VERSION: u32 = 2;
const BASE_SCHEMA_VERSION: u32 = 1;
const BASE_MIGRATION_NAME: &str = "server_store_baseline";
const PROFILE_MIGRATION_NAME: &str = "profile_registry";
const IMPORT_ACTOR: &str = "migration:agents-v1";

pub(super) fn migrate_server_schema(connection: &mut Connection) -> AppResult<()> {
    let version: u32 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version > SERVER_SCHEMA_VERSION {
        return Err(AppError::SqliteSchemaVersion {
            expected: SERVER_SCHEMA_VERSION,
            actual: version,
        });
    }
    if version == SERVER_SCHEMA_VERSION {
        validate_migration_journal(connection, version)?;
        return Ok(());
    }

    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut version: u32 =
        transaction.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version > SERVER_SCHEMA_VERSION {
        return Err(AppError::SqliteSchemaVersion {
            expected: SERVER_SCHEMA_VERSION,
            actual: version,
        });
    }
    if version > 0 {
        validate_migration_journal(&transaction, version)?;
    }
    if version < BASE_SCHEMA_VERSION {
        migrate_baseline(&transaction)?;
        version = BASE_SCHEMA_VERSION;
    }
    if version < SERVER_SCHEMA_VERSION {
        migrate_profiles(&transaction)?;
    }
    validate_migration_journal(&transaction, SERVER_SCHEMA_VERSION)?;
    transaction.commit()?;
    Ok(())
}

fn migrate_baseline(transaction: &Transaction<'_>) -> AppResult<()> {
    transaction.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS server_schema_migrations (
          version INTEGER PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          applied_at_ms INTEGER NOT NULL
        );
        "#,
    )?;
    create_thread_authority_tables(transaction)?;
    create_thread_stop_table(transaction)?;
    create_thread_repository_tables(transaction)?;
    create_workspace_table(transaction)?;
    create_agent_table(transaction)?;
    create_tool_call_approval_table(transaction)?;
    create_run_cancellation_table(transaction)?;
    record_migration(transaction, BASE_SCHEMA_VERSION, BASE_MIGRATION_NAME)?;
    transaction.pragma_update(None, "user_version", BASE_SCHEMA_VERSION)?;
    Ok(())
}

fn migrate_profiles(transaction: &Transaction<'_>) -> AppResult<()> {
    create_profile_tables(transaction)?;
    let agents = import_agents(transaction)?;
    migrate_thread_authorities(transaction, &agents)?;
    record_migration(transaction, SERVER_SCHEMA_VERSION, PROFILE_MIGRATION_NAME)?;
    transaction.pragma_update(None, "user_version", SERVER_SCHEMA_VERSION)?;
    Ok(())
}

fn record_migration(transaction: &Transaction<'_>, version: u32, name: &str) -> AppResult<()> {
    transaction.execute(
        "INSERT INTO server_schema_migrations (version, name, applied_at_ms)
         VALUES (?1, ?2, ?3)",
        params![version, name, i64::try_from(now_ms()).unwrap_or(i64::MAX)],
    )?;
    Ok(())
}

fn validate_migration_journal(connection: &Connection, version: u32) -> AppResult<()> {
    let exists = connection
        .query_row(
            "SELECT 1 FROM sqlite_schema
             WHERE type = 'table' AND name = 'server_schema_migrations'",
            [],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if !exists {
        return Err(AppError::Config(format!(
            "server schema version {version} has no migration journal"
        )));
    }
    let mut statement = connection
        .prepare("SELECT version, name FROM server_schema_migrations ORDER BY version ASC")?;
    let entries = statement
        .query_map([], |row| {
            Ok((row.get::<_, u32>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let expected = [
        (BASE_SCHEMA_VERSION, BASE_MIGRATION_NAME),
        (SERVER_SCHEMA_VERSION, PROFILE_MIGRATION_NAME),
    ];
    let expected_len = usize::try_from(version)
        .map_err(|_| AppError::Config("server schema version exceeds usize".into()))?;
    if entries.len() != expected_len
        || entries.iter().zip(expected).any(
            |((actual_version, actual_name), (expected_version, expected_name))| {
                *actual_version != expected_version || actual_name != expected_name
            },
        )
    {
        return Err(AppError::Config(format!(
            "server schema migration journal does not match version {version}"
        )));
    }
    Ok(())
}

pub(super) fn create_workspace_table(connection: &Connection) -> AppResult<()> {
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

pub(super) fn create_agent_table(connection: &Connection) -> AppResult<()> {
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

#[derive(Clone)]
struct AgentImport {
    workspace_id: String,
    profile_id: Option<ProfileId>,
    missing_workspace: bool,
}

struct LegacyAgentRow {
    id: String,
    workspace_id: String,
    model: String,
    reasoning_effort: String,
    approval_policy: String,
    toolset: String,
    created_at_ms: i64,
}

struct LegacyThreadRow {
    thread_id: String,
    parent_thread_id: Option<String>,
    spawning_actor: String,
    cwd: Option<String>,
    agent_id: Option<String>,
    model: String,
    reasoning_effort: String,
    approval_policy: String,
    toolset: Option<String>,
    worktrees: Option<String>,
    granted_paths: Option<String>,
    network: Option<i64>,
    created_at_ms: i64,
}

fn create_profile_tables(connection: &Connection) -> AppResult<()> {
    connection.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS profiles (
          id TEXT PRIMARY KEY,
          workspace_id TEXT NOT NULL,
          display_name TEXT NOT NULL,
          model TEXT NOT NULL,
          reasoning_effort TEXT NOT NULL,
          approval_policy TEXT NOT NULL,
          toolset TEXT NOT NULL,
          current_revision INTEGER NOT NULL CHECK (current_revision > 0),
          home_thread_id TEXT UNIQUE,
          imported_agent_id TEXT UNIQUE,
          created_at_ms INTEGER NOT NULL,
          UNIQUE (workspace_id, display_name),
          FOREIGN KEY (workspace_id) REFERENCES workspaces(id)
        );

        CREATE TRIGGER IF NOT EXISTS profiles_identity_is_immutable
        BEFORE UPDATE ON profiles
        WHEN OLD.id IS NOT NEW.id
          OR OLD.workspace_id IS NOT NEW.workspace_id
          OR OLD.created_at_ms IS NOT NEW.created_at_ms
          OR OLD.imported_agent_id IS NOT NEW.imported_agent_id
        BEGIN
          SELECT RAISE(ABORT, 'profile identity is immutable');
        END;

        CREATE TABLE IF NOT EXISTS profile_revisions (
          profile_id TEXT NOT NULL,
          revision INTEGER NOT NULL CHECK (revision > 0),
          parent_revision INTEGER,
          actor TEXT NOT NULL,
          created_at_ms INTEGER NOT NULL,
          content_hash TEXT NOT NULL,
          instructions_markdown TEXT NOT NULL,
          memory_markdown TEXT NOT NULL,
          skill_refs TEXT NOT NULL,
          PRIMARY KEY (profile_id, revision),
          CHECK (
            (revision = 1 AND parent_revision IS NULL)
            OR (revision > 1 AND parent_revision = revision - 1)
          ),
          FOREIGN KEY (profile_id) REFERENCES profiles(id)
        );

        CREATE TRIGGER IF NOT EXISTS profile_revisions_no_update
        BEFORE UPDATE ON profile_revisions
        BEGIN
          SELECT RAISE(ABORT, 'profile revisions are immutable');
        END;

        CREATE TRIGGER IF NOT EXISTS profile_revisions_no_delete
        BEFORE DELETE ON profile_revisions
        BEGIN
          SELECT RAISE(ABORT, 'profile revisions are immutable');
        END;
        "#,
    )?;
    Ok(())
}

fn import_agents(transaction: &Transaction<'_>) -> AppResult<HashMap<String, AgentImport>> {
    let rows = {
        let mut statement = transaction.prepare(
            "SELECT id, workspace_id, model, reasoning_effort, approval_policy,
                    toolset, created_at_ms
             FROM agents ORDER BY created_at_ms ASC, id ASC",
        )?;
        statement
            .query_map([], |row| {
                Ok(LegacyAgentRow {
                    id: row.get(0)?,
                    workspace_id: row.get(1)?,
                    model: row.get(2)?,
                    reasoning_effort: row.get(3)?,
                    approval_policy: row.get(4)?,
                    toolset: row.get(5)?,
                    created_at_ms: row.get(6)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    let mut imports = HashMap::new();
    for row in rows {
        let workspace_exists = transaction
            .query_row(
                "SELECT 1 FROM workspaces WHERE id = ?1",
                params![row.workspace_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        let mut imported = AgentImport {
            workspace_id: row.workspace_id.clone(),
            profile_id: None,
            missing_workspace: !workspace_exists,
        };
        if !workspace_exists || !valid_legacy_agent(&row) {
            imports.insert(row.id, imported);
            continue;
        }

        let profile_id = imported_profile_id(transaction, &row)?;
        let existing_import: Option<String> = transaction
            .query_row(
                "SELECT imported_agent_id FROM profiles WHERE id = ?1",
                params![profile_id.as_str()],
                |result| result.get(0),
            )
            .optional()?;
        if existing_import.as_deref() != Some(row.id.as_str()) {
            let display_name = imported_display_name(transaction, &row, &profile_id)?;
            transaction.execute(
                "INSERT INTO profiles
                   (id, workspace_id, display_name, model, reasoning_effort,
                    approval_policy, toolset, current_revision, home_thread_id,
                    imported_agent_id, created_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1, NULL, ?8, ?9)",
                params![
                    profile_id.as_str(),
                    row.workspace_id,
                    display_name,
                    row.model,
                    row.reasoning_effort,
                    row.approval_policy,
                    row.toolset,
                    row.id,
                    row.created_at_ms,
                ],
            )?;
            let content = ProfileRevisionContent::empty();
            let content_hash = content
                .content_hash()
                .map_err(|error| AppError::Config(error.to_string()))?;
            transaction.execute(
                "INSERT INTO profile_revisions
                   (profile_id, revision, parent_revision, actor, created_at_ms,
                    content_hash, instructions_markdown, memory_markdown, skill_refs)
                 VALUES (?1, 1, NULL, ?2, ?3, ?4, '', '', '[]')",
                params![
                    profile_id.as_str(),
                    IMPORT_ACTOR,
                    row.created_at_ms,
                    content_hash,
                ],
            )?;
        }
        imported.profile_id = Some(profile_id);
        imports.insert(row.id, imported);
    }
    Ok(imports)
}

fn valid_legacy_agent(row: &LegacyAgentRow) -> bool {
    if row.created_at_ms < 0
        || ProfileId::new(row.id.clone()).is_err()
        || ModelName::new(row.model.clone()).is_err()
        || ReasoningEffort::parse(&row.reasoning_effort).is_none()
        || ThreadApprovalPolicy::parse(&row.approval_policy).is_none()
    {
        return false;
    }
    serde_json::from_str::<Vec<String>>(&row.toolset).is_ok_and(|toolset| {
        !toolset.is_empty() && toolset.into_iter().all(|tool| ToolName::new(tool).is_ok())
    })
}

fn imported_profile_id(
    transaction: &Transaction<'_>,
    row: &LegacyAgentRow,
) -> AppResult<ProfileId> {
    if safe_profile_component(&row.id)
        && !profile_id_exists_for_other_agent(transaction, &row.id, &row.id)?
    {
        return ProfileId::new(row.id.clone()).map_err(AppError::Core);
    }
    let digest = import_digest(&row.workspace_id, &row.id);
    for attempt in 0_u32.. {
        let candidate = if attempt == 0 {
            format!("profile-import-{digest}")
        } else {
            format!("profile-import-{digest}-{attempt}")
        };
        if !profile_id_exists_for_other_agent(transaction, &candidate, &row.id)? {
            return ProfileId::new(candidate).map_err(AppError::Core);
        }
    }
    unreachable!("deterministic profile id attempts are unbounded")
}

fn profile_id_exists_for_other_agent(
    transaction: &Transaction<'_>,
    profile_id: &str,
    agent_id: &str,
) -> AppResult<bool> {
    let existing = transaction
        .query_row(
            "SELECT imported_agent_id FROM profiles WHERE id = ?1",
            params![profile_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?;
    Ok(existing.is_some_and(|existing| existing.as_deref() != Some(agent_id)))
}

fn imported_display_name(
    transaction: &Transaction<'_>,
    row: &LegacyAgentRow,
    profile_id: &ProfileId,
) -> AppResult<String> {
    if !profile_name_exists(transaction, &row.workspace_id, &row.id)? {
        return Ok(row.id.clone());
    }
    let digest = import_digest(&row.workspace_id, profile_id.as_str());
    for attempt in 0_u32.. {
        let candidate = if attempt == 0 {
            format!("{} [imported {digest}]", row.id)
        } else {
            format!("{} [imported {digest}-{attempt}]", row.id)
        };
        if !profile_name_exists(transaction, &row.workspace_id, &candidate)? {
            return Ok(candidate);
        }
    }
    unreachable!("deterministic profile name attempts are unbounded")
}

fn profile_name_exists(
    transaction: &Transaction<'_>,
    workspace_id: &str,
    display_name: &str,
) -> AppResult<bool> {
    Ok(transaction
        .query_row(
            "SELECT 1 FROM profiles WHERE workspace_id = ?1 AND display_name = ?2",
            params![workspace_id, display_name],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

fn safe_profile_component(value: &str) -> bool {
    !matches!(value, "" | "." | "..")
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn import_digest(workspace_id: &str, value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(workspace_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(value.as_bytes());
    hasher
        .finalize()
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn migrate_thread_authorities(
    transaction: &Transaction<'_>,
    agents: &HashMap<String, AgentImport>,
) -> AppResult<()> {
    let rows = {
        let mut statement = transaction.prepare(
            "SELECT thread_id, parent_thread_id, spawning_actor, cwd, agent_id,
                    model, reasoning_effort, approval_policy, toolset, worktrees,
                    granted_paths, network, created_at_ms
             FROM thread_authorities ORDER BY created_at_ms ASC, thread_id ASC",
        )?;
        statement
            .query_map([], |row| {
                Ok(LegacyThreadRow {
                    thread_id: row.get(0)?,
                    parent_thread_id: row.get(1)?,
                    spawning_actor: row.get(2)?,
                    cwd: row.get(3)?,
                    agent_id: row.get(4)?,
                    model: row.get(5)?,
                    reasoning_effort: row.get(6)?,
                    approval_policy: row.get(7)?,
                    toolset: row.get(8)?,
                    worktrees: row.get(9)?,
                    granted_paths: row.get(10)?,
                    network: row.get(11)?,
                    created_at_ms: row.get(12)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    let thread_agents = rows
        .iter()
        .map(|row| (row.thread_id.clone(), row.agent_id.clone()))
        .collect::<HashMap<_, _>>();
    transaction.execute_batch(
        r#"
        DROP TRIGGER IF EXISTS thread_authorities_no_update;
        DROP TRIGGER IF EXISTS thread_authorities_no_delete;
        ALTER TABLE thread_authorities RENAME TO thread_authorities_v1;

        CREATE TABLE thread_authorities (
          thread_id TEXT PRIMARY KEY,
          parent_thread_id TEXT,
          spawning_actor TEXT NOT NULL,
          cwd TEXT,
          agent_id TEXT,
          workspace_id TEXT,
          profile_id TEXT,
          profile_revision INTEGER,
          thread_kind TEXT NOT NULL,
          legacy_reason TEXT,
          model TEXT NOT NULL,
          reasoning_effort TEXT NOT NULL,
          approval_policy TEXT NOT NULL,
          toolset TEXT,
          worktrees TEXT,
          granted_paths TEXT,
          network INTEGER,
          created_at_ms INTEGER NOT NULL,
          CHECK (thread_kind IN ('home', 'child', 'legacy')),
          CHECK (
            (profile_id IS NULL AND profile_revision IS NULL)
            OR (profile_id IS NOT NULL AND profile_revision > 0)
          ),
          CHECK (
            (thread_kind = 'legacy' AND legacy_reason IN (
              'additional_root', 'missing_profile', 'cross_profile_edge',
              'missing_workspace', 'unsupported_authority'
            ))
            OR (
              thread_kind IN ('home', 'child')
              AND legacy_reason IS NULL
              AND workspace_id IS NOT NULL
              AND profile_id IS NOT NULL
            )
          ),
          FOREIGN KEY (profile_id) REFERENCES profiles(id)
        );
        "#,
    )?;
    for row in rows {
        let imported = row.agent_id.as_ref().and_then(|id| agents.get(id));
        let profile_id = imported.and_then(|agent| agent.profile_id.as_ref());
        let legacy_reason = if imported.is_some_and(|agent| agent.missing_workspace) {
            "missing_workspace"
        } else if profile_id.is_none() {
            "missing_profile"
        } else if row.parent_thread_id.is_none() {
            "additional_root"
        } else {
            let parent_profile = row
                .parent_thread_id
                .as_ref()
                .and_then(|parent| thread_agents.get(parent))
                .and_then(Option::as_ref)
                .and_then(|agent_id| agents.get(agent_id))
                .and_then(|agent| agent.profile_id.as_ref());
            if parent_profile.is_some_and(|parent| Some(parent) != profile_id) {
                "cross_profile_edge"
            } else {
                "unsupported_authority"
            }
        };
        transaction.execute(
            "INSERT INTO thread_authorities
               (thread_id, parent_thread_id, spawning_actor, cwd, agent_id,
                workspace_id, profile_id, profile_revision, thread_kind,
                legacy_reason, model, reasoning_effort, approval_policy,
                toolset, worktrees, granted_paths, network, created_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'legacy', ?9,
                     ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            params![
                row.thread_id,
                row.parent_thread_id,
                row.spawning_actor,
                row.cwd,
                row.agent_id,
                imported.map(|agent| agent.workspace_id.as_str()),
                profile_id.map(ProfileId::as_str),
                profile_id.map(|_| 1_i64),
                legacy_reason,
                row.model,
                row.reasoning_effort,
                row.approval_policy,
                row.toolset,
                row.worktrees,
                row.granted_paths,
                row.network,
                row.created_at_ms,
            ],
        )?;
    }
    transaction.execute_batch(
        r#"
        DROP TABLE thread_authorities_v1;

        CREATE UNIQUE INDEX one_non_legacy_home_per_profile
          ON thread_authorities(profile_id)
          WHERE thread_kind = 'home';

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
pub(super) fn create_tool_call_approval_table(connection: &Connection) -> AppResult<()> {
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

pub(super) fn create_run_cancellation_table(connection: &Connection) -> AppResult<()> {
    connection.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS run_cancellations (
          run_id TEXT PRIMARY KEY,
          actor TEXT NOT NULL,
          requested_at_ms INTEGER NOT NULL
        );

        CREATE TRIGGER IF NOT EXISTS run_cancellations_no_update
        BEFORE UPDATE ON run_cancellations
        BEGIN
          SELECT RAISE(ABORT, 'run cancellation records are immutable');
        END;

        CREATE TRIGGER IF NOT EXISTS run_cancellations_no_delete
        BEFORE DELETE ON run_cancellations
        BEGIN
          SELECT RAISE(ABORT, 'run cancellation records are immutable');
        END;
        "#,
    )?;
    Ok(())
}

pub(super) fn create_thread_authority_tables(connection: &Connection) -> AppResult<()> {
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
        connection.execute_batch(
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

pub(super) fn create_thread_stop_table(connection: &Connection) -> AppResult<()> {
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

pub(super) fn create_thread_repository_tables(connection: &Connection) -> AppResult<()> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn journal(connection: &Connection) -> Vec<(u32, String)> {
        connection
            .prepare("SELECT version, name FROM server_schema_migrations ORDER BY version")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    #[test]
    fn migration_journal_steps_monotonically_and_reopen_is_idempotent() {
        let mut connection = Connection::open_in_memory().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        migrate_baseline(&transaction).unwrap();
        transaction.commit().unwrap();
        assert_eq!(
            connection
                .pragma_query_value::<u32, _>(None, "user_version", |row| row.get(0))
                .unwrap(),
            1
        );
        assert_eq!(journal(&connection), [(1, BASE_MIGRATION_NAME.into())]);

        migrate_server_schema(&mut connection).unwrap();
        assert_eq!(
            connection
                .pragma_query_value::<u32, _>(None, "user_version", |row| row.get(0))
                .unwrap(),
            SERVER_SCHEMA_VERSION
        );
        assert_eq!(
            journal(&connection),
            [
                (1, BASE_MIGRATION_NAME.into()),
                (2, PROFILE_MIGRATION_NAME.into())
            ]
        );
        let schema_before: String = connection
            .query_row(
                "SELECT group_concat(sql, '\n') FROM sqlite_schema WHERE sql IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        migrate_server_schema(&mut connection).unwrap();
        let schema_after: String = connection
            .query_row(
                "SELECT group_concat(sql, '\n') FROM sqlite_schema WHERE sql IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(schema_after, schema_before);
        assert_eq!(journal(&connection).len(), 2);
    }

    #[test]
    fn future_or_unjournaled_server_schema_fails_closed() {
        let mut future = Connection::open_in_memory().unwrap();
        future
            .pragma_update(None, "user_version", SERVER_SCHEMA_VERSION + 1)
            .unwrap();
        assert!(matches!(
            migrate_server_schema(&mut future),
            Err(AppError::SqliteSchemaVersion {
                expected: SERVER_SCHEMA_VERSION,
                actual: 3,
            })
        ));

        let mut unjournaled = Connection::open_in_memory().unwrap();
        unjournaled
            .pragma_update(None, "user_version", BASE_SCHEMA_VERSION)
            .unwrap();
        assert!(matches!(
            migrate_server_schema(&mut unjournaled),
            Err(AppError::Config(message)) if message.contains("no migration journal")
        ));
    }
}
