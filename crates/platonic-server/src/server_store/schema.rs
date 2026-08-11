use crate::AppResult;
use rusqlite::{Connection, TransactionBehavior};

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

pub(super) fn create_thread_authority_tables(connection: &mut Connection) -> AppResult<()> {
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
