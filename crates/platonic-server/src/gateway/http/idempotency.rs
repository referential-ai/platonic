use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::{path::Path, time::Duration};

const COMPLETED_TTL_MS: u64 = 24 * 60 * 60 * 1_000;
const AMBIGUOUS_TTL_MS: u64 = 7 * COMPLETED_TTL_MS;
const MAX_ROWS: u64 = 100_000;
const MAX_REPLAY_BODY: usize = 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub(super) enum IdempotencyError {
    #[error("idempotency store is full")]
    Full,
    #[error("invalid idempotency state: {0}")]
    InvalidState(String),
    #[error("idempotency response exceeds {MAX_REPLAY_BODY} bytes")]
    ResponseTooLarge,
    #[error("idempotency I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("idempotency SQLite failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StoredResponse {
    pub(super) status: u16,
    pub(super) body: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Reservation {
    Fresh,
    InProgress,
    Replay(StoredResponse),
    Conflict,
    Ambiguous,
}

pub(super) struct IdempotencyStore {
    connection: Connection,
    max_rows: u64,
}

impl IdempotencyStore {
    pub(super) fn open(path: &Path, now_ms: u64) -> Result<Self, IdempotencyError> {
        Self::open_with_max_rows(path, now_ms, MAX_ROWS)
    }

    fn open_with_max_rows(
        path: &Path,
        now_ms: u64,
        max_rows: u64,
    ) -> Result<Self, IdempotencyError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_millis(100))?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "locking_mode", "EXCLUSIVE")?;
        connection.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS requests (
              principal TEXT NOT NULL,
              key_hash BLOB NOT NULL,
              fingerprint BLOB NOT NULL,
              state TEXT NOT NULL CHECK(state IN ('in_flight', 'completed', 'known_error', 'ambiguous')),
              created_at_ms INTEGER NOT NULL,
              updated_at_ms INTEGER NOT NULL,
              expires_at_ms INTEGER NOT NULL,
              status INTEGER,
              body BLOB,
              PRIMARY KEY (principal, key_hash),
              CHECK(body IS NULL OR length(body) <= 1048576)
            );
            CREATE INDEX IF NOT EXISTS requests_expiry ON requests(expires_at_ms);
            "#,
        )?;
        // EXCLUSIVE persists for this connection after the first transaction.
        connection.execute_batch("BEGIN EXCLUSIVE; COMMIT;")?;
        connection.execute(
            "UPDATE requests
                SET state = 'ambiguous', updated_at_ms = ?1, expires_at_ms = ?2,
                    status = NULL, body = NULL
              WHERE state = 'in_flight'",
            params![sqlite_i64(now_ms)?, sqlite_i64(now_ms + AMBIGUOUS_TTL_MS)?],
        )?;
        Ok(Self {
            connection,
            max_rows,
        })
    }

    pub(super) fn reserve(
        &mut self,
        principal: &str,
        key_hash: &[u8; 32],
        fingerprint: &[u8; 32],
        now_ms: u64,
    ) -> Result<Reservation, IdempotencyError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "DELETE FROM requests WHERE expires_at_ms <= ?1",
            params![sqlite_i64(now_ms)?],
        )?;
        let existing = transaction
            .query_row(
                "SELECT fingerprint, state, status, body
                   FROM requests WHERE principal = ?1 AND key_hash = ?2",
                params![principal, key_hash.as_slice()],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, Option<Vec<u8>>>(3)?,
                    ))
                },
            )
            .optional()?;
        if let Some((stored_fingerprint, state, status, body)) = existing {
            transaction.commit()?;
            if stored_fingerprint.as_slice() != fingerprint {
                return Ok(Reservation::Conflict);
            }
            return match state.as_str() {
                "in_flight" => Ok(Reservation::InProgress),
                "ambiguous" => Ok(Reservation::Ambiguous),
                "completed" | "known_error" => {
                    let status = status.ok_or_else(|| {
                        IdempotencyError::InvalidState(format!("{state} row has no HTTP status"))
                    })?;
                    let status = u16::try_from(status).map_err(|_| {
                        IdempotencyError::InvalidState(format!(
                            "{state} row has invalid HTTP status"
                        ))
                    })?;
                    let body = body.ok_or_else(|| {
                        IdempotencyError::InvalidState(format!("{state} row has no body"))
                    })?;
                    Ok(Reservation::Replay(StoredResponse { status, body }))
                }
                other => Err(IdempotencyError::InvalidState(other.into())),
            };
        }

        let row_count: i64 =
            transaction.query_row("SELECT COUNT(*) FROM requests", [], |row| row.get(0))?;
        if row_count >= self.max_rows as i64 {
            return Err(IdempotencyError::Full);
        }
        transaction.execute(
            "INSERT INTO requests
                (principal, key_hash, fingerprint, state, created_at_ms, updated_at_ms, expires_at_ms)
             VALUES (?1, ?2, ?3, 'in_flight', ?4, ?4, ?5)",
            params![
                principal,
                key_hash.as_slice(),
                fingerprint.as_slice(),
                sqlite_i64(now_ms)?,
                sqlite_i64(now_ms + AMBIGUOUS_TTL_MS)?
            ],
        )?;
        transaction.commit()?;
        Ok(Reservation::Fresh)
    }

    pub(super) fn complete(
        &mut self,
        principal: &str,
        key_hash: &[u8; 32],
        fingerprint: &[u8; 32],
        response: &StoredResponse,
        known_error: bool,
        now_ms: u64,
    ) -> Result<(), IdempotencyError> {
        if response.body.len() > MAX_REPLAY_BODY {
            return Err(IdempotencyError::ResponseTooLarge);
        }
        let state = if known_error {
            "known_error"
        } else {
            "completed"
        };
        let changed = self.connection.execute(
            "UPDATE requests
                SET state = ?1, updated_at_ms = ?2, expires_at_ms = ?3,
                    status = ?4, body = ?5
              WHERE principal = ?6 AND key_hash = ?7 AND fingerprint = ?8
                AND state = 'in_flight'",
            params![
                state,
                sqlite_i64(now_ms)?,
                sqlite_i64(now_ms + COMPLETED_TTL_MS)?,
                i64::from(response.status),
                response.body,
                principal,
                key_hash.as_slice(),
                fingerprint.as_slice()
            ],
        )?;
        if changed != 1 {
            return Err(IdempotencyError::InvalidState(
                "completion did not own one in-flight row".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn mark_ambiguous(
        &mut self,
        principal: &str,
        key_hash: &[u8; 32],
        fingerprint: &[u8; 32],
        now_ms: u64,
    ) -> Result<(), IdempotencyError> {
        let changed = self.connection.execute(
            "UPDATE requests
                SET state = 'ambiguous', updated_at_ms = ?1, expires_at_ms = ?2,
                    status = NULL, body = NULL
              WHERE principal = ?3 AND key_hash = ?4 AND fingerprint = ?5
                AND state = 'in_flight'",
            params![
                sqlite_i64(now_ms)?,
                sqlite_i64(now_ms + AMBIGUOUS_TTL_MS)?,
                principal,
                key_hash.as_slice(),
                fingerprint.as_slice()
            ],
        )?;
        if changed != 1 {
            return Err(IdempotencyError::InvalidState(
                "ambiguity transition did not own one in-flight row".into(),
            ));
        }
        Ok(())
    }
}

fn sqlite_i64(value: u64) -> Result<i64, IdempotencyError> {
    i64::try_from(value)
        .map_err(|_| IdempotencyError::InvalidState("timestamp exceeds SQLite range".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: [u8; 32] = [1; 32];
    const FINGERPRINT: [u8; 32] = [2; 32];

    #[test]
    fn reservation_state_machine_replays_without_a_second_fresh_admission() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("idempotency.db");
        let mut store = IdempotencyStore::open(&path, 1_000).unwrap();

        assert_eq!(
            store.reserve("remote", &KEY, &FINGERPRINT, 1_000).unwrap(),
            Reservation::Fresh
        );
        assert_eq!(
            store.reserve("remote", &KEY, &FINGERPRINT, 1_001).unwrap(),
            Reservation::InProgress
        );
        assert_eq!(
            store.reserve("remote", &KEY, &[3; 32], 1_001).unwrap(),
            Reservation::Conflict
        );

        let response = StoredResponse {
            status: 200,
            body: br#"{"status":"started"}"#.to_vec(),
        };
        store
            .complete("remote", &KEY, &FINGERPRINT, &response, false, 1_002)
            .unwrap();
        assert_eq!(
            store.reserve("remote", &KEY, &FINGERPRINT, 1_003).unwrap(),
            Reservation::Replay(response)
        );
    }

    #[test]
    fn restart_converts_in_flight_to_ambiguous() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("idempotency.db");
        {
            let mut store = IdempotencyStore::open(&path, 1_000).unwrap();
            assert_eq!(
                store.reserve("remote", &KEY, &FINGERPRINT, 1_000).unwrap(),
                Reservation::Fresh
            );
        }

        let mut restarted = IdempotencyStore::open(&path, 2_000).unwrap();
        assert_eq!(
            restarted
                .reserve("remote", &KEY, &FINGERPRINT, 2_001)
                .unwrap(),
            Reservation::Ambiguous
        );
    }

    #[test]
    fn expiry_is_the_only_eviction_and_row_cap_fails_closed() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("idempotency.db");
        let mut store = IdempotencyStore::open_with_max_rows(&path, 1_000, 1).unwrap();
        store.reserve("remote", &KEY, &FINGERPRINT, 1_000).unwrap();
        assert!(matches!(
            store.reserve("remote", &[4; 32], &[5; 32], 1_001),
            Err(IdempotencyError::Full)
        ));

        store
            .mark_ambiguous("remote", &KEY, &FINGERPRINT, 1_002)
            .unwrap();
        assert_eq!(
            store
                .reserve("remote", &[4; 32], &[5; 32], 1_002 + AMBIGUOUS_TTL_MS + 1,)
                .unwrap(),
            Reservation::Fresh
        );
    }

    #[test]
    fn one_gateway_process_exclusively_owns_the_database() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("idempotency.db");
        let _owner = IdempotencyStore::open(&path, 1_000).unwrap();

        assert!(IdempotencyStore::open(&path, 1_001).is_err());
    }
}
