use crate::{
    daemon::{
        handlers::{thread_session_id, typed_entries_for_run},
        runtime::DaemonRuntime,
    },
    ledger::{SessionRunRecords, SqliteLedger},
    server_store::{ProfileRevisionContent, ProfileRevisionRecord, ServerStore},
    tool_catalog::is_logical_read_tool,
    tools::{
        LogicalReadErrorCode, LogicalReadRequest, LogicalReadResult, LogicalReadToolHandler,
        LogicalReadToolOutput, MAX_LOGICAL_READ_SERIALIZED_BYTES, ProfileContentView,
        ProfileEventEntry, ProfileFilesystemIsolation, ProfileReadResult, ProfileRevisionMetadata,
        ProfileRevisionView, ProfileThreadMetadata, ProfileTranscriptEntry, ThreadEventsResult,
        ThreadTranscriptResult, ThreadTreeResult,
    },
};
use platonic_core::{HarnessEvent, ProfileId, RunIdentity, RunReadback};
use platonic_protocol::{ThreadConfinement, TypedTranscriptEntry};
use serde::Serialize;
use std::fs;

const DEFAULT_LIST_LIMIT: usize = 50;
const MAX_LIST_LIMIT: usize = 100;
const DEFAULT_HISTORY_LIMIT: usize = 50;
const MAX_HISTORY_LIMIT: usize = 256;
const PROFILE_INSTRUCTIONS_MAX_CHARS: usize = 14 * 1024;
const PROFILE_MEMORY_MAX_CHARS: usize = 14 * 1024;
const PROFILE_SKILL_REFS_MAX_CHARS: usize = 4 * 1024;
const CONTENT_TRUNCATION_MARKER: &str = "\n[truncated]";

#[derive(Debug)]
struct ReadError {
    code: LogicalReadErrorCode,
    message: String,
}

type ReadResult<T> = Result<T, ReadError>;

impl ReadError {
    fn new(code: LogicalReadErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    fn invalid(message: impl Into<String>) -> Self {
        Self::new(LogicalReadErrorCode::InvalidRequest, message)
    }

    fn membership() -> Self {
        Self::new(
            LogicalReadErrorCode::MembershipDenied,
            "target is not a member of the current profile",
        )
    }
}

impl From<crate::AppError> for ReadError {
    fn from(_error: crate::AppError) -> Self {
        Self::new(LogicalReadErrorCode::ReadFailed, "profile read failed")
    }
}

impl From<serde_json::Error> for ReadError {
    fn from(error: serde_json::Error) -> Self {
        crate::AppError::from(error).into()
    }
}

impl From<platonic_core::Error> for ReadError {
    fn from(error: platonic_core::Error) -> Self {
        crate::AppError::from(error).into()
    }
}

pub(in crate::daemon) fn projected_handler(
    runtime: &DaemonRuntime,
    caller_thread_id: &str,
    identity: &RunIdentity,
    toolset: &[String],
) -> Option<LogicalReadToolHandler> {
    let RunIdentity::Profile { profile_id, .. } = identity else {
        return None;
    };
    if !toolset.iter().any(|tool| is_logical_read_tool(tool)) {
        return None;
    }
    let runtime = runtime.clone();
    let profile_id = profile_id.clone();
    let caller_thread_id = caller_thread_id.to_owned();
    Some(LogicalReadToolHandler::new(move |request| {
        Ok(handle(&runtime, &profile_id, &caller_thread_id, request))
    }))
}

fn handle(
    runtime: &DaemonRuntime,
    caller_profile_id: &ProfileId,
    caller_thread_id: &str,
    request: LogicalReadRequest,
) -> LogicalReadToolOutput {
    let result = match request {
        LogicalReadRequest::Profile(input) => {
            read_profile(runtime, caller_profile_id, caller_thread_id, input)
                .map(LogicalReadResult::Profile)
        }
        LogicalReadRequest::ThreadTree(input) => {
            read_thread_tree(runtime, caller_profile_id, caller_thread_id, input)
                .map(LogicalReadResult::ThreadTree)
        }
        LogicalReadRequest::ThreadEvents(input) => {
            read_thread_events(runtime, caller_profile_id, caller_thread_id, input)
                .map(LogicalReadResult::ThreadEvents)
        }
        LogicalReadRequest::ThreadTranscript(input) => {
            read_thread_transcript(runtime, caller_profile_id, caller_thread_id, input)
                .map(LogicalReadResult::ThreadTranscript)
        }
    };
    match result {
        Ok(result) => LogicalReadToolOutput::Ok {
            result: Box::new(result),
        },
        Err(error) => LogicalReadToolOutput::error(error.code, error.message),
    }
}

fn open_scope(
    runtime: &DaemonRuntime,
    profile_id: &ProfileId,
    caller_thread_id: &str,
) -> ReadResult<ServerStore> {
    let store = runtime.paths.server_store()?;
    let profile = store.profile(profile_id)?;
    if profile.is_none_or(|profile| profile.workspace_id != runtime.paths.workspace_id)
        || !thread_is_member(
            &store,
            &runtime.paths.workspace_id,
            profile_id,
            caller_thread_id,
        )?
    {
        return Err(ReadError::membership());
    }
    Ok(store)
}

fn require_own_profile(target: Option<&str>, caller: &ProfileId) -> ReadResult<()> {
    if target.is_some_and(|target| target != caller.as_str()) {
        return Err(ReadError::new(
            LogicalReadErrorCode::CrossProfile,
            "cross-profile reads are denied",
        ));
    }
    Ok(())
}

fn thread_is_member(
    store: &ServerStore,
    workspace_id: &str,
    profile_id: &ProfileId,
    thread_id: &str,
) -> ReadResult<bool> {
    let Some(authority) = store.thread_authority(thread_id)? else {
        return Ok(false);
    };
    let Some(classification) = store.thread_profile_authority(thread_id)? else {
        return Ok(false);
    };
    Ok(authority.profile_id.as_ref() == Some(profile_id)
        && classification.profile_id.as_ref() == Some(profile_id)
        && classification.workspace_id.as_deref() == Some(workspace_id))
}

fn read_profile(
    runtime: &DaemonRuntime,
    caller_profile_id: &ProfileId,
    caller_thread_id: &str,
    input: crate::tools::ProfileReadInput,
) -> ReadResult<ProfileReadResult> {
    require_own_profile(input.profile_id.as_deref(), caller_profile_id)?;
    let store = open_scope(runtime, caller_profile_id, caller_thread_id)?;
    let profile = store
        .profile(caller_profile_id)?
        .ok_or_else(ReadError::membership)?;
    let selected_revision = input.revision.unwrap_or(profile.current_revision);
    let selected = store
        .profile_revision(caller_profile_id, selected_revision)?
        .ok_or_else(|| {
            ReadError::new(
                LogicalReadErrorCode::NotFound,
                format!("profile revision not found: {selected_revision}"),
            )
        })?;
    let limit = list_limit(input.limit)?;
    let cursor = parse_revision_cursor(input.cursor.as_deref())?;
    let mut revisions = store.profile_revisions(caller_profile_id, cursor, limit + 1)?;
    let truncated = revisions.len() > limit;
    if truncated {
        revisions.truncate(limit);
    }
    let next_cursor = truncated.then(|| {
        revisions
            .last()
            .expect("a truncated profile page is non-empty")
            .revision
            .to_string()
    });
    Ok(ProfileReadResult {
        profile_id: caller_profile_id.to_string(),
        current_revision: profile.current_revision,
        selected: revision_view(&selected),
        revisions: revisions.iter().map(revision_metadata).collect(),
        truncated,
        next_cursor,
    })
}

fn revision_metadata(revision: &ProfileRevisionRecord) -> ProfileRevisionMetadata {
    ProfileRevisionMetadata {
        revision: revision.revision,
        parent_revision: revision.parent_revision,
        actor: revision.actor.clone(),
        created_at_ms: revision.created_at_ms,
        content_hash: revision.content_hash.clone(),
    }
}

fn revision_view(revision: &ProfileRevisionRecord) -> ProfileRevisionView {
    ProfileRevisionView {
        metadata: revision_metadata(revision),
        content: bounded_profile_content(&revision.content),
    }
}

fn bounded_profile_content(content: &ProfileRevisionContent) -> ProfileContentView {
    let (instructions_markdown, instructions_truncated) = truncate_chars(
        &content.instructions_markdown,
        PROFILE_INSTRUCTIONS_MAX_CHARS,
    );
    let (memory_markdown, memory_truncated) =
        truncate_chars(&content.memory_markdown, PROFILE_MEMORY_MAX_CHARS);
    let all_skill_chars = content
        .skill_refs
        .iter()
        .map(|skill_ref| skill_ref.chars().count())
        .sum::<usize>();
    let skills_truncated = all_skill_chars > PROFILE_SKILL_REFS_MAX_CHARS;
    let skill_budget = if skills_truncated {
        PROFILE_SKILL_REFS_MAX_CHARS
            .saturating_sub("[remaining skill references truncated]".chars().count())
    } else {
        PROFILE_SKILL_REFS_MAX_CHARS
    };
    let mut skill_refs = Vec::new();
    let mut skill_chars = 0usize;
    for skill_ref in &content.skill_refs {
        let chars = skill_ref.chars().count();
        if skill_chars.saturating_add(chars) > skill_budget {
            break;
        }
        skill_chars += chars;
        skill_refs.push(skill_ref.clone());
    }
    if skills_truncated {
        skill_refs.push("[remaining skill references truncated]".into());
    }
    ProfileContentView {
        instructions_markdown,
        memory_markdown,
        skill_refs,
        truncated: instructions_truncated || memory_truncated || skills_truncated,
    }
}

fn truncate_chars(value: &str, limit: usize) -> (String, bool) {
    if value.chars().count() <= limit {
        return (value.into(), false);
    }
    let retained = limit.saturating_sub(CONTENT_TRUNCATION_MARKER.chars().count());
    let mut value = value.chars().take(retained).collect::<String>();
    value.push_str(CONTENT_TRUNCATION_MARKER);
    (value, true)
}

fn read_thread_tree(
    runtime: &DaemonRuntime,
    caller_profile_id: &ProfileId,
    caller_thread_id: &str,
    input: crate::tools::ThreadTreeInput,
) -> ReadResult<ThreadTreeResult> {
    require_own_profile(input.profile_id.as_deref(), caller_profile_id)?;
    let store = open_scope(runtime, caller_profile_id, caller_thread_id)?;
    let limit = list_limit(input.limit)?;
    let cursor = parse_thread_cursor(input.cursor.as_deref())?;
    let rows = store.profile_thread_authorities(
        caller_profile_id,
        cursor
            .as_ref()
            .map(|(created_at_ms, thread_id)| (*created_at_ms, thread_id.as_str())),
        limit + 1,
    )?;
    let mut threads = Vec::with_capacity(rows.len().min(limit));
    let mut next_cursor = None;
    for authority in rows {
        if threads.len() == limit {
            next_cursor = threads.last().map(thread_cursor);
            break;
        }
        if !thread_is_member(
            &store,
            &runtime.paths.workspace_id,
            caller_profile_id,
            &authority.thread_id,
        )? {
            return Err(ReadError::membership());
        }
        if let Some(parent) = authority.parent_thread_id.as_deref()
            && !thread_is_member(
                &store,
                &runtime.paths.workspace_id,
                caller_profile_id,
                parent,
            )?
        {
            return Err(ReadError::membership());
        }
        let confinement = store
            .thread_confinement(&authority.thread_id)?
            .unwrap_or(ThreadConfinement::None);
        let stopped_at_ms = store
            .thread_stop(&authority.thread_id)?
            .map(|stop| stop.occurred_at_ms);
        let thread = ProfileThreadMetadata {
            thread_id: authority.thread_id,
            parent_thread_id: authority.parent_thread_id,
            profile_revision: authority
                .profile_revision
                .ok_or_else(ReadError::membership)?,
            thread_kind: authority.thread_kind,
            created_at_ms: authority.created_at_ms,
            stopped_at_ms,
            confinement,
            profile_filesystem_isolation: match confinement {
                ThreadConfinement::Landlock => ProfileFilesystemIsolation::Confined,
                ThreadConfinement::None => ProfileFilesystemIsolation::Unconfined,
            },
        };
        let mut candidate = threads.clone();
        candidate.push(thread.clone());
        if !serialized_fits(&LogicalReadToolOutput::Ok {
            result: Box::new(LogicalReadResult::ThreadTree(ThreadTreeResult {
                profile_id: caller_profile_id.to_string(),
                threads: candidate,
                truncated: true,
                next_cursor: Some(thread_cursor(&thread)),
            })),
        })? {
            next_cursor = threads.last().map(thread_cursor);
            if next_cursor.is_none() {
                return Err(ReadError::new(
                    LogicalReadErrorCode::ReadFailed,
                    "one thread metadata entry exceeds the response cap",
                ));
            }
            break;
        }
        threads.push(thread);
    }
    Ok(ThreadTreeResult {
        profile_id: caller_profile_id.to_string(),
        truncated: next_cursor.is_some(),
        next_cursor,
        threads,
    })
}

fn thread_cursor(thread: &ProfileThreadMetadata) -> String {
    format!("{}:{}", thread.created_at_ms, thread.thread_id)
}

fn read_thread_events(
    runtime: &DaemonRuntime,
    caller_profile_id: &ProfileId,
    caller_thread_id: &str,
    input: crate::tools::ThreadHistoryInput,
) -> ReadResult<ThreadEventsResult> {
    let target_thread_id = input.thread_id.as_deref().unwrap_or(caller_thread_id);
    let store = open_scope(runtime, caller_profile_id, caller_thread_id)?;
    require_thread_member(runtime, &store, caller_profile_id, target_thread_id)?;
    let limit = history_limit(input.limit)?;
    let mut source = HistorySource::open(
        runtime,
        caller_profile_id,
        target_thread_id,
        input.run_id.as_deref(),
        input.cursor.as_deref(),
    )?;
    let mut entries = Vec::new();
    let mut content_truncated = false;
    while let Some((position, run, record)) = source.next_event()? {
        if entries.len() == limit {
            return Ok(ThreadEventsResult {
                thread_id: target_thread_id.into(),
                entries,
                truncated: true,
                next_cursor: Some(position),
            });
        }
        let normal = ProfileEventEntry::Event {
            session_index: run.session_index,
            run_id: run.run_id.clone(),
            record: record.clone(),
        };
        let entry = if event_entry_fits(target_thread_id, &normal)? {
            normal
        } else {
            content_truncated = true;
            ProfileEventEntry::Omitted {
                session_index: run.session_index,
                run_id: run.run_id.clone(),
                sequence: record.seq,
                event: event_name(&record.event),
                serialized_bytes: serde_json::to_vec(&normal)?.len(),
                reason: "entry exceeded the serialized response cap".into(),
            }
        };
        let mut candidate = entries.clone();
        candidate.push(entry.clone());
        if !serialized_fits(&LogicalReadToolOutput::Ok {
            result: Box::new(LogicalReadResult::ThreadEvents(ThreadEventsResult {
                thread_id: target_thread_id.into(),
                entries: candidate,
                truncated: true,
                next_cursor: Some(position.clone()),
            })),
        })? {
            return Ok(ThreadEventsResult {
                thread_id: target_thread_id.into(),
                entries,
                truncated: true,
                next_cursor: Some(position),
            });
        }
        entries.push(entry);
    }
    Ok(ThreadEventsResult {
        thread_id: target_thread_id.into(),
        entries,
        truncated: content_truncated,
        next_cursor: None,
    })
}

fn read_thread_transcript(
    runtime: &DaemonRuntime,
    caller_profile_id: &ProfileId,
    caller_thread_id: &str,
    input: crate::tools::ThreadHistoryInput,
) -> ReadResult<ThreadTranscriptResult> {
    let target_thread_id = input.thread_id.as_deref().unwrap_or(caller_thread_id);
    let store = open_scope(runtime, caller_profile_id, caller_thread_id)?;
    require_thread_member(runtime, &store, caller_profile_id, target_thread_id)?;
    let limit = history_limit(input.limit)?;
    let mut source = HistorySource::open(
        runtime,
        caller_profile_id,
        target_thread_id,
        input.run_id.as_deref(),
        input.cursor.as_deref(),
    )?;
    let mut entries = Vec::new();
    let mut content_truncated = false;
    while let Some((position, run, value)) = source.next_transcript()? {
        if entries.len() == limit {
            return Ok(ThreadTranscriptResult {
                thread_id: target_thread_id.into(),
                entries,
                truncated: true,
                next_cursor: Some(position),
            });
        }
        let normal = ProfileTranscriptEntry::Transcript {
            session_index: run.session_index,
            run_id: run.run_id.clone(),
            status: run.status,
            value,
        };
        let entry = if transcript_entry_fits(target_thread_id, &normal)? {
            normal
        } else {
            content_truncated = true;
            ProfileTranscriptEntry::Omitted {
                session_index: run.session_index,
                run_id: run.run_id.clone(),
                status: run.status,
                serialized_bytes: serde_json::to_vec(&normal)?.len(),
                reason: "entry exceeded the serialized response cap".into(),
            }
        };
        let mut candidate = entries.clone();
        candidate.push(entry.clone());
        if !serialized_fits(&LogicalReadToolOutput::Ok {
            result: Box::new(LogicalReadResult::ThreadTranscript(
                ThreadTranscriptResult {
                    thread_id: target_thread_id.into(),
                    entries: candidate,
                    truncated: true,
                    next_cursor: Some(position.clone()),
                },
            )),
        })? {
            return Ok(ThreadTranscriptResult {
                thread_id: target_thread_id.into(),
                entries,
                truncated: true,
                next_cursor: Some(position),
            });
        }
        entries.push(entry);
    }
    Ok(ThreadTranscriptResult {
        thread_id: target_thread_id.into(),
        entries,
        truncated: content_truncated,
        next_cursor: None,
    })
}

fn require_thread_member(
    runtime: &DaemonRuntime,
    store: &ServerStore,
    profile_id: &ProfileId,
    thread_id: &str,
) -> ReadResult<()> {
    if !thread_is_member(store, &runtime.paths.workspace_id, profile_id, thread_id)? {
        return Err(ReadError::membership());
    }
    Ok(())
}

struct HistorySource {
    ledger: SqliteLedger,
    profile_id: ProfileId,
    session_id: String,
    exact_run_id: Option<String>,
    session_index: u64,
    entry_index: usize,
    current_run: Option<SessionRunRecords>,
    transcript_entries: Option<Vec<TypedTranscriptEntry>>,
}

#[derive(Clone)]
struct HistoryRunMetadata {
    run_id: String,
    session_index: u64,
    status: platonic_protocol::RunStateName,
}

impl From<&SessionRunRecords> for HistoryRunMetadata {
    fn from(run: &SessionRunRecords) -> Self {
        Self {
            run_id: run.run_id.clone(),
            session_index: run.session_index,
            status: run.status,
        }
    }
}

impl HistorySource {
    fn open(
        runtime: &DaemonRuntime,
        profile_id: &ProfileId,
        thread_id: &str,
        run_id: Option<&str>,
        cursor: Option<&str>,
    ) -> ReadResult<Self> {
        let path = runtime.paths.default_ledger();
        if fs::symlink_metadata(path.as_path())
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
        {
            return Err(ReadError::new(
                LogicalReadErrorCode::NotFound,
                "thread has no committed history",
            ));
        }
        let ledger = SqliteLedger::open_default_readonly(&path)?;
        let session_id = thread_session_id(thread_id);
        let exact = match run_id {
            Some(run_id) => {
                let location = ledger
                    .run_session_location(run_id)?
                    .filter(|(candidate, _)| candidate == &session_id)
                    .ok_or_else(ReadError::membership)?;
                Some((run_id.to_owned(), location.1))
            }
            None => None,
        };
        let (session_index, entry_index) = parse_history_cursor(cursor)?.unwrap_or_else(|| {
            exact
                .as_ref()
                .map_or((0, 0), |(_, session_index)| (*session_index, 0))
        });
        if exact
            .as_ref()
            .is_some_and(|(_, expected)| *expected != session_index)
        {
            return Err(ReadError::invalid(
                "cursor does not belong to the selected run",
            ));
        }
        Ok(Self {
            ledger,
            profile_id: profile_id.clone(),
            session_id,
            exact_run_id: exact.map(|(run_id, _)| run_id),
            session_index,
            entry_index,
            current_run: None,
            transcript_entries: None,
        })
    }

    fn next_event(
        &mut self,
    ) -> ReadResult<Option<(String, HistoryRunMetadata, platonic_core::RecordedEvent)>> {
        loop {
            self.ensure_run()?;
            let Some(run) = self.current_run.as_ref() else {
                return Ok(None);
            };
            if self.entry_index > run.records.len() {
                return Err(ReadError::invalid("event cursor is out of range"));
            }
            if let Some(record) = run.records.get(self.entry_index).cloned() {
                let position = history_cursor(run.session_index, self.entry_index);
                self.entry_index += 1;
                return Ok(Some((position, run.into(), record)));
            }
            if self.exact_run_id.is_some() {
                return Ok(None);
            }
            self.advance_run()?;
        }
    }

    fn next_transcript(
        &mut self,
    ) -> ReadResult<Option<(String, HistoryRunMetadata, TypedTranscriptEntry)>> {
        loop {
            self.ensure_run()?;
            let Some(run) = self.current_run.as_ref() else {
                return Ok(None);
            };
            if self.transcript_entries.is_none() {
                self.transcript_entries = Some(typed_entries_for_run(run)?);
            }
            let values = self
                .transcript_entries
                .as_ref()
                .expect("transcript projection was initialized");
            if self.entry_index > values.len() {
                return Err(ReadError::invalid("transcript cursor is out of range"));
            }
            if let Some(value) = values.get(self.entry_index).cloned() {
                let position = history_cursor(run.session_index, self.entry_index);
                self.entry_index += 1;
                return Ok(Some((position, run.into(), value)));
            }
            if self.exact_run_id.is_some() {
                return Ok(None);
            }
            self.advance_run()?;
        }
    }

    fn ensure_run(&mut self) -> ReadResult<()> {
        if self.current_run.is_some() {
            return Ok(());
        }
        let run = self
            .ledger
            .read_session_run_at_or_after(&self.session_id, self.session_index)
            .map_err(|error| match error {
                crate::AppError::SessionNotFound(_) => ReadError::new(
                    LogicalReadErrorCode::NotFound,
                    "thread has no committed history",
                ),
                error => error.into(),
            })?;
        let Some(run) = run else {
            return Ok(());
        };
        if run.session_index != self.session_index && self.entry_index != 0 {
            return Err(ReadError::invalid("history cursor is out of range"));
        }
        if self
            .exact_run_id
            .as_ref()
            .is_some_and(|run_id| run_id != &run.run_id)
        {
            return Err(ReadError::membership());
        }
        let readback = RunReadback::from_events(&run.records)?;
        if !matches!(
            readback.identity,
            Some(RunIdentity::Profile { ref profile_id, .. }) if profile_id == &self.profile_id
        ) {
            return Err(ReadError::membership());
        }
        self.session_index = run.session_index;
        self.current_run = Some(run);
        Ok(())
    }

    fn advance_run(&mut self) -> ReadResult<()> {
        self.session_index = self
            .session_index
            .checked_add(1)
            .ok_or_else(|| ReadError::invalid("history cursor overflowed"))?;
        self.entry_index = 0;
        self.current_run = None;
        self.transcript_entries = None;
        Ok(())
    }
}

fn list_limit(limit: Option<usize>) -> ReadResult<usize> {
    bounded_limit(limit, DEFAULT_LIST_LIMIT, MAX_LIST_LIMIT)
}

fn history_limit(limit: Option<usize>) -> ReadResult<usize> {
    bounded_limit(limit, DEFAULT_HISTORY_LIMIT, MAX_HISTORY_LIMIT)
}

fn bounded_limit(limit: Option<usize>, default: usize, maximum: usize) -> ReadResult<usize> {
    let limit = limit.unwrap_or(default);
    if limit == 0 || limit > maximum {
        return Err(ReadError::invalid(format!(
            "limit must be between 1 and {maximum}"
        )));
    }
    Ok(limit)
}

fn parse_revision_cursor(cursor: Option<&str>) -> ReadResult<u64> {
    cursor
        .map(|cursor| {
            cursor
                .parse()
                .map_err(|_| ReadError::invalid("invalid profile revision cursor"))
        })
        .transpose()
        .map(Option::unwrap_or_default)
}

fn parse_thread_cursor(cursor: Option<&str>) -> ReadResult<Option<(u64, String)>> {
    cursor
        .map(|cursor| {
            let (created_at_ms, thread_id) = cursor
                .split_once(':')
                .ok_or_else(|| ReadError::invalid("invalid thread cursor"))?;
            if thread_id.is_empty() {
                return Err(ReadError::invalid("invalid thread cursor"));
            }
            Ok((
                created_at_ms
                    .parse()
                    .map_err(|_| ReadError::invalid("invalid thread cursor"))?,
                thread_id.into(),
            ))
        })
        .transpose()
}

fn parse_history_cursor(cursor: Option<&str>) -> ReadResult<Option<(u64, usize)>> {
    cursor
        .map(|cursor| {
            let (session_index, entry_index) = cursor
                .split_once(':')
                .ok_or_else(|| ReadError::invalid("invalid history cursor"))?;
            Ok((
                session_index
                    .parse()
                    .map_err(|_| ReadError::invalid("invalid history cursor"))?,
                entry_index
                    .parse()
                    .map_err(|_| ReadError::invalid("invalid history cursor"))?,
            ))
        })
        .transpose()
}

fn history_cursor(session_index: u64, entry_index: usize) -> String {
    format!("{session_index}:{entry_index}")
}

fn event_name(event: &HarnessEvent) -> String {
    serde_json::to_value(event)
        .ok()
        .and_then(|value| value.get("event")?.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unknown".into())
}

fn event_entry_fits(thread_id: &str, entry: &ProfileEventEntry) -> ReadResult<bool> {
    serialized_fits(&LogicalReadToolOutput::Ok {
        result: Box::new(LogicalReadResult::ThreadEvents(ThreadEventsResult {
            thread_id: thread_id.into(),
            entries: vec![entry.clone()],
            truncated: true,
            next_cursor: Some("18446744073709551615:18446744073709551615".into()),
        })),
    })
}

fn transcript_entry_fits(thread_id: &str, entry: &ProfileTranscriptEntry) -> ReadResult<bool> {
    serialized_fits(&LogicalReadToolOutput::Ok {
        result: Box::new(LogicalReadResult::ThreadTranscript(
            ThreadTranscriptResult {
                thread_id: thread_id.into(),
                entries: vec![entry.clone()],
                truncated: true,
                next_cursor: Some("18446744073709551615:18446744073709551615".into()),
            },
        )),
    })
}

fn serialized_fits(value: &impl Serialize) -> ReadResult<bool> {
    Ok(serde_json::to_vec(value)?.len() < MAX_LOGICAL_READ_SERIALIZED_BYTES)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        confinement::ConfinementSupport,
        daemon::{
            handlers::{
                grant_thread, pending_spawn, start_thread_for_logical_read, thread_test_runtime,
            },
            runtime::DaemonRuntime,
        },
        ledger::SqliteLedger,
        server_store::ProfileRevisionContent,
        tool_catalog::{
            PROFILE_READ, THREAD_EVENTS_READ, THREAD_TRANSCRIPT_READ, THREAD_TREE_READ,
        },
        tools::{ProfileReadInput, ThreadHistoryInput, ThreadTreeInput},
    };
    use platonic_core::{
        ContextPack, HarnessEvent, Message, MessageRole, ModelName, RecordedEvent, RunId,
        RunStartedEvent, TurnId,
    };
    use platonic_protocol::ThreadApprovalPolicy;

    fn profile_runtime() -> (tempfile::TempDir, DaemonRuntime, String, ProfileId) {
        let (root, detected_runtime) = thread_test_runtime();
        let runtime = DaemonRuntime::new_with_server_policy(
            detected_runtime.paths,
            1,
            false,
            ConfinementSupport::None,
        );
        fs::write(
            runtime.paths.workspace_root.join("plato.toml"),
            format!(
                "[tools]\nenabled = [{PROFILE_READ:?}, {THREAD_TREE_READ:?}, {THREAD_EVENTS_READ:?}, {THREAD_TRANSCRIPT_READ:?}]\n"
            ),
        )
        .unwrap();
        let (spawn_id, thread_id) = pending_spawn(start_thread_for_logical_read(
            &runtime,
            &runtime.paths.workspace_root,
            ThreadApprovalPolicy::Prompt,
        ));
        grant_thread(&runtime, &spawn_id, "profile-test");
        let profile_id = runtime
            .paths
            .server_store()
            .unwrap()
            .thread_authority(&thread_id)
            .unwrap()
            .unwrap()
            .profile_id
            .unwrap();
        (root, runtime, thread_id, profile_id)
    }

    fn write_profile_run(
        runtime: &DaemonRuntime,
        thread_id: &str,
        profile_id: &ProfileId,
        answer: &str,
    ) -> String {
        let run_id = RunId::new("run_profile_history").unwrap();
        let turn_id = TurnId::new("turn_profile_history").unwrap();
        let events = [
            HarnessEvent::RunStarted(RunStartedEvent {
                run_id: run_id.clone(),
                identity: RunIdentity::Profile {
                    profile_id: profile_id.clone(),
                    profile_revision: 1,
                },
            }),
            HarnessEvent::ContextBuilt {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                context: ContextPack {
                    fragments: vec![],
                    token_budget: 1,
                },
            },
            HarnessEvent::ModelRequested {
                run_id: run_id.clone(),
                turn_id: turn_id.clone(),
                step: 0,
                model: ModelName::new("test-model").unwrap(),
            },
            HarnessEvent::ModelResponded {
                run_id: run_id.clone(),
                turn_id,
                step: 0,
                output: Message {
                    role: MessageRole::Assistant,
                    content: answer.into(),
                },
                proposed_calls: vec![],
                served_model: None,
                usage: None,
            },
            HarnessEvent::RunFinished {
                run_id: run_id.clone(),
            },
        ];
        let mut ledger =
            SqliteLedger::open_or_create_default(&runtime.paths.default_ledger()).unwrap();
        ledger
            .begin_session_run(
                &thread_session_id(thread_id),
                &run_id,
                "profile question",
                true,
            )
            .unwrap();
        for (sequence, event) in events.into_iter().enumerate() {
            ledger
                .append(
                    run_id.as_str(),
                    &RecordedEvent {
                        seq: u64::try_from(sequence).unwrap(),
                        occurred_at_ms: u64::try_from(sequence).unwrap(),
                        event,
                    },
                )
                .unwrap();
        }
        ledger.finish_session_run(&run_id, answer).unwrap();
        run_id.to_string()
    }

    fn ok_result(output: LogicalReadToolOutput) -> LogicalReadResult {
        match output {
            LogicalReadToolOutput::Ok { result } => *result,
            output => panic!("expected logical read success, got {output:?}"),
        }
    }

    #[test]
    fn profile_read_pages_hash_verified_revisions_and_denies_cross_profile_targets() {
        let (_root, runtime, thread_id, profile_id) = profile_runtime();
        let mut store = runtime.paths.server_store().unwrap();
        for revision in 2..=3 {
            store
                .update_profile_content(
                    &profile_id,
                    "operator",
                    revision,
                    ProfileRevisionContent {
                        instructions_markdown: format!("instructions {revision}"),
                        memory_markdown: format!("memory {revision}"),
                        skill_refs: vec![format!("skill {revision}")],
                    },
                )
                .unwrap()
                .unwrap();
        }
        drop(store);

        let first = handle(
            &runtime,
            &profile_id,
            &thread_id,
            LogicalReadRequest::Profile(ProfileReadInput {
                profile_id: None,
                revision: Some(2),
                cursor: None,
                limit: Some(1),
            }),
        );
        let LogicalReadResult::Profile(first) = ok_result(first) else {
            panic!("expected profile read")
        };
        assert_eq!(first.current_revision, 3);
        assert_eq!(first.selected.metadata.revision, 2);
        assert_eq!(first.selected.metadata.parent_revision, Some(1));
        assert_eq!(first.revisions.len(), 1);
        assert!(first.truncated);
        assert_eq!(first.next_cursor.as_deref(), Some("1"));
        assert_eq!(
            first.selected.metadata.content_hash,
            ProfileRevisionContent {
                instructions_markdown: "instructions 2".into(),
                memory_markdown: "memory 2".into(),
                skill_refs: vec!["skill 2".into()],
            }
            .content_hash()
            .unwrap()
        );
        let second = handle(
            &runtime,
            &profile_id,
            &thread_id,
            LogicalReadRequest::Profile(ProfileReadInput {
                profile_id: None,
                revision: None,
                cursor: first.next_cursor,
                limit: Some(2),
            }),
        );
        let LogicalReadResult::Profile(second) = ok_result(second) else {
            panic!("expected second profile page")
        };
        assert_eq!(
            second
                .revisions
                .iter()
                .map(|revision| revision.revision)
                .collect::<Vec<_>>(),
            [2, 3]
        );
        assert!(!second.truncated);
        assert_eq!(second.next_cursor, None);

        assert!(matches!(
            handle(
                &runtime,
                &profile_id,
                &thread_id,
                LogicalReadRequest::Profile(ProfileReadInput {
                    profile_id: Some("profile-other".into()),
                    revision: None,
                    cursor: None,
                    limit: None,
                }),
            ),
            LogicalReadToolOutput::Error {
                code: LogicalReadErrorCode::CrossProfile,
                ..
            }
        ));
        assert!(matches!(
            handle(
                &runtime,
                &profile_id,
                &thread_id,
                LogicalReadRequest::ThreadEvents(ThreadHistoryInput {
                    thread_id: Some("thread-other-profile".into()),
                    run_id: None,
                    cursor: None,
                    limit: None,
                }),
            ),
            LogicalReadToolOutput::Error {
                code: LogicalReadErrorCode::MembershipDenied,
                ..
            }
        ));
    }

    #[test]
    fn tree_and_history_reads_are_paginated_byte_capped_and_truthful_when_unconfined() {
        let (_root, runtime, thread_id, profile_id) = profile_runtime();
        let tree = handle(
            &runtime,
            &profile_id,
            &thread_id,
            LogicalReadRequest::ThreadTree(ThreadTreeInput {
                profile_id: None,
                cursor: None,
                limit: Some(1),
            }),
        );
        let LogicalReadResult::ThreadTree(tree) = ok_result(tree) else {
            panic!("expected thread tree")
        };
        assert_eq!(tree.threads.len(), 1);
        assert_eq!(tree.threads[0].confinement, ThreadConfinement::None);
        assert_eq!(
            tree.threads[0].profile_filesystem_isolation,
            ProfileFilesystemIsolation::Unconfined
        );

        let run_id = write_profile_run(
            &runtime,
            &thread_id,
            &profile_id,
            &"large answer ".repeat(30_000),
        );
        let first = handle(
            &runtime,
            &profile_id,
            &thread_id,
            LogicalReadRequest::ThreadEvents(ThreadHistoryInput {
                thread_id: None,
                run_id: Some(run_id.clone()),
                cursor: None,
                limit: Some(2),
            }),
        );
        let LogicalReadResult::ThreadEvents(first) = ok_result(first) else {
            panic!("expected event page")
        };
        assert_eq!(first.entries.len(), 2);
        assert!(first.truncated);
        assert_eq!(first.next_cursor.as_deref(), Some("0:2"));
        let second = handle(
            &runtime,
            &profile_id,
            &thread_id,
            LogicalReadRequest::ThreadEvents(ThreadHistoryInput {
                thread_id: None,
                run_id: Some(run_id.clone()),
                cursor: first.next_cursor,
                limit: Some(2),
            }),
        );
        let LogicalReadResult::ThreadEvents(second) = ok_result(second) else {
            panic!("expected second event page")
        };
        assert!(matches!(
            second.entries.first(),
            Some(ProfileEventEntry::Event { record, .. }) if record.seq == 2
        ));
        assert!(matches!(
            handle(
                &runtime,
                &profile_id,
                &thread_id,
                LogicalReadRequest::ThreadEvents(ThreadHistoryInput {
                    thread_id: None,
                    run_id: Some(run_id.clone()),
                    cursor: None,
                    limit: Some(MAX_HISTORY_LIMIT + 1),
                }),
            ),
            LogicalReadToolOutput::Error {
                code: LogicalReadErrorCode::InvalidRequest,
                ..
            }
        ));

        for request in [
            LogicalReadRequest::ThreadEvents(ThreadHistoryInput {
                thread_id: None,
                run_id: Some(run_id.clone()),
                cursor: None,
                limit: Some(MAX_HISTORY_LIMIT),
            }),
            LogicalReadRequest::ThreadTranscript(ThreadHistoryInput {
                thread_id: None,
                run_id: Some(run_id),
                cursor: None,
                limit: Some(MAX_HISTORY_LIMIT),
            }),
        ] {
            let output = handle(&runtime, &profile_id, &thread_id, request);
            assert!(serde_json::to_vec(&output).unwrap().len() < MAX_LOGICAL_READ_SERIALIZED_BYTES);
            let omitted = match &output {
                LogicalReadToolOutput::Ok { result } => match result.as_ref() {
                    LogicalReadResult::ThreadEvents(result) => {
                        assert!(result.truncated);
                        result
                            .entries
                            .iter()
                            .any(|entry| matches!(entry, ProfileEventEntry::Omitted { .. }))
                    }
                    LogicalReadResult::ThreadTranscript(result) => {
                        assert!(result.truncated);
                        result
                            .entries
                            .iter()
                            .any(|entry| matches!(entry, ProfileTranscriptEntry::Omitted { .. }))
                    }
                    _ => false,
                },
                _ => false,
            };
            assert!(
                omitted,
                "oversize committed entry was not explicitly omitted"
            );
        }
    }

    #[test]
    fn profile_content_read_has_a_deterministic_token_bound() {
        let content = ProfileRevisionContent {
            instructions_markdown: "i".repeat(128 * 1024),
            memory_markdown: "m".repeat(128 * 1024),
            skill_refs: (0..64).map(|_| "s".repeat(8 * 1024)).collect(),
        };
        let first = bounded_profile_content(&content);
        let second = bounded_profile_content(&content);
        assert_eq!(first, second);
        assert!(first.truncated);
        let chars = first.instructions_markdown.chars().count()
            + first.memory_markdown.chars().count()
            + first
                .skill_refs
                .iter()
                .map(|value| value.chars().count())
                .sum::<usize>();
        assert!(chars <= 8_192 * 4);
    }

    #[test]
    fn logical_request_shape_is_typed_and_rejects_unknown_fields() {
        assert!(matches!(
            LogicalReadRequest::from_tool(PROFILE_READ, serde_json::json!({"limit": 1})).unwrap(),
            LogicalReadRequest::Profile(ProfileReadInput { limit: Some(1), .. })
        ));
        assert!(
            LogicalReadRequest::from_tool(
                PROFILE_READ,
                serde_json::json!({"limit": 1, "unknown": true})
            )
            .is_err()
        );
    }
}
