//! Synchronous daemon request client with bounded connection and request calls.

pub use crate::{ClientError, ClientResult};
use crate::{
    paths,
    transport::{self, Stream},
};
use platonic_protocol::{
    AgentCreateParams, AgentCreateResult, AgentId, AgentListParams, AgentListResult,
    AgentStatusParams, AgentStatusResult, ApprovalDecideParams, ApprovalDecision, ApprovalProfile,
    CommandAcceptedResult, DaemonStatusParams, DaemonStatusResult, Envelope, EnvelopeKind,
    EventsStreamParams, EventsStreamResult, HelloParams, HelloResult, IssuePrepStartParams,
    IssuePrepStartResult, MessageAppendParams, PROTOCOL_VERSION, ProtocolMethod, ProtocolRequest,
    ProtocolResponse, ReasoningEffort, RunCancelParams, RunOverrides, RunStartParams,
    RunStartResult, SessionApprovalProfileSetParams, SessionApprovalProfileSetResult,
    SessionSummary, SessionsListResult, ShutdownIfIdleResult, ThreadApprovalPolicy,
    ThreadAuthorityParams, ThreadAuthorityResult, ThreadEventsParams, ThreadEventsResult,
    ThreadListResult, ThreadSendParams, ThreadSendResult, ThreadSpawnDecision, ThreadSpawnParams,
    ThreadSpawnResult, ThreadStatusParams, ThreadStatusResult, ThreadStopParams, ThreadStopResult,
    TranscriptReadParams, TranscriptReadResult, WorkspaceCreateParams, WorkspaceCreateResult,
    WorkspaceListParams, WorkspaceListResult, WorkspaceStatusParams, WorkspaceStatusResult,
};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::{
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

/// Synchronous client for one Platonic server connection.
pub struct DaemonClient {
    reader: BufReader<Stream>,
    writer: Stream,
    next_id: u64,
    response_limit: Option<u64>,
    request_timeout: Option<Duration>,
}

#[cfg(windows)]
const CONTROL_RESPONSE_LIMIT: u64 = 64 * 1024;
impl DaemonClient {
    /// Connects to a daemon endpoint using the platform's ordinary connect bound.
    pub fn connect(socket_path: &Path) -> ClientResult<Self> {
        let writer = transport::connect(socket_path)?;
        Self::from_stream(writer, None, None)
    }

    /// Connects to a daemon endpoint within `timeout` and applies that bound to requests.
    pub fn connect_with_timeout(socket_path: &Path, timeout: Duration) -> ClientResult<Self> {
        let writer = transport::connect_with_timeout(socket_path, timeout)?;
        Self::from_stream(writer, None, Some(timeout))
    }

    #[cfg(unix)]
    /// Replaces the per-request timeout used by this client.
    pub fn set_timeout(&mut self, timeout: Duration) -> ClientResult<()> {
        self.request_timeout = Some(timeout);
        Ok(())
    }

    /// Clears the per-request timeout and returns the stream to blocking I/O.
    pub fn clear_request_timeout(&mut self) -> ClientResult<()> {
        transport::clear_deadline(self.reader.get_mut())?;
        transport::clear_deadline(&mut self.writer)?;
        self.request_timeout = None;
        Ok(())
    }

    #[cfg(windows)]
    /// Connects to the named-pipe server identified by lock metadata.
    pub fn connect_expected_server(socket_path: &Path, expected_pid: u32) -> ClientResult<Self> {
        let writer = transport::connect_expected_server(socket_path, expected_pid)?;
        Self::from_stream(writer, Some(CONTROL_RESPONSE_LIMIT), None)
    }

    fn from_stream(
        writer: Stream,
        response_limit: Option<u64>,
        request_timeout: Option<Duration>,
    ) -> ClientResult<Self> {
        let reader = BufReader::new(transport::try_clone(&writer)?);
        Ok(Self {
            reader,
            writer,
            next_id: 1,
            response_limit,
            request_timeout,
        })
    }

    /// Performs the daemon handshake for `workspace_root`.
    pub fn hello(&mut self, workspace_root: &Path) -> ClientResult<HelloResult> {
        let workspace_root = workspace_root.canonicalize()?;
        let workspace_id = paths::workspace_id(&workspace_root)?;
        self.request(ProtocolRequest::Hello(HelloParams {
            workspace_root: workspace_root.to_string_lossy().into_owned(),
            workspace_id,
        }))
    }

    /// Lists daemon sessions for the connected workspace.
    pub fn sessions_list(&mut self) -> ClientResult<Vec<SessionSummary>> {
        let result: SessionsListResult = self.request(ProtocolRequest::SessionsList)?;
        Ok(result.sessions)
    }

    /// Registers one named workspace directory with the server.
    pub fn workspace_create(
        &mut self,
        name: String,
        root: PathBuf,
    ) -> ClientResult<WorkspaceCreateResult> {
        let root = root.canonicalize()?;
        self.request(ProtocolRequest::WorkspaceCreate(WorkspaceCreateParams {
            name,
            root: root.to_string_lossy().into_owned(),
        }))
    }

    /// Lists every registered workspace, including broken entries.
    pub fn workspace_list(&mut self) -> ClientResult<WorkspaceListResult> {
        self.request(ProtocolRequest::WorkspaceList(
            WorkspaceListParams::default(),
        ))
    }

    /// Reads one registered workspace by its server-minted id.
    pub fn workspace_status(
        &mut self,
        workspace_id: String,
    ) -> ClientResult<WorkspaceStatusResult> {
        self.request(ProtocolRequest::WorkspaceStatus(WorkspaceStatusParams {
            workspace_id,
        }))
    }

    /// Creates one configured agent profile with a hard workspace binding.
    pub fn agent_create(
        &mut self,
        agent_id: AgentId,
        workspace_id: String,
        model: String,
        reasoning_effort: ReasoningEffort,
        approval_policy: ThreadApprovalPolicy,
        toolset: Vec<String>,
    ) -> ClientResult<AgentCreateResult> {
        self.request(ProtocolRequest::AgentCreate(AgentCreateParams {
            agent_id,
            workspace_id,
            model,
            reasoning_effort,
            approval_policy,
            toolset,
        }))
    }

    /// Lists every configured agent profile.
    pub fn agent_list(&mut self) -> ClientResult<AgentListResult> {
        self.request(ProtocolRequest::AgentList(AgentListParams::default()))
    }

    /// Reads one configured agent profile.
    pub fn agent_status(&mut self, agent_id: AgentId) -> ClientResult<AgentStatusResult> {
        self.request(ProtocolRequest::AgentStatus(AgentStatusParams { agent_id }))
    }

    /// Starts one typed thread spawn admission.
    pub fn thread_spawn_start(
        &mut self,
        parent_thread_id: Option<String>,
        cwd: String,
        model: String,
        reasoning_effort: platonic_protocol::ReasoningEffort,
        approval_policy: ThreadApprovalPolicy,
    ) -> ClientResult<ThreadSpawnResult> {
        self.thread_spawn_start_with_repositories(
            parent_thread_id,
            cwd,
            model,
            reasoning_effort,
            approval_policy,
            Vec::new(),
        )
    }

    /// Starts one typed thread spawn admission with explicit repository claims.
    pub fn thread_spawn_start_with_repositories(
        &mut self,
        parent_thread_id: Option<String>,
        cwd: String,
        model: String,
        reasoning_effort: platonic_protocol::ReasoningEffort,
        approval_policy: ThreadApprovalPolicy,
        repositories: Vec<platonic_protocol::ThreadRepositoryRequest>,
    ) -> ClientResult<ThreadSpawnResult> {
        self.request(ProtocolRequest::ThreadSpawn(ThreadSpawnParams::Start {
            parent_thread_id,
            cwd,
            model,
            reasoning_effort,
            approval_policy,
            repositories,
        }))
    }

    /// Resolves one pending typed thread spawn admission.
    pub fn thread_spawn_decide(
        &mut self,
        spawn_id: String,
        approval: ThreadSpawnDecision,
    ) -> ClientResult<ThreadSpawnResult> {
        self.request(ProtocolRequest::ThreadSpawn(ThreadSpawnParams::Decide {
            spawn_id,
            approval,
        }))
    }

    /// Lists every durable thread in the selected workspace authority ledger.
    pub fn thread_list(&mut self) -> ClientResult<ThreadListResult> {
        self.request(ProtocolRequest::ThreadList)
    }

    /// Reads one durable thread joined with current daemon state.
    pub fn thread_status(&mut self, thread_id: String) -> ClientResult<ThreadStatusResult> {
        self.request(ProtocolRequest::ThreadStatus(ThreadStatusParams {
            thread_id,
        }))
    }

    /// Reads one complete immutable twelve-field thread authority record.
    pub fn thread_authority(&mut self, thread_id: String) -> ClientResult<ThreadAuthorityResult> {
        self.request(ProtocolRequest::ThreadAuthority(ThreadAuthorityParams {
            thread_id,
        }))
    }

    /// Starts an idle thread turn or steers the exact active turn owned by `controller_id`.
    pub fn thread_send(
        &mut self,
        thread_id: String,
        controller_id: String,
        turn_id: Option<String>,
        message: String,
    ) -> ClientResult<ThreadSendResult> {
        self.request(ProtocolRequest::ThreadSend(ThreadSendParams {
            thread_id,
            controller_id,
            turn_id,
            message,
        }))
    }

    /// Reads one bounded retained event page for a live thread.
    pub fn thread_events(
        &mut self,
        thread_id: String,
        from_offset: Option<u64>,
        limit: usize,
        wait_ms: u64,
    ) -> ClientResult<ThreadEventsResult> {
        self.request(ProtocolRequest::ThreadEvents(ThreadEventsParams {
            thread_id,
            from_offset,
            limit: Some(limit),
            wait_ms: Some(wait_ms),
        }))
    }

    /// Stops one durable thread and records the requesting actor.
    pub fn thread_stop(
        &mut self,
        thread_id: String,
        actor: String,
    ) -> ClientResult<ThreadStopResult> {
        self.request(ProtocolRequest::ThreadStop(ThreadStopParams {
            thread_id,
            actor,
        }))
    }

    /// Reads authoritative daemon, model, session, usage, and trust status.
    pub fn daemon_status(
        &mut self,
        session_id: Option<String>,
        config_path: Option<String>,
    ) -> ClientResult<DaemonStatusResult> {
        self.request(ProtocolRequest::DaemonStatus(DaemonStatusParams {
            session_id,
            config_path,
        }))
    }

    /// Sets one daemon-lifetime approval profile for an existing session.
    pub fn session_approval_profile_set(
        &mut self,
        session_id: String,
        profile: ApprovalProfile,
    ) -> ClientResult<SessionApprovalProfileSetResult> {
        self.request(ProtocolRequest::SessionApprovalProfileSet(
            SessionApprovalProfileSetParams {
                session_id,
                profile,
            },
        ))
    }

    /// Requests daemon shutdown when no run or approval is active.
    pub fn shutdown_if_idle(&mut self) -> ClientResult<ShutdownIfIdleResult> {
        self.request(ProtocolRequest::DaemonShutdownIfIdle)
    }

    /// Reads the ledger-backed transcript containing `run_id`.
    pub fn transcript_read(&mut self, run_id: &str) -> ClientResult<TranscriptReadResult> {
        self.request(ProtocolRequest::TranscriptRead(TranscriptReadParams {
            run_id: Some(run_id.into()),
            session_id: None,
        }))
    }

    /// Reads the latest ledger-backed transcript for `session_id`.
    pub fn transcript_read_session(
        &mut self,
        session_id: &str,
    ) -> ClientResult<TranscriptReadResult> {
        self.request(ProtocolRequest::TranscriptRead(TranscriptReadParams {
            run_id: None,
            session_id: Some(session_id.into()),
        }))
    }

    /// Starts a new daemon run with default model overrides.
    pub fn run_start(
        &mut self,
        question: String,
        config_path: Option<String>,
        wait: bool,
    ) -> ClientResult<RunStartResult> {
        self.run_start_with_overrides(question, config_path, RunOverrides::default(), wait)
    }

    /// Starts a new daemon run with explicit model overrides.
    pub fn run_start_with_overrides(
        &mut self,
        question: String,
        config_path: Option<String>,
        overrides: RunOverrides,
        wait: bool,
    ) -> ClientResult<RunStartResult> {
        self.run_start_with_overrides_and_profile(question, config_path, overrides, None, wait)
    }

    /// Starts a new daemon run with explicit model overrides and approval profile.
    pub fn run_start_with_overrides_and_profile(
        &mut self,
        question: String,
        config_path: Option<String>,
        overrides: RunOverrides,
        approval_profile: Option<ApprovalProfile>,
        wait: bool,
    ) -> ClientResult<RunStartResult> {
        self.request(ProtocolRequest::RunStart(RunStartParams {
            question,
            config_path,
            overrides,
            approval_profile,
            wait: Some(wait),
        }))
    }

    /// Appends a message to the daemon's latest workspace session.
    pub fn message_append(
        &mut self,
        message: String,
        config_path: Option<String>,
        wait: bool,
    ) -> ClientResult<RunStartResult> {
        self.message_append_to_session(message, None, config_path, wait)
    }

    /// Appends a message to an optional session with default model overrides.
    pub fn message_append_to_session(
        &mut self,
        message: String,
        session_id: Option<String>,
        config_path: Option<String>,
        wait: bool,
    ) -> ClientResult<RunStartResult> {
        self.message_append_to_session_with_overrides(
            message,
            session_id,
            config_path,
            RunOverrides::default(),
            wait,
        )
    }

    /// Appends a message to an optional session with explicit model overrides.
    pub fn message_append_to_session_with_overrides(
        &mut self,
        message: String,
        session_id: Option<String>,
        config_path: Option<String>,
        overrides: RunOverrides,
        wait: bool,
    ) -> ClientResult<RunStartResult> {
        self.message_append_to_session_with_overrides_and_profile(
            message,
            session_id,
            config_path,
            overrides,
            None,
            wait,
        )
    }

    /// Appends a message with explicit model overrides and an optional profile replacement.
    pub fn message_append_to_session_with_overrides_and_profile(
        &mut self,
        message: String,
        session_id: Option<String>,
        config_path: Option<String>,
        overrides: RunOverrides,
        approval_profile: Option<ApprovalProfile>,
        wait: bool,
    ) -> ClientResult<RunStartResult> {
        self.request(ProtocolRequest::MessageAppend(MessageAppendParams {
            message,
            session_id,
            config_path,
            overrides,
            approval_profile,
            wait: Some(wait),
        }))
    }

    /// Runs the daemon's synchronous issue-preparation command.
    pub fn issue_prep_start(
        &mut self,
        input: String,
        config_path: Option<String>,
    ) -> ClientResult<IssuePrepStartResult> {
        self.request(ProtocolRequest::IssuePrepStart(IssuePrepStartParams {
            input,
            config_path,
        }))
    }

    /// Reads one buffered event page for `run_id`.
    pub fn events_stream(
        &mut self,
        run_id: &str,
        from_offset: Option<u64>,
        limit: usize,
    ) -> ClientResult<EventsStreamResult> {
        self.request(ProtocolRequest::EventsStream(EventsStreamParams {
            run_id: run_id.into(),
            from_offset,
            limit: Some(limit),
        }))
    }

    /// Grants a pending daemon approval request.
    pub fn approval_grant(
        &mut self,
        run_id: &str,
        tool_call_id: &str,
    ) -> ClientResult<CommandAcceptedResult> {
        self.request(ProtocolRequest::ApprovalDecide(ApprovalDecideParams {
            run_id: run_id.into(),
            tool_call_id: tool_call_id.into(),
            decision: ApprovalDecision::Grant,
            reason: None,
            actor: None,
        }))
    }

    /// Grants a pending daemon approval and attributes the decision to an actor.
    pub fn approval_grant_as(
        &mut self,
        run_id: &str,
        tool_call_id: &str,
        actor: String,
    ) -> ClientResult<CommandAcceptedResult> {
        self.request(ProtocolRequest::ApprovalDecide(ApprovalDecideParams {
            run_id: run_id.into(),
            tool_call_id: tool_call_id.into(),
            decision: ApprovalDecision::Grant,
            reason: None,
            actor: Some(actor),
        }))
    }

    /// Grants a pending `shell.exec` request and later calls in its daemon session.
    pub fn approval_grant_session(
        &mut self,
        run_id: &str,
        tool_call_id: &str,
    ) -> ClientResult<CommandAcceptedResult> {
        self.request(ProtocolRequest::ApprovalDecide(ApprovalDecideParams {
            run_id: run_id.into(),
            tool_call_id: tool_call_id.into(),
            decision: ApprovalDecision::GrantSession,
            reason: None,
            actor: None,
        }))
    }

    /// Denies a pending daemon approval request with a reason.
    pub fn approval_deny(
        &mut self,
        run_id: &str,
        tool_call_id: &str,
        reason: String,
    ) -> ClientResult<CommandAcceptedResult> {
        self.request(ProtocolRequest::ApprovalDecide(ApprovalDecideParams {
            run_id: run_id.into(),
            tool_call_id: tool_call_id.into(),
            decision: ApprovalDecision::Deny,
            reason: Some(reason),
            actor: None,
        }))
    }

    /// Denies a pending daemon approval and attributes the decision to an actor.
    pub fn approval_deny_as(
        &mut self,
        run_id: &str,
        tool_call_id: &str,
        actor: String,
        reason: String,
    ) -> ClientResult<CommandAcceptedResult> {
        self.request(ProtocolRequest::ApprovalDecide(ApprovalDecideParams {
            run_id: run_id.into(),
            tool_call_id: tool_call_id.into(),
            decision: ApprovalDecision::Deny,
            reason: Some(reason),
            actor: Some(actor),
        }))
    }

    /// Requests cancellation for an active daemon run.
    pub fn run_cancel(&mut self, run_id: &str) -> ClientResult<CommandAcceptedResult> {
        self.request(ProtocolRequest::RunCancel(RunCancelParams {
            run_id: run_id.into(),
        }))
    }

    fn request<T>(&mut self, request: ProtocolRequest) -> ClientResult<T>
    where
        T: DeserializeOwned,
    {
        let method = request.method();
        let id = self.next_request_id(method.as_str());
        let envelope = Envelope::request(Some(id.clone()), request);
        let mut request = serde_json::to_vec(&envelope)?;
        request.push(b'\n');
        let deadline = self.request_timeout.map(|timeout| Instant::now() + timeout);
        #[cfg(windows)]
        if deadline.is_none() && self.response_limit.is_some() {
            transport::reset_deadline(self.reader.get_mut());
            transport::reset_deadline(&mut self.writer);
        }
        if let Some(deadline) = deadline {
            transport::set_deadline(self.reader.get_mut(), deadline)?;
            transport::set_deadline(&mut self.writer, deadline)?;
            self.write_until_deadline(method.as_str(), &request, deadline)?;
        } else {
            self.writer.write_all(&request)?;
            self.writer.flush()?;
        }

        let mut line = Vec::new();
        if let Some(deadline) = deadline {
            let bytes_read = self.read_line_until_deadline(
                method.as_str(),
                self.response_limit,
                deadline,
                &mut line,
            )?;
            return self.decode_response(method, id, bytes_read, line);
        }
        let bytes_read = match self.response_limit {
            Some(limit) => {
                Self::read_limited_line(&mut self.reader, method.as_str(), limit, &mut line)?
            }
            None => self.reader.read_until(b'\n', &mut line)?,
        };
        self.decode_response(method, id, bytes_read, line)
    }

    fn decode_response<T>(
        &self,
        method: ProtocolMethod,
        id: String,
        bytes_read: usize,
        line: Vec<u8>,
    ) -> ClientResult<T>
    where
        T: DeserializeOwned,
    {
        if bytes_read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "daemon connection closed before response",
            )
            .into());
        }
        let response = serde_json::from_slice::<Envelope>(&line)?;
        if response.v != PROTOCOL_VERSION {
            return Err(ClientError::DaemonProtocol(format!(
                "unsupported response protocol version: {}",
                response.v
            )));
        }
        if response.id.as_deref() != Some(&id) {
            return Err(ClientError::DaemonProtocol(format!(
                "response id mismatch: expected {id}, got {:?}",
                response.id
            )));
        }
        if response.method != Some(method) {
            return Err(ClientError::DaemonProtocol(format!(
                "response method mismatch: expected {method}, got {:?}",
                response.method
            )));
        }
        match response.kind {
            EnvelopeKind::Response => {
                let result = response.result.ok_or_else(|| {
                    ClientError::DaemonProtocol(format!("{method} response missing result"))
                })?;
                Ok(serde_json::from_value(response_result_value(result)?)?)
            }
            EnvelopeKind::Error => {
                let error = response.error.ok_or_else(|| {
                    ClientError::DaemonProtocol(format!("{method} error missing payload"))
                })?;
                Err(ClientError::DaemonResponse(error))
            }
            other => Err(ClientError::DaemonProtocol(format!(
                "{method} returned unexpected envelope kind {other:?}"
            ))),
        }
    }

    fn read_limited_line(
        reader: &mut BufReader<Stream>,
        method: &str,
        limit: u64,
        line: &mut Vec<u8>,
    ) -> ClientResult<usize> {
        let mut reader = std::io::Read::take(reader, limit + 1);
        let bytes_read = reader.read_until(b'\n', line)?;
        if bytes_read as u64 > limit {
            return Err(ClientError::DaemonProtocol(format!(
                "{method} response exceeds {limit} bytes"
            )));
        }
        Ok(bytes_read)
    }

    fn write_until_deadline(
        &mut self,
        method: &str,
        mut request: &[u8],
        deadline: Instant,
    ) -> ClientResult<()> {
        while !request.is_empty() {
            Self::ensure_deadline(method, deadline)?;
            transport::set_deadline(&mut self.writer, deadline)?;
            let written = match self.writer.write(request) {
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                result => Self::deadline_io(method, result)?,
            };
            if written == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write daemon request",
                )
                .into());
            }
            request = &request[written..];
        }
        Self::ensure_deadline(method, deadline)?;
        transport::set_deadline(&mut self.writer, deadline)?;
        Self::deadline_io(method, self.writer.flush())?;
        Self::ensure_deadline(method, deadline)
    }

    fn read_line_until_deadline(
        &mut self,
        method: &str,
        limit: Option<u64>,
        deadline: Instant,
        line: &mut Vec<u8>,
    ) -> ClientResult<usize> {
        loop {
            Self::ensure_deadline(method, deadline)?;
            transport::set_deadline(self.reader.get_mut(), deadline)?;
            let available = Self::deadline_io(method, self.reader.fill_buf())?;
            Self::ensure_deadline(method, deadline)?;
            if available.is_empty() {
                return Ok(line.len());
            }
            let consumed = available
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(available.len(), |position| position + 1);
            let ended = available[consumed - 1] == b'\n';
            let retained = match limit {
                Some(limit) => consumed.min((limit + 1).saturating_sub(line.len() as u64) as usize),
                None => consumed,
            };
            line.extend_from_slice(&available[..retained]);
            self.reader.consume(consumed);
            if limit.is_some_and(|limit| line.len() as u64 > limit) {
                return Err(ClientError::DaemonProtocol(format!(
                    "{method} response exceeds {} bytes",
                    limit.expect("checked as present")
                )));
            }
            if ended {
                return Ok(line.len());
            }
        }
    }

    fn ensure_deadline(method: &str, deadline: Instant) -> ClientResult<()> {
        if Instant::now() >= deadline {
            Err(Self::request_timeout(method).into())
        } else {
            Ok(())
        }
    }

    fn deadline_io<T>(method: &str, result: std::io::Result<T>) -> ClientResult<T> {
        result.map_err(|error| {
            if matches!(
                error.kind(),
                std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
            ) {
                Self::request_timeout(method).into()
            } else {
                error.into()
            }
        })
    }

    fn request_timeout(method: &str) -> std::io::Error {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("{method} request timed out"),
        )
    }

    fn next_request_id(&mut self, method: &str) -> String {
        let id = format!("{}_{}", method.replace('.', "_"), self.next_id);
        self.next_id += 1;
        id
    }
}

fn response_result_value(response: ProtocolResponse) -> serde_json::Result<Value> {
    match response {
        ProtocolResponse::Hello(result) => serde_json::to_value(result),
        ProtocolResponse::RunStart(result) => serde_json::to_value(result),
        ProtocolResponse::MessageAppend(result) => serde_json::to_value(result),
        ProtocolResponse::IssuePrepStart(result) => serde_json::to_value(result),
        ProtocolResponse::EventsStream(result) => serde_json::to_value(result),
        ProtocolResponse::ApprovalDecide(result) => serde_json::to_value(result),
        ProtocolResponse::RunCancel(result) => serde_json::to_value(result),
        ProtocolResponse::SessionsList(result) => serde_json::to_value(result),
        ProtocolResponse::TranscriptRead(result) => serde_json::to_value(result),
        ProtocolResponse::DaemonStatus(result) => serde_json::to_value(result),
        ProtocolResponse::SessionApprovalProfileSet(result) => serde_json::to_value(result),
        ProtocolResponse::DaemonShutdownIfIdle(result) => serde_json::to_value(result),
        ProtocolResponse::ThreadSpawn(result) => serde_json::to_value(result),
        ProtocolResponse::ThreadList(result) => serde_json::to_value(result),
        ProtocolResponse::ThreadStatus(result) => serde_json::to_value(result),
        ProtocolResponse::ThreadAuthority(result) => serde_json::to_value(result),
        ProtocolResponse::ThreadSend(result) => serde_json::to_value(result),
        ProtocolResponse::ThreadEvents(result) => serde_json::to_value(result),
        ProtocolResponse::ThreadStop(result) => serde_json::to_value(result),
        ProtocolResponse::WorkspaceCreate(result) => serde_json::to_value(result),
        ProtocolResponse::WorkspaceList(result) => serde_json::to_value(result),
        ProtocolResponse::WorkspaceStatus(result) => serde_json::to_value(result),
        ProtocolResponse::AgentCreate(result) => serde_json::to_value(result),
        ProtocolResponse::AgentList(result) => serde_json::to_value(result),
        ProtocolResponse::AgentStatus(result) => serde_json::to_value(result),
    }
}

#[cfg(test)]
fn request_params_value(envelope: &Envelope) -> Option<Value> {
    let request = serde_json::to_value(envelope.params.as_ref()?).expect("request serializes");
    request.get("params").cloned()
}

/// Canonical workspace and endpoint used to establish daemon connections.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DaemonConnectionConfig {
    /// Canonical workspace root supplied during `hello`.
    pub workspace_root: PathBuf,
    /// Explicit test endpoint or the stable host endpoint.
    pub socket_path: PathBuf,
}

impl DaemonConnectionConfig {
    /// Resolves a canonical workspace root and optional test endpoint override.
    pub fn resolve(workspace_root: &Path, socket_path: Option<PathBuf>) -> ClientResult<Self> {
        let workspace_root = workspace_root.canonicalize()?;
        let socket_path = match socket_path {
            Some(socket_path) => socket_path,
            None => paths::host_socket_path()?,
        };
        Ok(Self {
            workspace_root,
            socket_path,
        })
    }
}

#[cfg(test)]
mod timeout_tests {
    use super::*;
    use crate::transport;
    use platonic_protocol::Envelope;
    use serde_json::json;
    use std::{
        io::{BufRead, BufReader, Write},
        path::PathBuf,
        sync::mpsc,
        thread,
        time::{Duration, Instant},
    };

    const REQUEST_TIMEOUT: Duration = Duration::from_millis(150);

    #[cfg(unix)]
    #[test]
    fn connection_config_defaults_every_workspace_to_the_host_endpoint() {
        let root = tempfile::tempdir().unwrap();
        let runtime = root.path().join("runtime");
        let first = root.path().join("first");
        let second = root.path().join("second");
        std::fs::create_dir(&runtime).unwrap();
        std::fs::create_dir(&first).unwrap();
        std::fs::create_dir(&second).unwrap();

        temp_env::with_var("XDG_RUNTIME_DIR", Some(runtime.as_os_str()), || {
            let first = DaemonConnectionConfig::resolve(&first, None).unwrap();
            let second = DaemonConnectionConfig::resolve(&second, None).unwrap();

            assert_eq!(first.socket_path, paths::host_socket_path().unwrap());
            assert_eq!(second.socket_path, first.socket_path);
            assert!(
                !first.socket_path.components().any(|component| {
                    component.as_os_str() == std::ffi::OsStr::new("workspaces")
                })
            );
        });
    }

    #[test]
    fn timed_client_stops_when_response_has_no_newline() {
        const TIMEOUT: Duration = Duration::from_millis(200);
        const PROGRESS_INTERVAL: Duration = Duration::from_millis(80);

        let endpoint = TestEndpoint::new("partial-response");
        let listener = transport::bind(&endpoint.path).unwrap();
        let server = thread::spawn(move || {
            let mut stream = transport::accept(&listener).unwrap();
            let mut reader = BufReader::new(transport::try_clone(&stream).unwrap());
            read_request(&mut reader);
            for byte in b"{\"partial" {
                if stream.write_all(&[*byte]).is_err() {
                    break;
                }
                if stream.flush().is_err() {
                    break;
                }
                thread::sleep(PROGRESS_INTERVAL);
            }
        });
        let mut client = DaemonClient::connect_with_timeout(&endpoint.path, TIMEOUT).unwrap();

        let started = Instant::now();
        let error = client.sessions_list().unwrap_err();
        let elapsed = started.elapsed();
        server.join().unwrap();

        assert_timed_out(error);
        assert!(
            elapsed < TIMEOUT + Duration::from_millis(75),
            "partial progress extended the request to {elapsed:?}"
        );
    }

    #[test]
    fn timed_client_gives_successive_requests_fresh_deadlines() {
        const TIMEOUT: Duration = Duration::from_millis(500);
        const RESPONSE_DELAY: Duration = Duration::from_millis(300);

        let endpoint = TestEndpoint::new("successive-requests");
        let listener = transport::bind(&endpoint.path).unwrap();
        let server = thread::spawn(move || {
            let mut stream = transport::accept(&listener).unwrap();
            let mut reader = BufReader::new(transport::try_clone(&stream).unwrap());
            for _ in 0..2 {
                let request = read_request(&mut reader);
                thread::sleep(RESPONSE_DELAY);
                write_sessions_response(&mut stream, request.id);
            }
        });
        let mut client = DaemonClient::connect_with_timeout(&endpoint.path, TIMEOUT).unwrap();

        let started = Instant::now();
        assert!(client.sessions_list().unwrap().is_empty());
        assert!(client.sessions_list().unwrap().is_empty());
        let elapsed = started.elapsed();
        server.join().unwrap();

        assert!(
            elapsed > TIMEOUT,
            "two delayed requests did not outlive one budget: {elapsed:?}"
        );
    }

    #[test]
    fn timed_client_can_clear_its_request_timeout() {
        const TIMEOUT: Duration = Duration::from_millis(100);
        const OUTER_WATCHDOG: Duration = Duration::from_secs(5);

        let endpoint = TestEndpoint::new("clear-request-timeout");
        let listener = transport::bind(&endpoint.path).unwrap();
        let (request_seen_sender, request_seen_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut stream = transport::accept(&listener).unwrap();
            let mut reader = BufReader::new(transport::try_clone(&stream).unwrap());
            let first = read_request(&mut reader);
            write_sessions_response(&mut stream, first.id);
            let second = read_request(&mut reader);
            request_seen_sender.send(()).unwrap();
            release_receiver.recv_timeout(OUTER_WATCHDOG).unwrap();
            write_sessions_response(&mut stream, second.id);
        });
        let mut client = DaemonClient::connect_with_timeout(&endpoint.path, TIMEOUT).unwrap();
        assert!(client.sessions_list().unwrap().is_empty());
        client.clear_request_timeout().unwrap();
        let (result_sender, result_receiver) = mpsc::channel();
        let request = thread::spawn(move || {
            result_sender.send(client.sessions_list()).unwrap();
        });

        request_seen_receiver.recv_timeout(OUTER_WATCHDOG).unwrap();
        let premature = result_receiver.recv_timeout(TIMEOUT + Duration::from_millis(50));
        release_sender.send(()).unwrap();
        let (crossed_timeout, result) = match premature {
            Err(mpsc::RecvTimeoutError::Timeout) => {
                (true, result_receiver.recv_timeout(OUTER_WATCHDOG).unwrap())
            }
            Ok(result) => (false, result),
            Err(error) => panic!("request result channel failed: {error}"),
        };
        request.join().unwrap();
        let server_result = server.join();

        assert!(
            crossed_timeout,
            "request completed before the cleared timeout elapsed"
        );
        assert!(result.unwrap().is_empty());
        server_result.unwrap();
    }

    #[test]
    fn timed_client_stops_when_request_write_stalls() {
        let endpoint = TestEndpoint::new("stalled-write");
        let listener = transport::bind(&endpoint.path).unwrap();
        let (release, released) = mpsc::channel();
        let server = thread::spawn(move || {
            let _stream = transport::accept(&listener).unwrap();
            released.recv_timeout(Duration::from_secs(5)).unwrap();
        });
        let mut client =
            DaemonClient::connect_with_timeout(&endpoint.path, REQUEST_TIMEOUT).unwrap();
        let question = "x".repeat(8 * 1024 * 1024);

        let started = Instant::now();
        let error = client.run_start(question, None, false).unwrap_err();
        let elapsed = started.elapsed();
        release.send(()).unwrap();
        server.join().unwrap();

        assert_timed_out(error);
        assert!(elapsed < Duration::from_secs(1), "request took {elapsed:?}");
    }

    fn read_request(reader: &mut BufReader<transport::Stream>) -> Envelope {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        serde_json::from_str(line.trim()).unwrap()
    }

    fn write_sessions_response(writer: &mut transport::Stream, id: Option<String>) {
        let response =
            Envelope::response(id, Some("sessions.list".into()), json!({"sessions": []}));
        serde_json::to_writer(writer.by_ref(), &response).unwrap();
        writer.write_all(b"\n").unwrap();
        writer.flush().unwrap();
    }

    fn assert_timed_out(error: ClientError) {
        match error {
            ClientError::Io(error) => {
                assert_eq!(error.kind(), std::io::ErrorKind::TimedOut, "{error}")
            }
            error => panic!("expected I/O timeout, got {error}"),
        }
    }

    struct TestEndpoint {
        path: PathBuf,
        _directory: Option<tempfile::TempDir>,
    }

    impl TestEndpoint {
        fn new(name: &str) -> Self {
            #[cfg(unix)]
            {
                let directory = tempfile::tempdir().unwrap();
                let path = directory.path().join(format!("{name}.sock"));
                Self {
                    path,
                    _directory: Some(directory),
                }
            }
            #[cfg(windows)]
            {
                Self {
                    path: PathBuf::from(format!(
                        r"\\.\pipe\plato-agent-client-{name}-{}",
                        std::process::id()
                    )),
                    _directory: None,
                }
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use platonic_protocol::{
        DaemonStatusProviderKind, ERROR_NOT_FOUND, ProtocolError, RunStateName, SessionSummary,
        ShutdownIfIdleResultName,
    };
    use serde_json::json;
    use std::{
        io::{BufRead, BufReader, Write},
        os::unix::net::{UnixListener, UnixStream},
        thread,
    };

    #[test]
    fn client_sends_hello_and_sessions_requests() {
        let workspace = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let workspace_id = paths::workspace_id(workspace.path()).unwrap();
        let workspace_root = workspace.path().canonicalize().unwrap();
        let expected_id = workspace_id.clone();
        let expected_root = workspace_root.to_string_lossy().into_owned();

        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);

            let hello = read_request(&mut reader);
            assert_eq!(hello.method.as_deref(), Some("hello"));
            assert_eq!(
                request_params_value(&hello).unwrap()["workspace_id"],
                expected_id
            );
            assert_eq!(
                request_params_value(&hello).unwrap()["workspace_root"],
                expected_root
            );
            write_response(
                &mut writer,
                hello.id,
                "hello",
                json!({
                    "daemon_version": "0.1.0",
                    "workspace_id": expected_id,
                    "ledger_path": "/tmp/agent.db",
                    "capabilities": ["hello", "sessions.list"]
                }),
            );

            let sessions = read_request(&mut reader);
            assert_eq!(sessions.method.as_deref(), Some("sessions.list"));
            write_response(
                &mut writer,
                sessions.id,
                "sessions.list",
                json!({
                    "sessions": [{
                        "session_id": "run_1",
                        "run_id": "run_1",
                        "status": "finished",
                        "latest_question": "hello",
                        "first_question": "hello",
                        "updated_at_ms": 123456,
                        "ledger_path": "/tmp/agent.db"
                    }]
                }),
            );
        });

        let mut client = DaemonClient::connect(&socket_path).unwrap();
        let hello = client.hello(&workspace_root).unwrap();
        let sessions = client.sessions_list().unwrap();
        handle.join().unwrap();

        assert_eq!(hello.workspace_id, workspace_id);
        assert_eq!(
            sessions,
            vec![SessionSummary {
                session_id: "run_1".into(),
                run_id: "run_1".into(),
                status: RunStateName::Finished,
                latest_question: "hello".into(),
                first_question: "hello".into(),
                updated_at_ms: 123_456,
                ledger_path: "/tmp/agent.db".into(),
            }]
        );
    }

    #[test]
    fn client_sends_all_six_typed_workspace_and_agent_requests() {
        let workspace = tempfile::tempdir().unwrap();
        let workspace_root = workspace.path().canonicalize().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let expected_root = workspace_root.to_string_lossy().into_owned();
        let server_root = expected_root.clone();
        let workspace_json = json!({
            "id": "ws-alpha",
            "name": "alpha",
            "root": server_root,
            "ledger_path": "/state/alpha.db",
            "created_at_ms": 41,
            "health": "present"
        });
        let agent_json = json!({
            "id": "builder",
            "workspace_id": "ws-alpha",
            "model": "gpt-5.6-sol",
            "reasoning_effort": "xhigh",
            "approval_policy": "prompt",
            "toolset": ["file.read", "file.write"],
            "created_at_ms": 42
        });
        let server_workspace = workspace_json.clone();
        let server_agent = agent_json.clone();
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);

            let request = read_request(&mut reader);
            assert_eq!(request.method.as_deref(), Some("workspace.create"));
            assert_eq!(
                request_params_value(&request),
                Some(json!({"name": "alpha", "root": expected_root}))
            );
            write_response(
                &mut writer,
                request.id,
                "workspace.create",
                json!({"workspace": server_workspace.clone()}),
            );

            let request = read_request(&mut reader);
            assert_eq!(request.method.as_deref(), Some("workspace.list"));
            assert_eq!(request_params_value(&request), Some(json!({})));
            write_response(
                &mut writer,
                request.id,
                "workspace.list",
                json!({"workspaces": [server_workspace.clone()]}),
            );

            let request = read_request(&mut reader);
            assert_eq!(request.method.as_deref(), Some("workspace.status"));
            assert_eq!(
                request_params_value(&request),
                Some(json!({"workspace_id": "ws-alpha"}))
            );
            write_response(
                &mut writer,
                request.id,
                "workspace.status",
                json!({"workspace": server_workspace}),
            );

            let request = read_request(&mut reader);
            assert_eq!(request.method.as_deref(), Some("agent.create"));
            assert_eq!(
                request_params_value(&request),
                Some(json!({
                    "agent_id": "builder",
                    "workspace_id": "ws-alpha",
                    "model": "gpt-5.6-sol",
                    "reasoning_effort": "xhigh",
                    "approval_policy": "prompt",
                    "toolset": ["file.read", "file.write"]
                }))
            );
            write_response(
                &mut writer,
                request.id,
                "agent.create",
                json!({"agent": server_agent.clone()}),
            );

            let request = read_request(&mut reader);
            assert_eq!(request.method.as_deref(), Some("agent.list"));
            assert_eq!(request_params_value(&request), Some(json!({})));
            write_response(
                &mut writer,
                request.id,
                "agent.list",
                json!({"agents": [server_agent.clone()]}),
            );

            let request = read_request(&mut reader);
            assert_eq!(request.method.as_deref(), Some("agent.status"));
            assert_eq!(
                request_params_value(&request),
                Some(json!({"agent_id": "builder"}))
            );
            write_response(
                &mut writer,
                request.id,
                "agent.status",
                json!({"agent": server_agent}),
            );
        });

        let mut client = DaemonClient::connect(&socket_path).unwrap();
        assert_eq!(
            serde_json::to_value(
                client
                    .workspace_create("alpha".into(), workspace_root)
                    .unwrap()
                    .workspace
            )
            .unwrap(),
            workspace_json
        );
        assert_eq!(client.workspace_list().unwrap().workspaces.len(), 1);
        assert_eq!(
            client
                .workspace_status("ws-alpha".into())
                .unwrap()
                .workspace
                .id,
            "ws-alpha"
        );
        assert_eq!(
            serde_json::to_value(
                client
                    .agent_create(
                        AgentId::new("builder").unwrap(),
                        "ws-alpha".into(),
                        "gpt-5.6-sol".into(),
                        ReasoningEffort::Xhigh,
                        ThreadApprovalPolicy::Prompt,
                        vec!["file.read".into(), "file.write".into()],
                    )
                    .unwrap()
                    .agent
            )
            .unwrap(),
            agent_json
        );
        assert_eq!(client.agent_list().unwrap().agents.len(), 1);
        assert_eq!(
            client
                .agent_status(AgentId::new("builder").unwrap())
                .unwrap()
                .agent
                .id
                .as_str(),
            "builder"
        );
        handle.join().unwrap();
    }

    #[test]
    fn client_sends_typed_thread_management_requests() {
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let authority = json!({
            "thread_id": "thread_1",
            "parent_thread_id": null,
            "spawning_actor": "stdin",
            "agent_id": "plato",
            "model": "gpt-5.6-sol",
            "reasoning_effort": "xhigh",
            "approval_policy": "prompt",
            "toolset": ["read_file"],
            "worktrees": [],
            "granted_paths": [{"path": "/tmp/work", "writable": false}],
            "network": false,
            "created_at_ms": 42
        });
        let legacy_authority = json!({
            "thread_id": "thread_1",
            "parent_thread_id": null,
            "spawning_actor": "stdin",
            "cwd": "/tmp/work",
            "model": "gpt-5.6-sol",
            "reasoning_effort": "xhigh",
            "approval_policy": "prompt",
            "created_at_ms": 42
        });
        let server_authority = authority.clone();
        let server_legacy_authority = legacy_authority.clone();
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);

            let start = read_request(&mut reader);
            assert_eq!(start.method.as_deref(), Some("thread.spawn"));
            assert_eq!(
                request_params_value(&start),
                Some(json!({
                    "action": "start",
                    "parent_thread_id": null,
                    "cwd": "/tmp/work",
                    "model": "gpt-5.6-sol",
                    "reasoning_effort": "xhigh",
                    "approval_policy": "prompt"
                }))
            );
            write_response(
                &mut writer,
                start.id,
                "thread.spawn",
                json!({
                    "status": "approval_required",
                    "spawn_id": "spawn_1",
                    "thread_id": "thread_1",
                    "effect": "workspace_write",
                    "reason": "thread.spawn requires approval"
                }),
            );

            let decide = read_request(&mut reader);
            assert_eq!(decide.method.as_deref(), Some("thread.spawn"));
            assert_eq!(
                request_params_value(&decide),
                Some(json!({
                    "action": "decide",
                    "spawn_id": "spawn_1",
                    "approval": {"decision": "grant", "actor": "stdin"}
                }))
            );
            write_response(
                &mut writer,
                decide.id,
                "thread.spawn",
                json!({
                    "status": "spawned",
                    "thread": {
                        "authority": server_legacy_authority.clone(),
                        "live": {"loaded": true, "current_turn_id": null, "last_activity_at_ms": 42}
                    }
                }),
            );

            let list = read_request(&mut reader);
            assert_eq!(list.method.as_deref(), Some("thread.list"));
            assert_eq!(request_params_value(&list), None);
            write_response(
                &mut writer,
                list.id,
                "thread.list",
                json!({
                    "threads": [{
                        "authority": server_legacy_authority.clone(),
                        "live": {"loaded": true, "current_turn_id": null}
                    }]
                }),
            );

            let status = read_request(&mut reader);
            assert_eq!(status.method.as_deref(), Some("thread.status"));
            assert_eq!(
                request_params_value(&status),
                Some(json!({"thread_id": "thread_1"}))
            );
            write_response(
                &mut writer,
                status.id,
                "thread.status",
                json!({
                    "thread": {
                        "authority": server_legacy_authority,
                        "live": {"loaded": true, "current_turn_id": null}
                    }
                }),
            );

            let authority = read_request(&mut reader);
            assert_eq!(authority.method.as_deref(), Some("thread.authority"));
            assert_eq!(
                request_params_value(&authority),
                Some(json!({"thread_id": "thread_1"}))
            );
            write_response(
                &mut writer,
                authority.id,
                "thread.authority",
                json!({"authority": server_authority}),
            );

            let start_turn = read_request(&mut reader);
            assert_eq!(start_turn.method.as_deref(), Some("thread.send"));
            assert_eq!(
                request_params_value(&start_turn),
                Some(json!({
                    "thread_id": "thread_1",
                    "controller_id": "terminal_a",
                    "message": "inspect it"
                }))
            );
            write_response(
                &mut writer,
                start_turn.id,
                "thread.send",
                json!({
                    "status": "started",
                    "thread_id": "thread_1",
                    "turn_id": "thread_turn_1"
                }),
            );

            let steer = read_request(&mut reader);
            assert_eq!(steer.method.as_deref(), Some("thread.send"));
            assert_eq!(
                request_params_value(&steer),
                Some(json!({
                    "thread_id": "thread_1",
                    "controller_id": "terminal_a",
                    "turn_id": "thread_turn_1",
                    "message": "also summarize"
                }))
            );
            write_response(
                &mut writer,
                steer.id,
                "thread.send",
                json!({
                    "status": "steered",
                    "thread_id": "thread_1",
                    "turn_id": "thread_turn_1"
                }),
            );

            let events = read_request(&mut reader);
            assert_eq!(events.method.as_deref(), Some("thread.events"));
            assert_eq!(
                request_params_value(&events),
                Some(json!({
                    "thread_id": "thread_1",
                    "from_offset": 0,
                    "limit": 128,
                    "wait_ms": 1000
                }))
            );
            write_response(
                &mut writer,
                events.id,
                "thread.events",
                json!({
                    "thread_id": "thread_1",
                    "from_offset": 0,
                    "next_offset": 0,
                    "current_turn_id": "thread_turn_1",
                    "events": []
                }),
            );

            let stop = read_request(&mut reader);
            assert_eq!(stop.method.as_deref(), Some("thread.stop"));
            assert_eq!(
                request_params_value(&stop),
                Some(json!({"thread_id": "thread_1", "actor": "stdin"}))
            );
            write_response(
                &mut writer,
                stop.id,
                "thread.stop",
                json!({
                    "status": "stopped",
                    "thread_id": "thread_1",
                    "stopped_turn_id": null,
                    "stopped_at_ms": 52
                }),
            );
        });

        let mut client = DaemonClient::connect(&socket_path).unwrap();
        assert!(matches!(
            client
                .thread_spawn_start(
                    None,
                    "/tmp/work".into(),
                    "gpt-5.6-sol".into(),
                    platonic_protocol::ReasoningEffort::Xhigh,
                    ThreadApprovalPolicy::Prompt,
                )
                .unwrap(),
            ThreadSpawnResult::ApprovalRequired { .. }
        ));
        let spawned = client
            .thread_spawn_decide(
                "spawn_1".into(),
                ThreadSpawnDecision::Grant {
                    actor: "stdin".into(),
                },
            )
            .unwrap();
        assert!(matches!(spawned, ThreadSpawnResult::Spawned { .. }));
        assert_eq!(client.thread_list().unwrap().threads.len(), 1);
        assert_eq!(
            serde_json::to_value(
                client
                    .thread_status("thread_1".into())
                    .unwrap()
                    .thread
                    .authority
            )
            .unwrap(),
            legacy_authority
        );
        assert_eq!(
            serde_json::to_value(
                client
                    .thread_authority("thread_1".into())
                    .unwrap()
                    .authority
            )
            .unwrap(),
            authority
        );
        let started = client
            .thread_send(
                "thread_1".into(),
                "terminal_a".into(),
                None,
                "inspect it".into(),
            )
            .unwrap();
        assert!(matches!(
            started,
            ThreadSendResult::Started { ref turn_id, .. } if turn_id == "thread_turn_1"
        ));
        let steered = client
            .thread_send(
                "thread_1".into(),
                "terminal_a".into(),
                Some("thread_turn_1".into()),
                "also summarize".into(),
            )
            .unwrap();
        assert!(matches!(
            steered,
            ThreadSendResult::Steered { ref turn_id, .. } if turn_id == "thread_turn_1"
        ));
        let events = client
            .thread_events("thread_1".into(), Some(0), 128, 1_000)
            .unwrap();
        assert_eq!(events.thread_id, "thread_1");
        assert_eq!(events.current_turn_id.as_deref(), Some("thread_turn_1"));
        assert_eq!(
            client
                .thread_stop("thread_1".into(), "stdin".into())
                .unwrap(),
            ThreadStopResult::Stopped {
                thread_id: "thread_1".into(),
                stopped_turn_id: None,
                stopped_at_ms: 52,
            }
        );
        handle.join().unwrap();
    }

    #[test]
    fn client_sends_typed_daemon_status_request_and_decodes_every_section() {
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);
            let request = read_request(&mut reader);
            assert_eq!(request.method.as_deref(), Some("daemon.status"));
            assert_eq!(
                request_params_value(&request),
                Some(json!({
                    "session_id": "session_1",
                    "config_path": "config/plato.toml"
                }))
            );
            write_response(
                &mut writer,
                request.id,
                "daemon.status",
                json!({
                    "model": {
                        "requested_alias": "~openai/gpt-latest",
                        "served_model": "openai/gpt-5.5-2026-08-01",
                        "provider_kind": "open_router",
                        "key_present": true
                    },
                    "daemon": {
                        "package_version": "0.1.0",
                        "build_commit": "0123456789abcdef0123456789abcdef01234567",
                        "build_date_utc": "2026-08-01",
                        "uptime_ms": 42,
                        "endpoint_path": "/tmp/agent.sock",
                        "workspace_id": "work-1234"
                    },
                    "session": {
                        "session_id": "session_1",
                        "latest_run_id": "run_2",
                        "human_turn_count": 2,
                        "ledger_path": "/tmp/agent.db",
                        "core_event_count": 17
                    },
                    "usage": {
                        "last_run": {
                            "input_tokens": 7,
                            "output_tokens": 3,
                            "unknown_response_count": 1
                        },
                        "session": {
                            "input_tokens": 17,
                            "output_tokens": 8,
                            "unknown_response_count": 2
                        }
                    },
                    "trust": {
                        "approval_granted_count": 2,
                        "approval_denied_count": 1,
                        "shell_session_grant": true
                    }
                }),
            );
        });

        let mut client = DaemonClient::connect(&socket_path).unwrap();
        let status = client
            .daemon_status(Some("session_1".into()), Some("config/plato.toml".into()))
            .unwrap();
        handle.join().unwrap();

        assert_eq!(
            status.model.provider_kind,
            DaemonStatusProviderKind::OpenRouter
        );
        assert_eq!(
            status.model.served_model.as_deref(),
            Some("openai/gpt-5.5-2026-08-01")
        );
        assert_eq!(status.daemon.uptime_ms, 42);
        assert_eq!(status.session.latest_run_id.as_deref(), Some("run_2"));
        assert_eq!(status.usage.last_run.unknown_response_count, 1);
        assert_eq!(status.usage.session.input_tokens, 17);
        assert_eq!(status.trust.approval_granted_count, 2);
        assert_eq!(status.trust.approval_denied_count, 1);
        assert!(status.trust.shell_session_grant);
    }

    #[test]
    fn client_omits_shutdown_params_and_decodes_both_outcomes() {
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);
            for outcome in ["shutdown", "refused_active"] {
                let request = read_request(&mut reader);
                assert_eq!(request.method.as_deref(), Some("daemon.shutdown_if_idle"));
                assert!(request_params_value(&request).is_none());
                write_response(
                    &mut writer,
                    request.id,
                    "daemon.shutdown_if_idle",
                    json!({"result": outcome}),
                );
            }
        });

        let mut client = DaemonClient::connect(&socket_path).unwrap();
        assert_eq!(
            client.shutdown_if_idle().unwrap().result,
            ShutdownIfIdleResultName::Shutdown
        );
        assert_eq!(
            client.shutdown_if_idle().unwrap().result,
            ShutdownIfIdleResultName::RefusedActive
        );
        handle.join().unwrap();
    }

    #[test]
    fn client_maps_protocol_errors() {
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);
            let request = read_request(&mut reader);
            let response = Envelope {
                v: PROTOCOL_VERSION,
                id: request.id,
                kind: EnvelopeKind::Error,
                method: Some("sessions.list".into()),
                params: None,
                result: None,
                error: Some(ProtocolError {
                    code: ERROR_NOT_FOUND,
                    message: "missing".into(),
                }),
            };
            serde_json::to_writer(&mut writer, &response).unwrap();
            writer.write_all(b"\n").unwrap();
        });

        let mut client = DaemonClient::connect(&socket_path).unwrap();
        let error = client.sessions_list().unwrap_err();
        handle.join().unwrap();

        assert!(matches!(
            error,
            ClientError::DaemonResponse(ProtocolError { code, message })
                if code == ERROR_NOT_FOUND && message == "missing"
        ));
    }

    #[test]
    fn client_maps_eof_before_response_to_unexpected_eof_io() {
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream);
            let request = read_request(&mut reader);
            assert_eq!(request.method.as_deref(), Some("sessions.list"));
        });

        let mut client = DaemonClient::connect(&socket_path).unwrap();
        let error = client.sessions_list().unwrap_err();
        handle.join().unwrap();

        assert!(matches!(
            error,
            ClientError::Io(error)
                if error.kind() == std::io::ErrorKind::UnexpectedEof
                    && error.to_string() == "daemon connection closed before response"
        ));
    }

    #[test]
    fn client_rejects_an_unsupported_response_protocol_version() {
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);
            let request = read_request(&mut reader);
            let mut response = Envelope::response(
                request.id,
                Some("sessions.list".into()),
                json!({"sessions": []}),
            );
            response.v = PROTOCOL_VERSION + 1;
            serde_json::to_writer(&mut writer, &response).unwrap();
            writer.write_all(b"\n").unwrap();
        });

        let mut client = DaemonClient::connect(&socket_path).unwrap();
        let error = client.sessions_list().unwrap_err();
        handle.join().unwrap();

        assert!(matches!(
            error,
            ClientError::DaemonProtocol(message)
                if message == "unsupported response protocol version: 2"
        ));
    }

    #[test]
    fn client_sends_run_start_and_events_stream_requests() {
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);

            let run_start = read_request(&mut reader);
            assert_eq!(run_start.method.as_deref(), Some("run.start"));
            assert_eq!(
                request_params_value(&run_start).unwrap()["question"],
                "summarize this"
            );
            assert_eq!(request_params_value(&run_start).unwrap()["wait"], false);
            write_response(
                &mut writer,
                run_start.id,
                "run.start",
                json!({
                    "run_id": "run_1",
                    "session_id": "run_1",
                    "ledger_path": "/tmp/agent.db",
                    "status": "running",
                    "final_answer": null
                }),
            );

            let events = read_request(&mut reader);
            assert_eq!(events.method.as_deref(), Some("events.stream"));
            assert_eq!(request_params_value(&events).unwrap()["run_id"], "run_1");
            assert_eq!(request_params_value(&events).unwrap()["from_offset"], 2);
            assert_eq!(request_params_value(&events).unwrap()["limit"], 16);
            write_response(
                &mut writer,
                events.id,
                "events.stream",
                json!({
                    "run_id": "run_1",
                    "from_offset": 2,
                    "next_offset": 3,
                    "status": "running",
                    "events": [{
                        "offset": 2,
                        "event": {"kind": "test"}
                    }]
                }),
            );

            let tail = read_request(&mut reader);
            assert_eq!(tail.method.as_deref(), Some("events.stream"));
            assert!(
                request_params_value(&tail)
                    .unwrap()
                    .get("from_offset")
                    .is_none()
            );
            write_response(
                &mut writer,
                tail.id,
                "events.stream",
                json!({
                    "run_id": "run_1",
                    "from_offset": 3,
                    "next_offset": 3,
                    "status": "finished",
                    "events": []
                }),
            );
        });

        let mut client = DaemonClient::connect(&socket_path).unwrap();
        let run = client
            .run_start("summarize this".into(), Some("plato.toml".into()), false)
            .unwrap();
        let events = client.events_stream(&run.run_id, Some(2), 16).unwrap();
        let tail = client.events_stream(&run.run_id, None, 16).unwrap();
        handle.join().unwrap();

        assert_eq!(run.run_id, "run_1");
        assert_eq!(events.next_offset, 3);
        assert_eq!(events.events.len(), 1);
        assert_eq!(tail.from_offset, 3);
        assert!(tail.events.is_empty());
    }

    #[test]
    fn client_sends_issue_prep_start_request() {
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);

            let request = read_request(&mut reader);
            assert_eq!(request.method.as_deref(), Some("issue-prep.start"));
            assert_eq!(
                request_params_value(&request),
                Some(json!({
                    "input": "rough issue",
                    "config_path": "plato.toml"
                }))
            );
            write_response(
                &mut writer,
                request.id,
                "issue-prep.start",
                json!({
                    "run_dir": "/work/.plato/issue-prep/run_1",
                    "outcome": {
                        "status": "candidate",
                        "markdown": "# Prepared issue"
                    }
                }),
            );
        });

        let mut client = DaemonClient::connect(&socket_path).unwrap();
        let result = client
            .issue_prep_start("rough issue".into(), Some("plato.toml".into()))
            .unwrap();
        handle.join().unwrap();

        assert_eq!(
            result,
            IssuePrepStartResult {
                run_dir: "/work/.plato/issue-prep/run_1".into(),
                outcome: platonic_protocol::IssuePrepResult::Candidate {
                    markdown: "# Prepared issue".into()
                }
            }
        );
    }

    #[test]
    fn client_sends_session_transcript_and_message_append_requests() {
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);

            let transcript = read_request(&mut reader);
            assert_eq!(transcript.method.as_deref(), Some("transcript.read"));
            assert!(request_params_value(&transcript).unwrap()["run_id"].is_null());
            assert_eq!(
                request_params_value(&transcript).unwrap()["session_id"],
                "session_1"
            );
            write_response(
                &mut writer,
                transcript.id,
                "transcript.read",
                json!({
                    "run_id": "run_1",
                    "status": "finished",
                    "final_answer": "hello",
                    "transcript": "[turn_1] user: hello"
                }),
            );

            let append = read_request(&mut reader);
            assert_eq!(append.method.as_deref(), Some("message.append"));
            assert_eq!(
                request_params_value(&append).unwrap()["message"],
                "follow up"
            );
            assert_eq!(
                request_params_value(&append).unwrap()["session_id"],
                "session_1"
            );
            assert_eq!(request_params_value(&append).unwrap()["wait"], false);
            write_response(
                &mut writer,
                append.id,
                "message.append",
                json!({
                    "run_id": "run_2",
                    "session_id": "session_1",
                    "ledger_path": "/tmp/agent.db",
                    "status": "running",
                    "final_answer": null
                }),
            );
        });

        let mut client = DaemonClient::connect(&socket_path).unwrap();
        let transcript = client.transcript_read_session("session_1").unwrap();
        let run = client
            .message_append_to_session(
                "follow up".into(),
                Some("session_1".into()),
                Some("plato.toml".into()),
                false,
            )
            .unwrap();
        handle.join().unwrap();

        assert_eq!(transcript.run_id, "run_1");
        assert_eq!(transcript.status, RunStateName::Finished);
        assert_eq!(transcript.final_answer.as_deref(), Some("hello"));
        assert_eq!(transcript.typed, None);
        assert_eq!(run.session_id, "session_1");
        assert_eq!(run.run_id, "run_2");
    }

    #[test]
    fn client_sends_approval_decisions_and_cancel_requests() {
        let socket_dir = tempfile::tempdir().unwrap();
        let socket_path = socket_dir.path().join("agent.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);

            let grant = read_request(&mut reader);
            assert_eq!(grant.method.as_deref(), Some("approval.decide"));
            let params = request_params_value(&grant).unwrap();
            assert_eq!(params["run_id"], "run_1");
            assert_eq!(params["tool_call_id"], "call_1");
            assert_eq!(params["decision"], "grant");
            assert!(params["reason"].is_null());
            assert!(params.get("actor").is_none());
            write_response(
                &mut writer,
                grant.id,
                "approval.decide",
                json!({"run_id": "run_1", "status": "running"}),
            );

            let grant_session = read_request(&mut reader);
            assert_eq!(grant_session.method.as_deref(), Some("approval.decide"));
            assert_eq!(
                request_params_value(&grant_session).unwrap()["run_id"],
                "run_session"
            );
            assert_eq!(
                request_params_value(&grant_session).unwrap()["tool_call_id"],
                "call_session"
            );
            assert_eq!(
                request_params_value(&grant_session).unwrap()["decision"],
                "grant_session"
            );
            assert!(request_params_value(&grant_session).unwrap()["reason"].is_null());
            assert!(
                request_params_value(&grant_session)
                    .unwrap()
                    .get("actor")
                    .is_none()
            );
            write_response(
                &mut writer,
                grant_session.id,
                "approval.decide",
                json!({"run_id": "run_session", "status": "running"}),
            );

            let deny = read_request(&mut reader);
            assert_eq!(deny.method.as_deref(), Some("approval.decide"));
            assert_eq!(request_params_value(&deny).unwrap()["run_id"], "run_2");
            assert_eq!(
                request_params_value(&deny).unwrap()["tool_call_id"],
                "call_2"
            );
            assert_eq!(request_params_value(&deny).unwrap()["decision"], "deny");
            assert_eq!(
                request_params_value(&deny).unwrap()["reason"],
                "denied by plato-tui"
            );
            assert!(request_params_value(&deny).unwrap().get("actor").is_none());
            write_response(
                &mut writer,
                deny.id,
                "approval.decide",
                json!({"run_id": "run_2", "status": "running"}),
            );

            let cancel = read_request(&mut reader);
            assert_eq!(cancel.method.as_deref(), Some("run.cancel"));
            assert_eq!(request_params_value(&cancel).unwrap()["run_id"], "run_3");
            write_response(
                &mut writer,
                cancel.id,
                "run.cancel",
                json!({"run_id": "run_3", "status": "cancel_requested"}),
            );
        });

        let mut client = DaemonClient::connect(&socket_path).unwrap();
        let granted = client.approval_grant("run_1", "call_1").unwrap();
        let session_granted = client
            .approval_grant_session("run_session", "call_session")
            .unwrap();
        let denied = client
            .approval_deny("run_2", "call_2", "denied by plato-tui".into())
            .unwrap();
        let canceled = client.run_cancel("run_3").unwrap();
        handle.join().unwrap();

        assert_eq!(granted.status, RunStateName::Running);
        assert_eq!(session_granted.status, RunStateName::Running);
        assert_eq!(denied.status, RunStateName::Running);
        assert_eq!(canceled.status, RunStateName::CancelRequested);
    }

    fn read_request(reader: &mut BufReader<UnixStream>) -> Envelope {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        serde_json::from_str(line.trim()).unwrap()
    }

    fn write_response(writer: &mut UnixStream, id: Option<String>, method: &str, result: Value) {
        let response = Envelope::response(id, Some(method.into()), result);
        serde_json::to_writer(writer.by_ref(), &response).unwrap();
        writer.write_all(b"\n").unwrap();
        writer.flush().unwrap();
    }
}
