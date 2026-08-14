use crate::{
    AppError, AppResult,
    config::ComputerConfig,
    tool_catalog::{COMPUTER_OBSERVE, COMPUTER_WINDOWS},
};
use platonic_core::{PolicyDecision, ResultVisibility, ToolCall, ToolCallId, ToolResult};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    env,
    ffi::{OsStr, OsString},
    fs::File,
    io::{self, BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStderr, ChildStdout, Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[cfg(any(target_os = "linux", test))]
use std::fs;
#[cfg(target_os = "linux")]
use std::os::unix::fs::MetadataExt;
#[cfg(unix)]
use std::os::unix::{ffi::OsStrExt, fs::PermissionsExt, process::CommandExt};

const DRIVER_VERSION: &str = "0.19.3";
const DRIVER_VERSION_OUTPUT: &str = "cua-driver 0.19.3";
const MCP_PROTOCOL_VERSION: &str = "2025-06-18";
const MAX_WINDOWS: usize = 200;
const DEFAULT_MAX_ELEMENTS: usize = 100;
const MAX_ELEMENTS: usize = 200;
const MAX_DEPTH: u64 = 32;
const MAX_STRING_BYTES: usize = 1024;
const MAX_MCP_FRAME_BYTES: usize = 1024 * 1024;
const MAX_STDERR_BYTES: usize = 16 * 1024;
const MAX_RESULT_BYTES: usize = 64 * 1024 - 256;
const VERSION_TIMEOUT: Duration = Duration::from_secs(3);
const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(8);
#[cfg(not(test))]
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
#[cfg(test)]
const REQUEST_TIMEOUT: Duration = Duration::from_millis(500);
#[cfg(not(test))]
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(3);
#[cfg(test)]
const CLEANUP_TIMEOUT: Duration = Duration::from_millis(250);
const LEASE_IDLE: Duration = Duration::from_secs(60);
const LEASE_ABSOLUTE: Duration = Duration::from_secs(5 * 60);
const DISPLAY_ENV: &[&str] = &[
    "DISPLAY",
    "XAUTHORITY",
    "DBUS_SESSION_BUS_ADDRESS",
    "AT_SPI_BUS_ADDRESS",
    "XDG_RUNTIME_DIR",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ComputerError {
    Disabled,
    UnsupportedPlatform,
    UnsupportedDisplayServer,
    DriverMissing,
    DriverVersionMismatch,
    DriverStartFailed,
    PermissionDenied,
    NoDisplay,
    Protocol,
    OutputTooLarge,
    UnexpectedImage,
    StaleWindowRef,
    Timeout,
    Canceled,
    CleanupFailed,
}

impl ComputerError {
    fn code(self) -> &'static str {
        match self {
            Self::Disabled => "computer_disabled",
            Self::UnsupportedPlatform => "computer_unsupported_platform",
            Self::UnsupportedDisplayServer => "computer_unsupported_display_server",
            Self::DriverMissing => "computer_driver_missing",
            Self::DriverVersionMismatch => "computer_driver_version_mismatch",
            Self::DriverStartFailed => "computer_driver_start_failed",
            Self::PermissionDenied => "computer_permission_denied",
            Self::NoDisplay => "computer_no_display",
            Self::Protocol => "computer_protocol_error",
            Self::OutputTooLarge => "computer_output_too_large",
            Self::UnexpectedImage => "computer_unexpected_image",
            Self::StaleWindowRef => "stale_window_ref",
            Self::Timeout => "computer_timeout",
            Self::Canceled => "computer_canceled",
            Self::CleanupFailed => "computer_cleanup_failed",
        }
    }
}

fn tool_error(error: ComputerError) -> AppError {
    AppError::Tool(error.code().into())
}

#[derive(Clone, Debug)]
struct DriverEvidence {
    version: String,
    executable_sha256: String,
    executable_path_fingerprint: String,
    display_kind: String,
}

impl DriverEvidence {
    fn to_json(&self) -> Value {
        json!({
            "driver_version": self.version,
            "executable_sha256": self.executable_sha256,
            "executable_path_fingerprint": self.executable_path_fingerprint,
            "display_kind": self.display_kind,
        })
    }
}

#[derive(Clone, Debug)]
struct DisplaySession {
    kind: String,
    identity: String,
    env: Vec<(OsString, OsString)>,
}

impl DisplaySession {
    fn current() -> Result<Self, ComputerError> {
        if !cfg!(target_os = "linux") {
            return Err(ComputerError::UnsupportedPlatform);
        }
        Self::from_lookup(|name| env::var_os(name))
    }

    fn from_lookup(
        mut lookup: impl FnMut(&str) -> Option<OsString>,
    ) -> Result<Self, ComputerError> {
        let display = lookup("DISPLAY").filter(|value| !value.is_empty());
        let wayland = lookup("WAYLAND_DISPLAY").filter(|value| !value.is_empty());
        let session_type = lookup("XDG_SESSION_TYPE")
            .and_then(|value| value.into_string().ok())
            .unwrap_or_default();
        let Some(display) = display else {
            return if wayland.is_some() || session_type.eq_ignore_ascii_case("wayland") {
                Err(ComputerError::UnsupportedDisplayServer)
            } else {
                Err(ComputerError::NoDisplay)
            };
        };

        let kind = if wayland.is_some() || session_type.eq_ignore_ascii_case("wayland") {
            "xwayland"
        } else {
            "x11"
        };
        let mut child_env = vec![(OsString::from("DISPLAY"), display)];
        for name in DISPLAY_ENV
            .iter()
            .copied()
            .filter(|name| *name != "DISPLAY")
        {
            if let Some(value) = lookup(name).filter(|value| !value.is_empty()) {
                child_env.push((OsString::from(name), value));
            }
        }
        child_env.sort_by(|left, right| left.0.cmp(&right.0));
        let mut digest = Sha256::new();
        digest.update(b"platonic-computer-display-v1\0");
        digest.update(kind.as_bytes());
        digest.update([0]);
        for (name, value) in &child_env {
            update_os_digest(&mut digest, name);
            digest.update([0]);
            update_os_digest(&mut digest, value);
            digest.update([0]);
        }

        Ok(Self {
            kind: kind.into(),
            identity: hex_digest(digest.finalize()),
            env: child_env,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Bounds {
    x: i64,
    y: i64,
    width: u64,
    height: u64,
}

impl Bounds {
    fn to_json(&self) -> Value {
        json!({
            "x": self.x,
            "y": self.y,
            "width": self.width,
            "height": self.height,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RawWindow {
    pid: u32,
    window_id: u64,
    app: String,
    title: String,
    bounds: Bounds,
    z_index: Option<i64>,
}

#[derive(Clone, Debug)]
struct WindowBinding {
    generation: u64,
    display_identity: String,
    process_start: String,
    executable_identity: String,
    window: RawWindow,
}

#[derive(Clone, Debug)]
struct ObserveLease {
    granted_at: Instant,
    last_used: Instant,
}

impl ObserveLease {
    fn active(&self, now: Instant) -> bool {
        now.duration_since(self.last_used) <= LEASE_IDLE
            && now.duration_since(self.granted_at) <= LEASE_ABSOLUTE
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WindowsInput {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObserveInput {
    window_ref: String,
    max_elements: Option<usize>,
}

pub(crate) struct ComputerToolHandler {
    executable: PathBuf,
    display: DisplaySession,
    evidence: DriverEvidence,
    cancel: Option<Arc<AtomicBool>>,
    seed: [u8; 32],
    token_counter: u64,
    generation: u64,
    child: Option<McpChild>,
    registry: HashMap<String, WindowBinding>,
    leases: HashMap<String, ObserveLease>,
    cleaned: bool,
}

impl std::fmt::Debug for ComputerToolHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ComputerToolHandler")
            .field("display_kind", &self.display.kind)
            .field("generation", &self.generation)
            .field("registry_size", &self.registry.len())
            .finish_non_exhaustive()
    }
}

impl ComputerToolHandler {
    pub(crate) fn new(config: &ComputerConfig, cancel: Option<Arc<AtomicBool>>) -> AppResult<Self> {
        let display = DisplaySession::current().map_err(tool_error)?;
        Self::build(config, cancel, display).map_err(tool_error)
    }

    fn build(
        config: &ComputerConfig,
        cancel: Option<Arc<AtomicBool>>,
        display: DisplaySession,
    ) -> Result<Self, ComputerError> {
        check_cancel_flag(cancel.as_deref())?;
        let executable = resolve_executable(config.executable.as_deref())?;
        let (version, executable_sha256, executable_path_fingerprint) =
            inspect_driver(&executable, cancel.as_deref())?;
        let seed = random_seed()?;
        Ok(Self {
            evidence: DriverEvidence {
                version,
                executable_sha256,
                executable_path_fingerprint,
                display_kind: display.kind.clone(),
            },
            executable,
            display,
            cancel,
            seed,
            token_counter: 0,
            generation: 0,
            child: None,
            registry: HashMap::new(),
            leases: HashMap::new(),
            cleaned: false,
        })
    }

    pub(crate) fn policy_decision(&mut self, call: &ToolCall) -> PolicyDecision {
        if call.tool.as_str() == COMPUTER_OBSERVE
            && let Ok(input) = parse_observe_input(call.input.clone())
        {
            let now = Instant::now();
            if self
                .leases
                .get(&input.window_ref)
                .is_some_and(|lease| lease.active(now))
                && self.registry.contains_key(&input.window_ref)
            {
                return PolicyDecision::Allow;
            }
            self.leases.remove(&input.window_ref);
        }
        PolicyDecision::RequireApproval {
            reason: format!("{} requires explicit local approval", call.tool),
        }
    }

    pub(crate) fn approval_granted(&mut self, call: &ToolCall) -> AppResult<()> {
        if call.tool.as_str() != COMPUTER_OBSERVE {
            return Ok(());
        }
        let input = parse_observe_input(call.input.clone()).map_err(tool_error)?;
        if !self.registry.contains_key(&input.window_ref) {
            return Err(tool_error(ComputerError::StaleWindowRef));
        }
        let now = Instant::now();
        self.leases.insert(
            input.window_ref,
            ObserveLease {
                granted_at: now,
                last_used: now,
            },
        );
        Ok(())
    }

    pub(crate) fn approval_denied(&mut self) {
        self.leases.clear();
    }

    pub(crate) fn approval_preview(&self, tool: &str, input: &Value) -> AppResult<String> {
        match tool {
            COMPUTER_WINDOWS => {
                serde_json::from_value::<WindowsInput>(input.clone())?;
                Ok("app: all visible applications\ntitle: all visible window titles\nwindow_ref: host-minted after approval".into())
            }
            COMPUTER_OBSERVE => {
                let input = parse_observe_input(input.clone()).map_err(tool_error)?;
                let binding = self
                    .registry
                    .get(&input.window_ref)
                    .ok_or_else(|| tool_error(ComputerError::StaleWindowRef))?;
                Ok(format!(
                    "app: {}\ntitle: {}\nwindow_ref: {}",
                    binding.window.app, binding.window.title, input.window_ref
                ))
            }
            _ => Err(tool_error(ComputerError::Disabled)),
        }
    }

    pub(crate) fn execute(
        &mut self,
        call_id: ToolCallId,
        tool: &str,
        input: Value,
    ) -> AppResult<ToolResult> {
        self.cleaned = false;
        let started = Instant::now();
        let observe_ref = (tool == COMPUTER_OBSERVE)
            .then(|| {
                input
                    .get("window_ref")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .flatten();
        let result = match tool {
            COMPUTER_WINDOWS => {
                serde_json::from_value::<WindowsInput>(input)?;
                self.windows(call_id, started)
            }
            COMPUTER_OBSERVE => {
                let input = parse_observe_input(input).map_err(tool_error)?;
                self.observe(call_id, input, started)
            }
            _ => Err(ComputerError::Disabled),
        };
        if let Err(error) = result.as_ref() {
            if let Some(window_ref) = observe_ref {
                self.leases.remove(&window_ref);
            }
            if matches!(
                error,
                ComputerError::Canceled
                    | ComputerError::Timeout
                    | ComputerError::Protocol
                    | ComputerError::OutputTooLarge
                    | ComputerError::UnexpectedImage
            ) {
                let _ = self.stop_child(true);
            }
        }
        result.map_err(tool_error)
    }

    pub(crate) fn cleanup(&mut self) -> AppResult<()> {
        self.leases.clear();
        self.registry.clear();
        if self.cleaned {
            return Ok(());
        }
        self.cleaned = true;
        self.stop_child(false).map_err(tool_error)
    }

    fn windows(
        &mut self,
        call_id: ToolCallId,
        started: Instant,
    ) -> Result<ToolResult, ComputerError> {
        check_cancel_flag(self.cancel.as_deref())?;
        let mut windows = self.list_windows()?;
        windows.sort_by(|left, right| {
            right
                .z_index
                .cmp(&left.z_index)
                .then_with(|| left.app.cmp(&right.app))
                .then_with(|| left.title.cmp(&right.title))
                .then_with(|| left.pid.cmp(&right.pid))
                .then_with(|| left.window_id.cmp(&right.window_id))
        });
        let focused_z = unique_frontmost_z(&windows);
        self.registry.clear();
        self.leases.clear();

        let total_visible = windows.len();
        let mut visible = Vec::new();
        for window in windows.into_iter().take(MAX_WINDOWS) {
            let Some((process_start, executable_identity)) = process_identity(window.pid) else {
                continue;
            };
            let window_ref = self.mint_ref(&window);
            let focused = focused_z.is_some_and(|z| window.z_index == Some(z));
            visible.push(json!({
                "window_ref": window_ref,
                "app": window.app,
                "title": window.title,
                "bounds": window.bounds.to_json(),
                "focused": focused,
            }));
            self.registry.insert(
                window_ref,
                WindowBinding {
                    generation: self.generation,
                    display_identity: self.display.identity.clone(),
                    process_start,
                    executable_identity,
                    window,
                },
            );
        }

        let mut data = windows_result(&visible, total_visible, &self.evidence, started.elapsed());
        while serialized_len(&data)? > MAX_RESULT_BYTES {
            let Some(removed) = visible.pop() else {
                return Err(ComputerError::OutputTooLarge);
            };
            if let Some(window_ref) = removed.get("window_ref").and_then(Value::as_str) {
                self.registry.remove(window_ref);
            }
            data = windows_result(&visible, total_visible, &self.evidence, started.elapsed());
        }
        check_cancel_flag(self.cancel.as_deref())?;
        Ok(ToolResult {
            call_id,
            summary: format!(
                "listed {} of {} visible windows",
                visible.len(),
                total_visible
            ),
            data,
            artifacts: vec![],
            visibility: ResultVisibility::Both,
        })
    }

    fn observe(
        &mut self,
        call_id: ToolCallId,
        input: ObserveInput,
        started: Instant,
    ) -> Result<ToolResult, ComputerError> {
        check_cancel_flag(self.cancel.as_deref())?;
        let binding = self
            .registry
            .get(&input.window_ref)
            .cloned()
            .ok_or(ComputerError::StaleWindowRef)?;
        if binding.generation != self.generation
            || binding.display_identity != self.display.identity
            || self.child.is_none()
        {
            self.revoke_ref(&input.window_ref);
            return Err(ComputerError::StaleWindowRef);
        }

        let current_display = DisplaySession::current()?;
        if current_display.identity != binding.display_identity {
            self.registry.clear();
            self.leases.clear();
            return Err(ComputerError::StaleWindowRef);
        }
        let matches = self
            .list_windows()?
            .into_iter()
            .filter(|window| {
                window.pid == binding.window.pid && window.window_id == binding.window.window_id
            })
            .collect::<Vec<_>>();
        let identity = process_identity(binding.window.pid);
        if matches.len() != 1
            || matches[0] != binding.window
            || identity.as_ref().map(|identity| &identity.0) != Some(&binding.process_start)
            || identity.as_ref().map(|identity| &identity.1) != Some(&binding.executable_identity)
        {
            self.revoke_ref(&input.window_ref);
            return Err(ComputerError::StaleWindowRef);
        }

        let max_elements = input.max_elements.unwrap_or(DEFAULT_MAX_ELEMENTS);
        let response = self.call_tool(
            "get_window_state",
            json!({
                "pid": binding.window.pid,
                "window_id": binding.window.window_id,
                "include_screenshot": false,
                "max_elements": max_elements,
                "max_depth": MAX_DEPTH,
            }),
        )?;
        let mut elements = match normalize_observation(
            response,
            max_elements,
            binding.window.pid,
            binding.window.window_id,
        ) {
            Ok(elements) => elements,
            Err(error) => {
                let _ = self.stop_child(true);
                return Err(error);
            }
        };
        let available = elements.available;
        let mut data = observation_result(
            &input.window_ref,
            &elements.values,
            available,
            &self.evidence,
            started.elapsed(),
        );
        while serialized_len(&data)? > MAX_RESULT_BYTES {
            if elements.values.pop().is_none() {
                self.revoke_ref(&input.window_ref);
                return Err(ComputerError::OutputTooLarge);
            }
            data = observation_result(
                &input.window_ref,
                &elements.values,
                available,
                &self.evidence,
                started.elapsed(),
            );
        }
        check_cancel_flag(self.cancel.as_deref())?;
        if let Some(lease) = self.leases.get_mut(&input.window_ref) {
            lease.last_used = Instant::now();
        }
        Ok(ToolResult {
            call_id,
            summary: format!(
                "observed {} of {} semantic elements",
                elements.values.len(),
                available
            ),
            data,
            artifacts: vec![],
            visibility: ResultVisibility::Both,
        })
    }

    fn list_windows(&mut self) -> Result<Vec<RawWindow>, ComputerError> {
        let response = self.call_tool("list_windows", json!({"on_screen_only": true}))?;
        match parse_windows(response) {
            Ok(windows) => Ok(windows),
            Err(error) => {
                let _ = self.stop_child(true);
                Err(error)
            }
        }
    }

    fn call_tool(&mut self, name: &'static str, arguments: Value) -> Result<Value, ComputerError> {
        if !matches!(name, "list_windows" | "get_window_state") {
            return Err(ComputerError::Protocol);
        }
        self.ensure_child()?;
        check_cancel_flag(self.cancel.as_deref())?;
        let request = json!({
            "jsonrpc": "2.0",
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        });
        let response = self.request(request, REQUEST_TIMEOUT);
        match response {
            Ok(response) => parse_tool_result(response),
            Err(error) => {
                if !matches!(error, ComputerError::PermissionDenied) {
                    let _ = self.stop_child(true);
                }
                Err(error)
            }
        }
    }

    fn ensure_child(&mut self) -> Result<(), ComputerError> {
        if self.child.is_some() {
            return Ok(());
        }
        check_cancel_flag(self.cancel.as_deref())?;
        if sha256_file(&self.executable, self.cancel.as_deref())? != self.evidence.executable_sha256
        {
            return Err(ComputerError::DriverVersionMismatch);
        }
        self.generation = self.generation.wrapping_add(1).max(1);
        self.registry.clear();
        self.leases.clear();
        let mut child = McpChild::spawn(&self.executable, &self.display.env)?;
        let initialize = json!({
            "jsonrpc": "2.0",
            "method": "initialize",
            "params": {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "platonic", "version": env!("CARGO_PKG_VERSION")},
            },
        });
        let response = child.request(initialize, INITIALIZE_TIMEOUT, self.cancel.as_deref())?;
        validate_initialize(response)?;
        child.notify(json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {},
        }))?;
        check_cancel_flag(self.cancel.as_deref())?;
        self.child = Some(child);
        Ok(())
    }

    fn request(&mut self, request: Value, timeout: Duration) -> Result<Value, ComputerError> {
        self.child
            .as_mut()
            .ok_or(ComputerError::DriverStartFailed)?
            .request(request, timeout, self.cancel.as_deref())
    }

    fn stop_child(&mut self, force: bool) -> Result<(), ComputerError> {
        self.registry.clear();
        self.leases.clear();
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        child.stop(force, self.cancel.as_deref())
    }

    fn revoke_ref(&mut self, window_ref: &str) {
        self.registry.remove(window_ref);
        self.leases.remove(window_ref);
    }

    fn mint_ref(&mut self, window: &RawWindow) -> String {
        self.token_counter = self.token_counter.wrapping_add(1);
        let mut digest = Sha256::new();
        digest.update(b"platonic-window-ref-v1\0");
        digest.update(self.seed);
        digest.update(self.generation.to_be_bytes());
        digest.update(self.token_counter.to_be_bytes());
        digest.update(window.pid.to_be_bytes());
        digest.update(window.window_id.to_be_bytes());
        base64url_no_pad(&digest.finalize()[..24])
    }
}

impl Drop for ComputerToolHandler {
    fn drop(&mut self) {
        let _ = self.stop_child(true);
    }
}

fn parse_observe_input(input: Value) -> Result<ObserveInput, ComputerError> {
    let input: ObserveInput = serde_json::from_value(input).map_err(|_| ComputerError::Protocol)?;
    if input.window_ref.is_empty()
        || input.window_ref.len() > 96
        || !input
            .window_ref
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        || input
            .max_elements
            .is_some_and(|value| !(1..=MAX_ELEMENTS).contains(&value))
    {
        return Err(ComputerError::Protocol);
    }
    Ok(input)
}

fn windows_result(
    windows: &[Value],
    total_visible: usize,
    evidence: &DriverEvidence,
    elapsed: Duration,
) -> Value {
    json!({
        "windows": windows,
        "returned": windows.len(),
        "total_visible": total_visible,
        "truncated": windows.len() < total_visible,
        "elapsed_ms": elapsed.as_millis().min(u128::from(u64::MAX)) as u64,
        "host": evidence.to_json(),
    })
}

fn observation_result(
    window_ref: &str,
    elements: &[Value],
    available: usize,
    evidence: &DriverEvidence,
    elapsed: Duration,
) -> Value {
    json!({
        "window_ref": window_ref,
        "elements": elements,
        "returned": elements.len(),
        "available": available,
        "truncated": elements.len() < available,
        "elapsed_ms": elapsed.as_millis().min(u128::from(u64::MAX)) as u64,
        "host": evidence.to_json(),
    })
}

fn serialized_len(value: &Value) -> Result<usize, ComputerError> {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .map_err(|_| ComputerError::Protocol)
}

fn unique_frontmost_z(windows: &[RawWindow]) -> Option<i64> {
    let maximum = windows.iter().filter_map(|window| window.z_index).max()?;
    (windows
        .iter()
        .filter(|window| window.z_index == Some(maximum))
        .count()
        == 1)
        .then_some(maximum)
}

fn resolve_executable(configured: Option<&Path>) -> Result<PathBuf, ComputerError> {
    let candidate = if let Some(configured) = configured {
        if !configured.is_absolute() {
            return Err(ComputerError::DriverMissing);
        }
        configured.to_path_buf()
    } else {
        let path = env::var_os("PATH").ok_or(ComputerError::DriverMissing)?;
        env::split_paths(&path)
            .map(|directory| directory.join("cua-driver"))
            .find(|candidate| executable_file(candidate))
            .ok_or(ComputerError::DriverMissing)?
    };
    if !executable_file(&candidate) {
        return Err(ComputerError::DriverMissing);
    }
    candidate
        .canonicalize()
        .map_err(|_| ComputerError::DriverMissing)
}

fn executable_file(path: &Path) -> bool {
    let Ok(metadata) = path.metadata() else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

fn inspect_driver(
    executable: &Path,
    cancel: Option<&AtomicBool>,
) -> Result<(String, String, String), ComputerError> {
    check_cancel_flag(cancel)?;
    let mut command = Command::new(executable);
    command
        .arg("--version")
        .env_clear()
        .env("CUA_DRIVER_RS_TELEMETRY_ENABLED", "false")
        .env("CUA_DRIVER_RS_UPDATE_CHECK", "false")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    command.process_group(0);
    #[cfg(target_os = "linux")]
    set_parent_death_signal(&mut command);
    let mut child = command.spawn().map_err(|error| match error.kind() {
        io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied => ComputerError::DriverMissing,
        _ => ComputerError::DriverStartFailed,
    })?;
    let deadline = Instant::now() + VERSION_TIMEOUT;
    let process_group = child.id();
    let Some(stdout_pipe) = child.stdout.take() else {
        let _ = terminate_process_tree(&mut child);
        return Err(ComputerError::DriverStartFailed);
    };
    let Some(stderr_pipe) = child.stderr.take() else {
        let _ = terminate_process_tree(&mut child);
        return Err(ComputerError::DriverStartFailed);
    };
    let (stdout_sender, stdout) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let _ = stdout_sender.send(read_capped(stdout_pipe, 4096));
    });
    let (stderr_sender, stderr) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let _ = stderr_sender.send(read_capped(stderr_pipe, MAX_STDERR_BYTES));
    });
    let status = loop {
        check_cancel_flag(cancel).inspect_err(|_| {
            let _ = terminate_process_tree(&mut child);
        })?;
        if let Some(status) = child
            .try_wait()
            .map_err(|_| ComputerError::DriverStartFailed)?
        {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = terminate_process_tree(&mut child);
            return Err(ComputerError::Timeout);
        }
        thread::sleep(Duration::from_millis(10));
    };
    kill_process_group(process_group);
    let stdout = receive_probe_output(&stdout, deadline, cancel)?;
    let stderr = receive_probe_output(&stderr, deadline, cancel)?;
    if stdout.1 || stderr.1 {
        return Err(ComputerError::OutputTooLarge);
    }
    if !status.success()
        || std::str::from_utf8(&stdout.0).ok().map(str::trim) != Some(DRIVER_VERSION_OUTPUT)
    {
        return Err(ComputerError::DriverVersionMismatch);
    }

    check_cancel_flag(cancel)?;
    let executable_sha256 = sha256_file(executable, cancel)?;
    let mut path_digest = Sha256::new();
    path_digest.update(b"platonic-cua-path-v1\0");
    update_path_digest(&mut path_digest, executable);
    Ok((
        DRIVER_VERSION.into(),
        executable_sha256,
        hex_digest(path_digest.finalize()),
    ))
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn set_parent_death_signal(command: &mut Command) {
    let expected_parent = rustix::process::getpid();
    // SAFETY: the pre-exec closure performs only rustix process syscalls.
    unsafe {
        command.pre_exec(move || {
            rustix::process::set_parent_process_death_signal(Some(rustix::process::Signal::KILL))
                .map_err(io::Error::from)?;
            if rustix::process::getppid() != Some(expected_parent) {
                return Err(rustix::io::Errno::SRCH.into());
            }
            Ok(())
        });
    }
}

fn receive_probe_output(
    receiver: &Receiver<io::Result<(Vec<u8>, bool)>>,
    deadline: Instant,
    cancel: Option<&AtomicBool>,
) -> Result<(Vec<u8>, bool), ComputerError> {
    loop {
        check_cancel_flag(cancel)?;
        let now = Instant::now();
        if now >= deadline {
            return Err(ComputerError::Timeout);
        }
        match receiver.recv_timeout((deadline - now).min(Duration::from_millis(10))) {
            Ok(output) => return output.map_err(|_| ComputerError::DriverStartFailed),
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => {
                return Err(ComputerError::DriverStartFailed);
            }
        }
    }
}

fn sha256_file(path: &Path, cancel: Option<&AtomicBool>) -> Result<String, ComputerError> {
    let mut file = File::open(path).map_err(|_| ComputerError::DriverStartFailed)?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        check_cancel_flag(cancel)?;
        let read = file
            .read(&mut buffer)
            .map_err(|_| ComputerError::DriverStartFailed)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex_digest(digest.finalize()))
}

fn random_seed() -> Result<[u8; 32], ComputerError> {
    let mut seed = [0u8; 32];
    File::open("/dev/urandom")
        .and_then(|mut random| random.read_exact(&mut seed))
        .map_err(|_| ComputerError::DriverStartFailed)?;
    Ok(seed)
}

fn process_identity(pid: u32) -> Option<(String, String)> {
    #[cfg(target_os = "linux")]
    {
        let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let mut fields = stat.get(stat.rfind(')')? + 1..)?.split_whitespace();
        let start = fields.nth(19)?.to_owned();
        let executable = fs::read_link(format!("/proc/{pid}/exe")).ok()?;
        let metadata = fs::metadata(format!("/proc/{pid}/exe")).ok()?;
        let mut digest = Sha256::new();
        digest.update(b"platonic-process-executable-v1\0");
        update_path_digest(&mut digest, &executable);
        digest.update(metadata.dev().to_be_bytes());
        digest.update(metadata.ino().to_be_bytes());
        Some((start, hex_digest(digest.finalize())))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        None
    }
}

fn check_cancel_flag(cancel: Option<&AtomicBool>) -> Result<(), ComputerError> {
    if cancel.is_some_and(|cancel| cancel.load(Ordering::SeqCst)) {
        Err(ComputerError::Canceled)
    } else {
        Ok(())
    }
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    let bytes = bytes.as_ref();
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn update_path_digest(digest: &mut Sha256, path: &Path) {
    update_os_digest(digest, path.as_os_str());
}

fn update_os_digest(digest: &mut Sha256, value: &OsStr) {
    #[cfg(unix)]
    digest.update(value.as_bytes());
    #[cfg(not(unix))]
    digest.update(value.to_string_lossy().as_bytes());
}

fn base64url_no_pad(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let value = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        output.push(ALPHABET[((value >> 18) & 63) as usize] as char);
        output.push(ALPHABET[((value >> 12) & 63) as usize] as char);
        if chunk.len() > 1 {
            output.push(ALPHABET[((value >> 6) & 63) as usize] as char);
        }
        if chunk.len() > 2 {
            output.push(ALPHABET[(value & 63) as usize] as char);
        }
    }
    output
}

fn read_capped(mut reader: impl Read, limit: usize) -> io::Result<(Vec<u8>, bool)> {
    let mut retained = Vec::new();
    let mut overflow = false;
    let mut buffer = [0u8; 8192];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        let remaining = limit.saturating_sub(retained.len());
        retained.extend_from_slice(&buffer[..read.min(remaining)]);
        overflow |= read > remaining;
    }
    Ok((retained, overflow))
}

#[derive(Debug)]
enum FrameEvent {
    Frame(Vec<u8>),
    Oversized,
    Partial,
    Io,
    Eof,
}

#[derive(Debug, Default)]
struct StderrState {
    retained: Vec<u8>,
    overflow: bool,
}

struct McpChild {
    child: Child,
    stdin: Option<std::process::ChildStdin>,
    frames: Receiver<FrameEvent>,
    stdout_reader: Option<JoinHandle<()>>,
    stderr_reader: Option<JoinHandle<()>>,
    stderr: Arc<Mutex<StderrState>>,
    next_id: u64,
}

impl McpChild {
    fn spawn(
        executable: &Path,
        display_env: &[(OsString, OsString)],
    ) -> Result<Self, ComputerError> {
        let mut command = Command::new(executable);
        command
            .args(["mcp", "--direct"])
            .env_clear()
            .envs(display_env.iter().cloned())
            .env("CUA_DRIVER_PERMISSION_MODE", "standard")
            .env("CUA_DRIVER_RS_TELEMETRY_ENABLED", "false")
            .env("CUA_DRIVER_RS_UPDATE_CHECK", "false")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(unix)]
        command.process_group(0);
        #[cfg(target_os = "linux")]
        set_parent_death_signal(&mut command);
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(_) => return Err(ComputerError::DriverStartFailed),
        };
        let stdin = child.stdin.take().ok_or(ComputerError::DriverStartFailed)?;
        let stdout = child
            .stdout
            .take()
            .ok_or(ComputerError::DriverStartFailed)?;
        let stderr = child
            .stderr
            .take()
            .ok_or(ComputerError::DriverStartFailed)?;
        let (sender, frames) = mpsc::channel();
        let stdout_reader = thread::spawn(move || read_stdout_frames(stdout, sender));
        let stderr_state = Arc::new(Mutex::new(StderrState::default()));
        let stderr_capture = Arc::clone(&stderr_state);
        let stderr_reader = thread::spawn(move || read_stderr(stderr, stderr_capture));
        Ok(Self {
            child,
            stdin: Some(stdin),
            frames,
            stdout_reader: Some(stdout_reader),
            stderr_reader: Some(stderr_reader),
            stderr: stderr_state,
            next_id: 1,
        })
    }

    fn notify(&mut self, notification: Value) -> Result<(), ComputerError> {
        self.write_message(&notification)
    }

    fn request(
        &mut self,
        mut request: Value,
        timeout: Duration,
        cancel: Option<&AtomicBool>,
    ) -> Result<Value, ComputerError> {
        if self.frames.try_recv().is_ok() {
            return Err(ComputerError::Protocol);
        }
        self.check_stderr()?;
        let id = self.next_id;
        self.next_id = self.next_id.checked_add(1).ok_or(ComputerError::Protocol)?;
        request
            .as_object_mut()
            .ok_or(ComputerError::Protocol)?
            .insert("id".into(), json!(id));
        self.write_message(&request)?;
        let deadline = Instant::now() + timeout;
        loop {
            check_cancel_flag(cancel)?;
            self.check_stderr()?;
            let now = Instant::now();
            if now >= deadline {
                return Err(ComputerError::Timeout);
            }
            match self
                .frames
                .recv_timeout((deadline - now).min(Duration::from_millis(20)))
            {
                Ok(FrameEvent::Frame(frame)) => {
                    let response = parse_response_frame(&frame, id)?;
                    return match self.frames.recv_timeout(Duration::from_millis(2)) {
                        Err(RecvTimeoutError::Timeout) => Ok(response),
                        Ok(_) | Err(RecvTimeoutError::Disconnected) => Err(ComputerError::Protocol),
                    };
                }
                Ok(FrameEvent::Oversized) => return Err(ComputerError::OutputTooLarge),
                Ok(FrameEvent::Partial | FrameEvent::Io | FrameEvent::Eof) => {
                    return Err(ComputerError::Protocol);
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => return Err(ComputerError::Protocol),
            }
        }
    }

    fn write_message(&mut self, message: &Value) -> Result<(), ComputerError> {
        let serialized = serde_json::to_vec(message).map_err(|_| ComputerError::Protocol)?;
        if serialized.len() > MAX_MCP_FRAME_BYTES {
            return Err(ComputerError::OutputTooLarge);
        }
        let stdin = self
            .stdin
            .as_mut()
            .ok_or(ComputerError::DriverStartFailed)?;
        stdin
            .write_all(&serialized)
            .and_then(|()| stdin.write_all(b"\n"))
            .and_then(|()| stdin.flush())
            .map_err(|_| ComputerError::DriverStartFailed)
    }

    fn check_stderr(&self) -> Result<(), ComputerError> {
        if self
            .stderr
            .lock()
            .map_err(|_| ComputerError::Protocol)?
            .overflow
        {
            Err(ComputerError::OutputTooLarge)
        } else {
            Ok(())
        }
    }

    fn stop(&mut self, force: bool, cancel: Option<&AtomicBool>) -> Result<(), ComputerError> {
        self.stdin.take();
        let deadline = Instant::now() + CLEANUP_TIMEOUT;
        let graceful_deadline = deadline
            .checked_sub(Duration::from_millis(50))
            .unwrap_or(deadline);
        let mut forced = force;
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) if !forced && Instant::now() < graceful_deadline => {
                    forced |= cancel.is_some_and(|cancel| cancel.load(Ordering::SeqCst));
                    thread::sleep(Duration::from_millis(10));
                }
                Ok(None) => {
                    terminate_process_tree(&mut self.child)
                        .map_err(|_| ComputerError::CleanupFailed)?;
                    break;
                }
                Err(_) => return Err(ComputerError::CleanupFailed),
            }
        }
        kill_process_group(self.child.id());
        self.finish_readers(deadline)?;
        if self
            .frames
            .try_iter()
            .any(|event| !matches!(event, FrameEvent::Eof))
        {
            return Err(ComputerError::CleanupFailed);
        }
        if self.check_stderr().is_err() {
            return Err(ComputerError::CleanupFailed);
        }
        Ok(())
    }

    fn finish_readers(&mut self, deadline: Instant) -> Result<(), ComputerError> {
        while self
            .stdout_reader
            .as_ref()
            .is_some_and(|reader| !reader.is_finished())
            || self
                .stderr_reader
                .as_ref()
                .is_some_and(|reader| !reader.is_finished())
        {
            if Instant::now() >= deadline {
                self.stdout_reader.take();
                self.stderr_reader.take();
                return Err(ComputerError::CleanupFailed);
            }
            thread::sleep(Duration::from_millis(5));
        }
        if let Some(reader) = self.stdout_reader.take() {
            reader.join().map_err(|_| ComputerError::CleanupFailed)?;
        }
        if let Some(reader) = self.stderr_reader.take() {
            reader.join().map_err(|_| ComputerError::CleanupFailed)?;
        }
        Ok(())
    }
}

impl Drop for McpChild {
    fn drop(&mut self) {
        self.stdin.take();
        let deadline = Instant::now() + CLEANUP_TIMEOUT;
        let _ = terminate_process_tree(&mut self.child);
        let _ = self.finish_readers(deadline);
    }
}

fn terminate_process_tree(child: &mut Child) -> io::Result<std::process::ExitStatus> {
    kill_process_group(child.id());
    let _ = child.kill();
    child.wait()
}

fn kill_process_group(process_group: u32) {
    #[cfg(unix)]
    if let Some(process_group) = rustix::process::Pid::from_raw(process_group as i32) {
        let _ = rustix::process::kill_process_group(process_group, rustix::process::Signal::KILL);
    }
}

fn read_stdout_frames(stdout: ChildStdout, sender: mpsc::Sender<FrameEvent>) {
    let mut reader = BufReader::new(stdout);
    loop {
        let mut retained = Vec::new();
        let mut oversized = false;
        loop {
            let buffer = match reader.fill_buf() {
                Ok(buffer) => buffer,
                Err(_) => {
                    let _ = sender.send(FrameEvent::Io);
                    return;
                }
            };
            if buffer.is_empty() {
                let event = if retained.is_empty() {
                    FrameEvent::Eof
                } else {
                    FrameEvent::Partial
                };
                let _ = sender.send(event);
                return;
            }
            let consumed = buffer
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(buffer.len(), |position| position + 1);
            let data = &buffer[..consumed];
            let remaining = MAX_MCP_FRAME_BYTES.saturating_sub(retained.len());
            retained.extend_from_slice(&data[..data.len().min(remaining)]);
            oversized |= data.len() > remaining;
            let ended = data.ends_with(b"\n");
            reader.consume(consumed);
            if ended {
                while retained
                    .last()
                    .is_some_and(|byte| matches!(byte, b'\r' | b'\n'))
                {
                    retained.pop();
                }
                let event = if oversized {
                    FrameEvent::Oversized
                } else {
                    FrameEvent::Frame(retained)
                };
                if sender.send(event).is_err() {
                    return;
                }
                break;
            }
        }
    }
}

fn read_stderr(mut stderr: ChildStderr, state: Arc<Mutex<StderrState>>) {
    let mut buffer = [0u8; 4096];
    loop {
        let Ok(read) = stderr.read(&mut buffer) else {
            return;
        };
        if read == 0 {
            return;
        }
        let Ok(mut state) = state.lock() else {
            return;
        };
        let remaining = MAX_STDERR_BYTES.saturating_sub(state.retained.len());
        state
            .retained
            .extend_from_slice(&buffer[..read.min(remaining)]);
        state.overflow |= read > remaining;
    }
}

fn parse_response_frame(frame: &[u8], expected_id: u64) -> Result<Value, ComputerError> {
    let response: Value = serde_json::from_slice(frame).map_err(|_| ComputerError::Protocol)?;
    let response = strict_object(&response, &["jsonrpc", "id", "result", "error"])?;
    if response.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || response.get("id").and_then(Value::as_u64) != Some(expected_id)
        || response.contains_key("result") == response.contains_key("error")
    {
        return Err(ComputerError::Protocol);
    }
    if let Some(error) = response.get("error") {
        let error = strict_object(error, &["code", "message", "data"])?;
        if error.get("code").and_then(Value::as_i64).is_none()
            || error.get("message").and_then(Value::as_str).is_none()
        {
            return Err(ComputerError::Protocol);
        }
        let permission = error
            .get("message")
            .and_then(Value::as_str)
            .is_some_and(permission_message)
            || error.get("data").is_some_and(value_mentions_permission);
        return Err(if permission {
            ComputerError::PermissionDenied
        } else {
            ComputerError::Protocol
        });
    }
    response
        .get("result")
        .cloned()
        .ok_or(ComputerError::Protocol)
}

fn validate_initialize(result: Value) -> Result<(), ComputerError> {
    let result = strict_object(
        &result,
        &[
            "protocolVersion",
            "capabilities",
            "serverInfo",
            "instructions",
        ],
    )?;
    if result.get("protocolVersion").and_then(Value::as_str) != Some(MCP_PROTOCOL_VERSION) {
        return Err(ComputerError::Protocol);
    }
    let capabilities = strict_object(
        result.get("capabilities").ok_or(ComputerError::Protocol)?,
        &["tools"],
    )?;
    let tools = strict_object(
        capabilities.get("tools").ok_or(ComputerError::Protocol)?,
        &[],
    )?;
    if !tools.is_empty() {
        return Err(ComputerError::Protocol);
    }
    let server = strict_object(
        result.get("serverInfo").ok_or(ComputerError::Protocol)?,
        &["name", "version"],
    )?;
    if server.get("name").and_then(Value::as_str) != Some("cua-driver")
        || server.get("version").and_then(Value::as_str) != Some(DRIVER_VERSION)
        || result
            .get("instructions")
            .is_some_and(|value| value.as_str().is_none())
    {
        return Err(ComputerError::DriverVersionMismatch);
    }
    Ok(())
}

fn parse_tool_result(result: Value) -> Result<Value, ComputerError> {
    reject_visual_data(&result)?;
    let result = strict_object(&result, &["content", "isError", "structuredContent"])?;
    let content = result
        .get("content")
        .and_then(Value::as_array)
        .ok_or(ComputerError::Protocol)?;
    for block in content {
        let block = strict_object(block, &["type", "text", "annotations"])?;
        if block.get("type").and_then(Value::as_str) != Some("text")
            || block.get("text").and_then(Value::as_str).is_none()
            || block
                .get("annotations")
                .is_some_and(|annotations| !annotations.is_null())
        {
            return Err(ComputerError::UnexpectedImage);
        }
    }
    if result
        .get("isError")
        .is_some_and(|value| value.as_bool().is_none())
    {
        return Err(ComputerError::Protocol);
    }
    if result.get("isError").and_then(Value::as_bool) == Some(true) {
        let permission = content.iter().any(value_mentions_permission)
            || result
                .get("structuredContent")
                .is_some_and(value_mentions_permission);
        return Err(if permission {
            ComputerError::PermissionDenied
        } else {
            ComputerError::Protocol
        });
    }
    result
        .get("structuredContent")
        .cloned()
        .ok_or(ComputerError::Protocol)
}

fn parse_windows(structured: Value) -> Result<Vec<RawWindow>, ComputerError> {
    let structured = strict_object(&structured, &["windows"])?;
    let windows = structured
        .get("windows")
        .and_then(Value::as_array)
        .ok_or(ComputerError::Protocol)?;
    let mut parsed = Vec::with_capacity(windows.len().min(MAX_WINDOWS));
    for window in windows {
        let window = strict_object(
            window,
            &[
                "window_id",
                "pid",
                "app_name",
                "title",
                "bounds",
                "is_on_screen",
                "z_index",
                "x",
                "y",
                "width",
                "height",
            ],
        )?;
        let bounds = parse_bounds(
            window.get("bounds").ok_or(ComputerError::Protocol)?,
            "width",
            "height",
        )?;
        if window.get("x").and_then(Value::as_i64) != Some(bounds.x)
            || window.get("y").and_then(Value::as_i64) != Some(bounds.y)
            || window.get("width").and_then(Value::as_u64) != Some(bounds.width)
            || window.get("height").and_then(Value::as_u64) != Some(bounds.height)
        {
            return Err(ComputerError::Protocol);
        }
        let is_on_screen = window
            .get("is_on_screen")
            .and_then(Value::as_bool)
            .ok_or(ComputerError::Protocol)?;
        let pid = window
            .get("pid")
            .and_then(Value::as_u64)
            .and_then(|pid| u32::try_from(pid).ok());
        let window_id = window.get("window_id").and_then(Value::as_u64);
        let z_index = match window.get("z_index") {
            Some(Value::Null) => None,
            Some(value) => Some(value.as_i64().ok_or(ComputerError::Protocol)?),
            None => return Err(ComputerError::Protocol),
        };
        let app = window
            .get("app_name")
            .and_then(Value::as_str)
            .ok_or(ComputerError::Protocol)?;
        let title = window
            .get("title")
            .and_then(Value::as_str)
            .ok_or(ComputerError::Protocol)?;
        if is_on_screen
            && bounds.width > 0
            && bounds.height > 0
            && let (Some(pid), Some(window_id)) = (pid, window_id)
            && pid > 0
            && window_id > 0
        {
            parsed.push(RawWindow {
                pid,
                window_id,
                app: truncate_utf8(app, MAX_STRING_BYTES),
                title: truncate_utf8(title, MAX_STRING_BYTES),
                bounds,
                z_index,
            });
        }
    }
    Ok(parsed)
}

struct NormalizedElements {
    values: Vec<Value>,
    available: usize,
}

fn normalize_observation(
    structured: Value,
    max_elements: usize,
    expected_pid: u32,
    expected_window_id: u64,
) -> Result<NormalizedElements, ComputerError> {
    reject_visual_data(&structured)?;
    let structured = strict_object(
        &structured,
        &[
            "window_id",
            "pid",
            "element_count",
            "elements_complete",
            "tree_markdown",
            "total_element_count",
            "returned_element_count",
            "elements",
            "snapshot_id",
            "_note",
            "degraded",
            "degraded_reason",
            "escalation",
        ],
    )?;
    for key in [
        "window_id",
        "pid",
        "element_count",
        "total_element_count",
        "returned_element_count",
    ] {
        if structured.get(key).and_then(Value::as_u64).is_none() {
            return Err(ComputerError::Protocol);
        }
    }
    if structured.get("pid").and_then(Value::as_u64) != Some(u64::from(expected_pid))
        || structured.get("window_id").and_then(Value::as_u64) != Some(expected_window_id)
    {
        return Err(ComputerError::Protocol);
    }
    if structured
        .get("elements_complete")
        .is_some_and(|value| value.as_bool().is_none())
        || structured
            .get("tree_markdown")
            .is_some_and(|value| value.as_str().is_none())
        || structured
            .get("snapshot_id")
            .is_some_and(|value| value.as_str().is_none())
        || structured
            .get("_note")
            .is_some_and(|value| value.as_str().is_none())
        || structured
            .get("degraded")
            .is_some_and(|value| value.as_bool().is_none())
        || structured
            .get("degraded_reason")
            .is_some_and(|value| value.as_str().is_none())
    {
        return Err(ComputerError::Protocol);
    }
    let elements = structured
        .get("elements")
        .and_then(Value::as_array)
        .ok_or(ComputerError::Protocol)?;
    let returned = structured
        .get("returned_element_count")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or(ComputerError::Protocol)?;
    let available = structured
        .get("total_element_count")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or(ComputerError::Protocol)?;
    if returned != elements.len() || available < returned {
        return Err(ComputerError::Protocol);
    }

    let mut normalized = Vec::with_capacity(elements.len().min(max_elements));
    for element in elements.iter().take(max_elements) {
        let element = strict_object(
            element,
            &[
                "element_index",
                "element_token",
                "in_web_content",
                "role",
                "name",
                "label",
                "value",
                "description",
                "enabled",
                "focused",
                "selected",
                "parent_index",
                "depth",
                "frame",
            ],
        )?;
        if element
            .get("element_index")
            .and_then(Value::as_u64)
            .is_none()
            || element
                .get("element_token")
                .is_some_and(|value| value.as_str().is_none())
            || element
                .get("in_web_content")
                .is_some_and(|value| value.as_bool().is_none())
            || element
                .get("parent_index")
                .is_some_and(|value| value.as_u64().is_none())
        {
            return Err(ComputerError::Protocol);
        }
        let role = element
            .get("role")
            .and_then(Value::as_str)
            .ok_or(ComputerError::Protocol)?;
        let depth = element
            .get("depth")
            .and_then(Value::as_u64)
            .ok_or(ComputerError::Protocol)?;
        if depth > MAX_DEPTH {
            return Err(ComputerError::Protocol);
        }
        let mut output = Map::new();
        output.insert(
            "role".into(),
            Value::String(truncate_utf8(role, MAX_STRING_BYTES)),
        );
        for key in ["name", "label", "value", "description"] {
            if let Some(value) = element.get(key) {
                let value = value.as_str().ok_or(ComputerError::Protocol)?;
                output.insert(
                    key.into(),
                    Value::String(truncate_utf8(value, MAX_STRING_BYTES)),
                );
            }
        }
        for key in ["enabled", "focused", "selected"] {
            if let Some(value) = element.get(key) {
                let value = value.as_bool().ok_or(ComputerError::Protocol)?;
                output.insert(key.into(), Value::Bool(value));
            }
        }
        output.insert("depth".into(), json!(depth));
        if let Some(frame) = element.get("frame") {
            output.insert("bounds".into(), parse_bounds(frame, "w", "h")?.to_json());
        }
        normalized.push(Value::Object(output));
    }
    Ok(NormalizedElements {
        values: normalized,
        available,
    })
}

fn parse_bounds(value: &Value, width: &str, height: &str) -> Result<Bounds, ComputerError> {
    let bounds = strict_object(value, &["x", "y", width, height])?;
    Ok(Bounds {
        x: bounds
            .get("x")
            .and_then(Value::as_i64)
            .ok_or(ComputerError::Protocol)?,
        y: bounds
            .get("y")
            .and_then(Value::as_i64)
            .ok_or(ComputerError::Protocol)?,
        width: bounds
            .get(width)
            .and_then(Value::as_u64)
            .ok_or(ComputerError::Protocol)?,
        height: bounds
            .get(height)
            .and_then(Value::as_u64)
            .ok_or(ComputerError::Protocol)?,
    })
}

fn strict_object<'a>(
    value: &'a Value,
    allowed: &[&str],
) -> Result<&'a Map<String, Value>, ComputerError> {
    let object = value.as_object().ok_or(ComputerError::Protocol)?;
    if object
        .keys()
        .any(|key| !allowed.iter().any(|allowed| key == allowed))
    {
        return Err(ComputerError::Protocol);
    }
    Ok(object)
}

fn reject_visual_data(value: &Value) -> Result<(), ComputerError> {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                let key = key.to_ascii_lowercase();
                if key.contains("screenshot")
                    || key == "image"
                    || key == "images"
                    || key.contains("media")
                    || object.get("type").and_then(Value::as_str) == Some("image")
                {
                    return Err(ComputerError::UnexpectedImage);
                }
                reject_visual_data(value)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                reject_visual_data(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn permission_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    [
        "permission",
        "denied",
        "outside the bounded",
        "not authorized",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

fn value_mentions_permission(value: &Value) -> bool {
    match value {
        Value::String(value) => permission_message(value),
        Value::Array(values) => values.iter().any(value_mentions_permission),
        Value::Object(object) => object.values().any(value_mentions_permission),
        _ => false,
    }
}

fn truncate_utf8(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.into();
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use platonic_core::{EffectClass, ToolName};
    use std::os::unix::fs::PermissionsExt;

    struct ChildGuard(Child);

    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn display() -> DisplaySession {
        DisplaySession::from_lookup(|name| match name {
            "DISPLAY" => Some(OsString::from(":561")),
            _ => None,
        })
        .unwrap()
    }

    fn write_driver(directory: &Path, mode: &str, log: &Path) -> PathBuf {
        let path = directory.join("cua-driver");
        let log = serde_json::to_string(log.to_str().unwrap()).unwrap();
        let mode = serde_json::to_string(mode).unwrap();
        let source = format!(
            r#"#!/usr/bin/python3
import ctypes, json, os, sys, time
MODE = {mode}
LOG = {log}
TARGET_PID = {target_pid}

def record(value):
    with open(LOG, "a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, sort_keys=True) + "\n")

def parent_death_signal():
    value = ctypes.c_int()
    if ctypes.CDLL(None, use_errno=True).prctl(2, ctypes.byref(value), 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "PR_GET_PDEATHSIG")
    return value.value

if sys.argv[1:] == ["--version"]:
    if MODE == "pdeathsig":
        record({{"event": "version_pdeathsig", "signal": parent_death_signal()}})
    if MODE == "probe_pipe_holder":
        descendant = os.fork()
        if descendant == 0:
            time.sleep(30)
            raise SystemExit(0)
        record({{"event": "probe_descendant", "pid": descendant}})
        print("cua-driver 0.19.3", flush=True)
        raise SystemExit(0)
    versions = {{
        "normal": "cua-driver 0.19.3",
        "wrong_old": "cua-driver 0.14.1",
        "wrong_new": "cua-driver 0.20.0",
        "malformed": "version nineteen",
    }}
    print(versions.get(MODE, "cua-driver 0.19.3"))
    raise SystemExit(0)

if MODE == "pdeathsig":
    record({{"event": "mcp_pdeathsig", "signal": parent_death_signal()}})
raw_env = open("/proc/self/environ", "rb").read().split(b"\0")
names = sorted(item.split(b"=", 1)[0].decode() for item in raw_env if item)
record({{
    "event": "start",
    "pid": os.getpid(),
    "argv": sys.argv[1:],
    "env_names": names,
    "permission_mode": os.environ["CUA_DRIVER_PERMISSION_MODE"],
}})
if MODE == "stderr_overflow":
    os.write(2, b"x" * 17000)
    time.sleep(0.05)

def emit(identifier, result):
    response = {{"jsonrpc": "2.0", "id": identifier, "result": result}}
    print(json.dumps(response), flush=True)
    if MODE == "duplicate":
        print(json.dumps(response), flush=True)

for line in sys.stdin:
    request = json.loads(line)
    record({{"event": "request", "request": request}})
    if "id" not in request:
        continue
    identifier = request["id"] + (1 if MODE == "wrong_id" else 0)
    if request["method"] == "initialize":
        if MODE == "oversize":
            print("x" * (1024 * 1024 + 1), flush=True)
            continue
        if MODE == "partial":
            sys.stdout.write('{{"jsonrpc":')
            sys.stdout.flush()
            raise SystemExit(0)
        if MODE == "crash":
            raise SystemExit(7)
        emit(identifier, {{
            "protocolVersion": "2025-06-18",
            "capabilities": {{"tools": {{}}}},
            "serverInfo": {{"name": "cua-driver", "version": "0.19.3"}},
        }})
        continue
    name = request["params"]["name"]
    if MODE == "timeout" and name == "list_windows":
        time.sleep(2)
    if name == "list_windows":
        structured = {{"windows": [
            {{"window_id": 101, "pid": TARGET_PID, "app_name": "Fixture", "title": "Visible", "bounds": {{"x": 4, "y": 8, "width": 640, "height": 480}}, "is_on_screen": True, "z_index": 9, "x": 4, "y": 8, "width": 640, "height": 480}},
            {{"window_id": 102, "pid": TARGET_PID, "app_name": "Fixture", "title": "Hidden", "bounds": {{"x": 0, "y": 0, "width": 10, "height": 10}}, "is_on_screen": False, "z_index": 8, "x": 0, "y": 0, "width": 10, "height": 10}},
            {{"window_id": 103, "pid": TARGET_PID, "app_name": "Fixture", "title": "Minimized", "bounds": {{"x": 0, "y": 0, "width": 0, "height": 0}}, "is_on_screen": True, "z_index": 7, "x": 0, "y": 0, "width": 0, "height": 0}},
        ]}}
    else:
        assert name == "get_window_state"
        assert request["params"]["arguments"]["include_screenshot"] is False
        assert request["params"]["arguments"]["max_depth"] == 32
        structured = {{
            "window_id": 101,
            "pid": TARGET_PID,
            "element_count": 2,
            "elements_complete": False,
            "tree_markdown": "untrusted raw tree",
            "total_element_count": 2,
            "returned_element_count": 2,
            "snapshot_id": "upstream-secret",
            "elements": [
                {{"element_index": 1, "element_token": "upstream-token", "role": "button", "label": "Save", "enabled": True, "selected": False, "depth": 1, "frame": {{"x": 10, "y": 20, "w": 80, "h": 30}}}},
                {{"element_index": 2, "role": "textbox", "label": "Name", "value": "Ada", "depth": 2}},
            ],
        }}
        if MODE == "cross_pid":
            structured["pid"] = TARGET_PID + 1
        if MODE == "cross_window":
            structured["window_id"] = 102
    if MODE == "image" and name == "get_window_state":
        result = {{"content": [{{"type": "image", "data": "AAAA", "mimeType": "image/png"}}], "structuredContent": structured}}
    elif MODE == "permission" and name == "get_window_state":
        result = {{"content": [{{"type": "text", "text": "permission denied for /secret/path"}}], "isError": True, "structuredContent": {{"code": "denied"}}}}
    else:
        result = {{"content": [{{"type": "text", "text": "raw upstream identifiers"}}], "structuredContent": structured}}
    emit(identifier, result)

record({{"event": "stop"}})
if MODE == "cleanup_pipe_holder":
    descendant = os.fork()
    if descendant == 0:
        time.sleep(30)
        raise SystemExit(0)
    record({{"event": "cleanup_descendant", "pid": descendant}})
if MODE == "shutdown_refusal":
    time.sleep(2)
"#,
            target_pid = std::process::id(),
        );
        fs::write(&path, source).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    fn handler(mode: &str) -> (tempfile::TempDir, PathBuf, ComputerToolHandler) {
        handler_with_cancel(mode, None)
    }

    fn handler_with_cancel(
        mode: &str,
        cancel: Option<Arc<AtomicBool>>,
    ) -> (tempfile::TempDir, PathBuf, ComputerToolHandler) {
        let directory = tempfile::tempdir().unwrap();
        let log = directory.path().join("driver.jsonl");
        let executable = write_driver(directory.path(), mode, &log);
        let handler = ComputerToolHandler::build(
            &ComputerConfig {
                executable: Some(executable),
            },
            cancel,
            display(),
        )
        .unwrap();
        (directory, log, handler)
    }

    fn call(tool: &str, input: Value) -> ToolCall {
        ToolCall {
            id: ToolCallId::new("call_policy").unwrap(),
            tool: ToolName::new(tool).unwrap(),
            effect: EffectClass::SecretAccess,
            input,
        }
    }

    fn log_records(path: &Path) -> Vec<Value> {
        fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn with_test_display<T>(run: impl FnOnce() -> T) -> T {
        temp_env::with_vars(
            [
                ("DISPLAY", Some(":561")),
                ("WAYLAND_DISPLAY", None),
                ("XDG_SESSION_TYPE", Some("x11")),
                ("XAUTHORITY", None),
                ("DBUS_SESSION_BUS_ADDRESS", None),
                ("AT_SPI_BUS_ADDRESS", None),
                ("XDG_RUNTIME_DIR", None),
            ],
            run,
        )
    }

    #[test]
    fn display_contract_accepts_x11_and_xwayland_but_not_native_wayland() {
        let x11 = display();
        assert_eq!(x11.kind, "x11");
        let xwayland = DisplaySession::from_lookup(|name| match name {
            "DISPLAY" => Some(OsString::from(":0")),
            "WAYLAND_DISPLAY" => Some(OsString::from("wayland-0")),
            "XDG_SESSION_TYPE" => Some(OsString::from("wayland")),
            _ => None,
        })
        .unwrap();
        assert_eq!(xwayland.kind, "xwayland");
        let reclassified = DisplaySession::from_lookup(|name| match name {
            "DISPLAY" => Some(OsString::from(":561")),
            "XDG_SESSION_TYPE" => Some(OsString::from("wayland")),
            _ => None,
        })
        .unwrap();
        assert_eq!(reclassified.kind, "xwayland");
        assert_ne!(x11.identity, reclassified.identity);
        assert_eq!(
            DisplaySession::from_lookup(
                |name| (name == "WAYLAND_DISPLAY").then(|| OsString::from("wayland-0"))
            )
            .unwrap_err(),
            ComputerError::UnsupportedDisplayServer
        );
        assert_eq!(
            DisplaySession::from_lookup(|_| None).unwrap_err(),
            ComputerError::NoDisplay
        );
    }

    #[test]
    fn version_pin_rejects_missing_malformed_old_and_new_drivers() {
        let directory = tempfile::tempdir().unwrap();
        assert_eq!(
            resolve_executable(Some(&directory.path().join("missing"))).unwrap_err(),
            ComputerError::DriverMissing
        );
        for mode in ["malformed", "wrong_old", "wrong_new"] {
            let log = directory.path().join(format!("{mode}.jsonl"));
            let executable = directory.path().join(mode);
            let source = fs::read_to_string(write_driver(directory.path(), mode, &log)).unwrap();
            fs::write(&executable, source).unwrap();
            fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
            assert_eq!(
                inspect_driver(&executable, None).unwrap_err(),
                ComputerError::DriverVersionMismatch,
                "{mode}"
            );
        }
    }

    #[test]
    fn exact_driver_captures_digest_without_disclosing_path() {
        let directory = tempfile::tempdir().unwrap();
        let log = directory.path().join("driver.jsonl");
        let executable = write_driver(directory.path(), "normal", &log);
        let (version, digest, fingerprint) = inspect_driver(&executable, None).unwrap();

        assert_eq!(version, DRIVER_VERSION);
        assert_eq!(digest, sha256_file(&executable, None).unwrap());
        assert_eq!(digest.len(), 64);
        assert_eq!(fingerprint.len(), 64);
        assert!(!fingerprint.contains(executable.to_str().unwrap()));
    }

    #[test]
    fn version_probe_reaps_pipe_holding_process_group_within_timeout() {
        let directory = tempfile::tempdir().unwrap();
        let log = directory.path().join("probe.jsonl");
        let executable = write_driver(directory.path(), "probe_pipe_holder", &log);

        let started = Instant::now();
        inspect_driver(&executable, None).unwrap();
        assert!(started.elapsed() < VERSION_TIMEOUT);

        let descendant = log_records(&log)[0]["pid"].as_u64().unwrap();
        let process = PathBuf::from(format!("/proc/{descendant}"));
        let deadline = Instant::now() + Duration::from_secs(1);
        while process.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(!process.exists(), "version-probe descendant was not reaped");
    }

    #[test]
    fn version_and_mcp_children_report_parent_death_sigkill() {
        with_test_display(|| {
            let (_directory, log, mut handler) = handler("pdeathsig");
            handler
                .execute(
                    ToolCallId::new("call_pdeathsig").unwrap(),
                    COMPUTER_WINDOWS,
                    json!({}),
                )
                .unwrap();
            handler.cleanup().unwrap();

            let reports = log_records(&log)
                .into_iter()
                .filter(|record| {
                    matches!(
                        record["event"].as_str(),
                        Some("version_pdeathsig" | "mcp_pdeathsig")
                    )
                })
                .map(|record| record["signal"].as_i64().unwrap())
                .collect::<Vec<_>>();
            assert_eq!(reports, [9, 9]);
        });
    }

    #[test]
    fn direct_child_lifecycle_filters_refs_observes_semantics_and_cleans_up() {
        with_test_display(|| {
            let (_directory, log, mut handler) = handler("normal");
            let windows_call = call(COMPUTER_WINDOWS, json!({}));
            assert!(matches!(
                handler.policy_decision(&windows_call),
                PolicyDecision::RequireApproval { .. }
            ));
            let windows = handler
                .execute(
                    ToolCallId::new("call_windows").unwrap(),
                    COMPUTER_WINDOWS,
                    json!({}),
                )
                .unwrap();
            assert_eq!(windows.data["returned"], 1);
            assert_eq!(windows.data["total_visible"], 1);
            assert_eq!(windows.data["truncated"], false);
            assert_eq!(windows.data["windows"][0]["focused"], true);
            let window_ref = windows.data["windows"][0]["window_ref"]
                .as_str()
                .unwrap()
                .to_owned();
            assert!((1..=96).contains(&window_ref.len()));
            assert!(
                window_ref
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            );
            let serialized = windows.data.to_string();
            assert!(!serialized.contains("window_id"));
            assert!(!serialized.contains("\"pid\""));

            let observe_call = call(
                COMPUTER_OBSERVE,
                json!({"window_ref": window_ref, "max_elements": 2}),
            );
            assert!(matches!(
                handler.policy_decision(&observe_call),
                PolicyDecision::RequireApproval { .. }
            ));
            let preview = handler
                .approval_preview(COMPUTER_OBSERVE, &observe_call.input)
                .unwrap();
            assert!(preview.contains("app: Fixture"));
            assert!(preview.contains("title: Visible"));
            assert!(!preview.contains("101"));
            handler.approval_granted(&observe_call).unwrap();
            let observation = handler
                .execute(
                    ToolCallId::new("call_observe").unwrap(),
                    COMPUTER_OBSERVE,
                    observe_call.input.clone(),
                )
                .unwrap();
            assert_eq!(observation.data["returned"], 2);
            assert_eq!(observation.data["available"], 2);
            assert_eq!(observation.data["elements"][0]["role"], "button");
            assert_eq!(observation.data["elements"][0]["label"], "Save");
            assert_eq!(observation.data["elements"][0]["bounds"]["width"], 80);
            let serialized = observation.data.to_string();
            for forbidden in [
                "window_id",
                "\"pid\"",
                "element_index",
                "element_token",
                "snapshot_id",
                "screenshot",
                "image/png",
            ] {
                assert!(!serialized.contains(forbidden), "leaked {forbidden}");
            }
            assert!(matches!(
                handler.policy_decision(&observe_call),
                PolicyDecision::Allow
            ));
            handler
                .leases
                .get_mut(observe_call.input["window_ref"].as_str().unwrap())
                .unwrap()
                .last_used = Instant::now() - LEASE_IDLE - Duration::from_millis(1);
            assert!(matches!(
                handler.policy_decision(&observe_call),
                PolicyDecision::RequireApproval { .. }
            ));
            handler.approval_granted(&observe_call).unwrap();
            handler
                .leases
                .get_mut(observe_call.input["window_ref"].as_str().unwrap())
                .unwrap()
                .granted_at = Instant::now() - LEASE_ABSOLUTE - Duration::from_millis(1);
            assert!(matches!(
                handler.policy_decision(&observe_call),
                PolicyDecision::RequireApproval { .. }
            ));
            handler.approval_granted(&observe_call).unwrap();
            handler.approval_denied();
            assert!(matches!(
                handler.policy_decision(&observe_call),
                PolicyDecision::RequireApproval { .. }
            ));

            let replacement = handler
                .execute(
                    ToolCallId::new("call_windows_2").unwrap(),
                    COMPUTER_WINDOWS,
                    json!({}),
                )
                .unwrap();
            assert_ne!(
                replacement.data["windows"][0]["window_ref"],
                observe_call.input["window_ref"]
            );
            let error = handler
                .execute(
                    ToolCallId::new("call_stale").unwrap(),
                    COMPUTER_OBSERVE,
                    observe_call.input,
                )
                .unwrap_err();
            assert!(matches!(error, AppError::Tool(code) if code == "stale_window_ref"));

            handler.cleanup().unwrap();
            let records = log_records(&log);
            let start = &records[0];
            assert_eq!(start["argv"], json!(["mcp", "--direct"]));
            assert_eq!(start["permission_mode"], "standard");
            assert_eq!(
                start["env_names"],
                json!([
                    "CUA_DRIVER_PERMISSION_MODE",
                    "CUA_DRIVER_RS_TELEMETRY_ENABLED",
                    "CUA_DRIVER_RS_UPDATE_CHECK",
                    "DISPLAY",
                ])
            );
            let child_pid = start["pid"].as_u64().unwrap();
            assert!(!Path::new(&format!("/proc/{child_pid}")).exists());
            assert_eq!(records.last().unwrap()["event"], "stop");
            let requests = records
                .iter()
                .filter(|record| record["event"] == "request")
                .collect::<Vec<_>>();
            assert_eq!(requests[0]["request"]["method"], "initialize");
            assert_eq!(
                requests[1]["request"]["method"],
                "notifications/initialized"
            );
            let tool_names = requests
                .iter()
                .filter_map(|record| record["request"]["params"]["name"].as_str())
                .collect::<Vec<_>>();
            assert_eq!(
                tool_names,
                [
                    "list_windows",
                    "list_windows",
                    "get_window_state",
                    "list_windows"
                ]
            );
        });
    }

    #[test]
    fn protocol_images_wrong_ids_duplicates_cross_window_and_permission_fail_typed() {
        with_test_display(|| {
            for (mode, expected) in [
                ("image", "computer_unexpected_image"),
                ("wrong_id", "computer_protocol_error"),
                ("duplicate", "computer_protocol_error"),
                ("cross_pid", "computer_protocol_error"),
                ("cross_window", "computer_protocol_error"),
            ] {
                let (_directory, _log, mut handler) = handler(mode);
                let windows = handler.execute(
                    ToolCallId::new("call_windows").unwrap(),
                    COMPUTER_WINDOWS,
                    json!({}),
                );
                if matches!(mode, "wrong_id" | "duplicate") {
                    let error = windows.unwrap_err();
                    assert!(
                        matches!(error, AppError::Tool(code) if code == expected),
                        "{mode}"
                    );
                    continue;
                }
                let windows = windows.unwrap();
                let window_ref = windows.data["windows"][0]["window_ref"].clone();
                let error = handler
                    .execute(
                        ToolCallId::new("call_observe").unwrap(),
                        COMPUTER_OBSERVE,
                        json!({"window_ref": window_ref}),
                    )
                    .unwrap_err();
                assert!(matches!(error, AppError::Tool(code) if code == expected));
                assert!(handler.child.is_none(), "{mode}");
                assert!(handler.registry.is_empty(), "{mode}");
            }

            let (_directory, _log, mut handler) = handler("permission");
            let windows = handler
                .execute(
                    ToolCallId::new("call_windows").unwrap(),
                    COMPUTER_WINDOWS,
                    json!({}),
                )
                .unwrap();
            let error = handler
                .execute(
                    ToolCallId::new("call_observe").unwrap(),
                    COMPUTER_OBSERVE,
                    json!({"window_ref": windows.data["windows"][0]["window_ref"]}),
                )
                .unwrap_err();
            assert!(matches!(
                error,
                AppError::Tool(code) if code == "computer_permission_denied"
            ));
        });
    }

    #[test]
    fn lifecycle_failures_are_bounded_typed_and_reaped() {
        with_test_display(|| {
            let directory = tempfile::tempdir().unwrap();
            let log = directory.path().join("prelaunch.jsonl");
            let executable = write_driver(directory.path(), "normal", &log);
            let canceled = Arc::new(AtomicBool::new(true));
            let error = ComputerToolHandler::build(
                &ComputerConfig {
                    executable: Some(executable),
                },
                Some(canceled),
                display(),
            )
            .unwrap_err();
            assert_eq!(error, ComputerError::Canceled);
            assert!(!log.exists());

            for (mode, expected) in [
                ("oversize", "computer_output_too_large"),
                ("stderr_overflow", "computer_output_too_large"),
                ("partial", "computer_protocol_error"),
                ("crash", "computer_protocol_error"),
                ("timeout", "computer_timeout"),
            ] {
                let (_directory, log, mut handler) = handler(mode);
                let error = handler
                    .execute(
                        ToolCallId::new("call_failure").unwrap(),
                        COMPUTER_WINDOWS,
                        json!({}),
                    )
                    .unwrap_err();
                assert!(
                    matches!(error, AppError::Tool(code) if code == expected),
                    "{mode}"
                );
                handler.cleanup().unwrap();
                let records = log_records(&log);
                let start = &records[0];
                assert!(!Path::new(&format!("/proc/{}", start["pid"].as_u64().unwrap())).exists());
            }

            let cancel = Arc::new(AtomicBool::new(false));
            let (_directory, _log, mut canceled_handler) =
                handler_with_cancel("timeout", Some(Arc::clone(&cancel)));
            let canceler = thread::spawn(move || {
                thread::sleep(Duration::from_millis(50));
                cancel.store(true, Ordering::SeqCst);
            });
            let error = canceled_handler
                .execute(
                    ToolCallId::new("call_canceled").unwrap(),
                    COMPUTER_WINDOWS,
                    json!({}),
                )
                .unwrap_err();
            canceler.join().unwrap();
            assert!(matches!(
                error,
                AppError::Tool(code) if code == "computer_canceled"
            ));
            canceled_handler.cleanup().unwrap();

            let (_directory, log, mut shutdown_handler) = handler("shutdown_refusal");
            shutdown_handler
                .execute(
                    ToolCallId::new("call_windows").unwrap(),
                    COMPUTER_WINDOWS,
                    json!({}),
                )
                .unwrap();
            let cleanup_started = Instant::now();
            shutdown_handler.cleanup().unwrap();
            assert!(cleanup_started.elapsed() < Duration::from_secs(1));
            let start = &log_records(&log)[0];
            assert!(!Path::new(&format!("/proc/{}", start["pid"].as_u64().unwrap())).exists());

            let (_directory, log, mut pipe_handler) = handler("cleanup_pipe_holder");
            pipe_handler
                .execute(
                    ToolCallId::new("call_pipe_holder").unwrap(),
                    COMPUTER_WINDOWS,
                    json!({}),
                )
                .unwrap();
            let cleanup_started = Instant::now();
            pipe_handler.cleanup().unwrap();
            assert!(cleanup_started.elapsed() < CLEANUP_TIMEOUT);
            let records = log_records(&log);
            let descendant = records
                .iter()
                .find(|record| record["event"] == "cleanup_descendant")
                .unwrap()["pid"]
                .as_u64()
                .unwrap();
            let process = PathBuf::from(format!("/proc/{descendant}"));
            let deadline = Instant::now() + Duration::from_secs(1);
            while process.exists() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(10));
            }
            assert!(!process.exists(), "cleanup descendant was not reaped");
        });
    }

    #[test]
    fn strict_normalization_bounds_strings_counts_and_visual_data() {
        let long = format!("{}x", "界".repeat(600));
        let structured = json!({
            "window_id": 1,
            "pid": 2,
            "element_count": 2,
            "total_element_count": 5,
            "returned_element_count": 2,
            "elements": [
                {"element_index": 1, "role": long, "label": long, "depth": 1},
                {"element_index": 2, "role": "button", "depth": 2}
            ]
        });
        let normalized = normalize_observation(structured.clone(), 1, 2, 1).unwrap();
        assert_eq!(normalized.values.len(), 1);
        assert_eq!(normalized.available, 5);
        assert!(normalized.values[0]["role"].as_str().unwrap().len() <= 1024);
        assert!(
            normalized.values[0]["role"]
                .as_str()
                .unwrap()
                .is_char_boundary(normalized.values[0]["role"].as_str().unwrap().len())
        );
        assert!(matches!(
            normalize_observation(structured.clone(), 1, 3, 1),
            Err(ComputerError::Protocol)
        ));
        assert!(matches!(
            normalize_observation(structured, 1, 2, 9),
            Err(ComputerError::Protocol)
        ));

        assert_eq!(
            parse_tool_result(json!({
                "content": [{"type": "text", "text": "ok"}],
                "structuredContent": {"screenshot_file_path": "/secret"}
            }))
            .unwrap_err(),
            ComputerError::UnexpectedImage
        );
        assert_eq!(
            parse_response_frame(br#"{"jsonrpc":"2.0","id":2,"result":{},"unknown":true}"#, 2)
                .unwrap_err(),
            ComputerError::Protocol
        );
    }

    #[test]
    #[ignore = "requires an operator-owned X11/XWayland session and exact Cua Driver 0.19.3"]
    fn real_pinned_linux_desktop_observation_is_screenshot_free() {
        let executable =
            PathBuf::from(env::var_os("PLATONIC_CUA_DRIVER").expect("set PLATONIC_CUA_DRIVER"));
        let title = format!("Platonic 561 fixture {}", std::process::id());
        let mut handler = ComputerToolHandler::new(
            &ComputerConfig {
                executable: Some(executable),
            },
            None,
        )
        .unwrap();
        let _fixture = ChildGuard(
            Command::new("zenity")
                .env("GDK_BACKEND", "x11")
                .args([
                    "--info",
                    "--no-wrap",
                    &format!("--title={title}"),
                    "--text=Read-only semantic observation fixture",
                ])
                .spawn()
                .expect("zenity is required for the owned native fixture"),
        );
        let proof = (|| -> AppResult<()> {
            let deadline = Instant::now() + Duration::from_secs(8);
            let window_ref = loop {
                let result = handler.execute(
                    ToolCallId::new("call_native_windows").unwrap(),
                    COMPUTER_WINDOWS,
                    json!({}),
                )?;
                assert!(
                    !result
                        .data
                        .to_string()
                        .to_ascii_lowercase()
                        .contains("screenshot")
                );
                if let Some(window_ref) = result.data["windows"].as_array().and_then(|windows| {
                    windows.iter().find_map(|window| {
                        window["title"]
                            .as_str()
                            .is_some_and(|candidate| candidate.contains(&title))
                            .then(|| window["window_ref"].as_str().map(str::to_owned))
                            .flatten()
                    })
                }) {
                    break window_ref;
                }
                assert!(
                    Instant::now() < deadline,
                    "owned fixture window was not listed"
                );
                thread::sleep(Duration::from_millis(100));
            };
            let observation = handler.execute(
                ToolCallId::new("call_native_observe").unwrap(),
                COMPUTER_OBSERVE,
                json!({"window_ref": window_ref}),
            )?;
            let output = observation.data.to_string().to_ascii_lowercase();
            for forbidden in ["screenshot", "image/", "window_id", "\"pid\""] {
                assert!(
                    !output.contains(forbidden),
                    "native output leaked {forbidden}"
                );
            }
            Ok(())
        })();
        let cleanup = handler.cleanup();
        proof.unwrap();
        cleanup.unwrap();
    }
}
