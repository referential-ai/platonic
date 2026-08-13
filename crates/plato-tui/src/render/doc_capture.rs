use super::render_overlay_at;
use crate::{
    ActiveRunView, ApprovalModalView, TranscriptState, TuiState,
    color::{self, ColorCapability, TerminalColors},
};
use platonic_protocol::{
    DaemonStatusResult, HelloResult, RunStateName, SessionSummary, TranscriptReadResult, TypedRun,
    TypedTranscript, TypedTranscriptEntry,
};
use ratatui::{
    Terminal,
    backend::TestBackend,
    buffer::{Buffer, Cell},
    style::{Color, Modifier},
};
use serde_json::{Value, json};
use std::{
    env, fs,
    io::{self, Write},
    path::Path,
    process::{Command, Stdio},
};

const COLUMNS: u16 = 100;
const ROWS: u16 = 24;
const CELL_WIDTH: u16 = 10;
const CELL_HEIGHT: u16 = 20;
const PADDING: u16 = 20;
const IMAGE_WIDTH: u16 = COLUMNS * CELL_WIDTH + PADDING * 2;
const IMAGE_HEIGHT: u16 = ROWS * CELL_HEIGHT + PADDING * 2;
const FIXED_NOW_MS: u64 = 1_786_536_000_000;
const FIXTURE_REVISION: &str = "tui-docs-v1";
const BACKGROUND: (u8, u8, u8) = (11, 15, 20);
const FOREGROUND: (u8, u8, u8) = (230, 237, 243);
const CAPTURE_COMMAND: &str = "./scripts/capture-tui-docs.sh";

const FORBIDDEN: [&str; 12] = [
    "/home/",
    "/Users/",
    "alanwilhelm",
    "jerome",
    "protonmail",
    "OPENROUTER_API_KEY=",
    "Authorization:",
    "Bearer ",
    "ghp_",
    "github_pat_",
    "sk-",
    "@referential.ai",
];

struct Scene {
    id: &'static str,
    issue: u16,
    task: &'static str,
    consuming_path: &'static str,
    expected: &'static [&'static str],
    state: TuiState,
}

struct RenderedScene {
    id: &'static str,
    issue: u16,
    task: &'static str,
    consuming_path: &'static str,
    text: String,
    svg: String,
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct SvgStyle {
    foreground: (u8, u8, u8),
    background: (u8, u8, u8),
    modifiers: Modifier,
}

#[test]
fn documentation_scenes_are_deterministic_and_sanitized() {
    let first = render_scenes().unwrap();
    let second = render_scenes().unwrap();

    assert_eq!(first.len(), 5);
    for (left, right) in first.iter().zip(&second) {
        assert_eq!(left.id, right.id);
        assert_eq!(left.text, right.text);
        assert_eq!(left.svg, right.svg);
        assert!(left.svg.len() < 128 * 1024, "{} is too large", left.id);
        assert!(left.svg.contains("width=\"1040\" height=\"520\""));
        assert!(!left.svg.contains("<metadata"));
        assert!(!left.svg.contains("<!--"));
        assert!(!left.svg.contains("animation"));
        assert_sanitized(&left.text);
        assert_sanitized(&left.svg);
    }

    let manifest = manifest_json(&"0".repeat(40), &"0".repeat(64), &first, |_| "0".repeat(64));
    let manifest: Value = serde_json::from_str(&manifest).unwrap();
    assert_eq!(manifest["fixture_revision"], FIXTURE_REVISION);
    assert_eq!(manifest["geometry"]["columns"], COLUMNS);
    assert_eq!(manifest["geometry"]["rows"], ROWS);
    assert_eq!(manifest["scenes"].as_array().unwrap().len(), 5);
}

#[test]
#[ignore = "writes the issue-owned documentation assets"]
fn write_documentation_assets() {
    let output_dir = env::var("PLATO_TUI_DOC_OUTPUT_DIR")
        .expect("PLATO_TUI_DOC_OUTPUT_DIR is set by scripts/capture-tui-docs.sh");
    let source_commit = env::var("PLATO_TUI_DOC_SOURCE_COMMIT")
        .expect("PLATO_TUI_DOC_SOURCE_COMMIT is set by scripts/capture-tui-docs.sh");
    let binary = env::var("PLATO_TUI_DOC_BINARY")
        .expect("PLATO_TUI_DOC_BINARY is set by scripts/capture-tui-docs.sh");
    assert_hex(&source_commit, 40, "source commit");

    let binary_sha256 = sha256(&fs::read(&binary).expect("read built plato-tui binary"));
    let scenes = render_scenes().unwrap();
    fs::create_dir_all(&output_dir).unwrap();
    for scene in &scenes {
        fs::write(
            Path::new(&output_dir).join(format!("{}.svg", scene.id)),
            &scene.svg,
        )
        .unwrap();
    }
    fs::write(
        Path::new(&output_dir).join("manifest.json"),
        format!(
            "{}\n",
            manifest_json(&source_commit, &binary_sha256, &scenes, sha256)
        ),
    )
    .unwrap();

    eprintln!("source commit: {source_commit}");
    eprintln!("plato-tui binary sha256: {binary_sha256}");
    for scene in scenes {
        eprintln!(
            "{} {} bytes {}",
            scene.id,
            scene.svg.len(),
            sha256(scene.svg.as_bytes())
        );
    }
}

fn render_scenes() -> io::Result<Vec<RenderedScene>> {
    scenes()
        .into_iter()
        .map(|scene| {
            let buffer = color::with_test_colors(
                TerminalColors::forced(ColorCapability::TrueColor, Some(BACKGROUND)),
                || render_buffer(&scene.state),
            )?;
            assert_eq!(buffer.area.width, COLUMNS);
            assert_eq!(buffer.area.height, ROWS);
            let text = buffer_text(&buffer);
            for expected in scene.expected {
                assert!(
                    text.contains(expected),
                    "{} missing {expected:?}\n{text}",
                    scene.id
                );
            }
            Ok(RenderedScene {
                id: scene.id,
                issue: scene.issue,
                task: scene.task,
                consuming_path: scene.consuming_path,
                text,
                svg: buffer_svg(&buffer),
            })
        })
        .collect()
}

fn render_buffer(state: &TuiState) -> io::Result<Buffer> {
    let backend = TestBackend::new(COLUMNS, ROWS);
    let mut terminal = Terminal::new(backend)?;
    terminal.draw(|frame| render_overlay_at(frame, state, 0, FIXED_NOW_MS))?;
    Ok(terminal.backend().buffer().clone())
}

fn scenes() -> Vec<Scene> {
    vec![
        Scene {
            id: "first-useful-thread",
            issue: 546,
            task: "Complete one read-only task",
            consuming_path: "docs-site/src/content/docs/user/first-run.md",
            expected: &[
                "Use only file.read",
                "First-run workspace",
                "harmless, read-only",
            ],
            state: first_useful_thread(),
        },
        Scene {
            id: "thread-status",
            issue: 548,
            task: "Read live state",
            consuming_path: "docs-site/src/content/docs/user/operations/tui-and-cli.md",
            expected: &["requested alias", "~openai/gpt-latest", "session-docs-001"],
            state: thread_status(),
        },
        Scene {
            id: "approval",
            issue: 548,
            task: "Decide from the proposed effect",
            consuming_path: "docs-site/src/content/docs/user/operations/approvals.md",
            expected: &["Approval", "file.write", "workspace_write", "notes.txt"],
            state: approval(),
        },
        Scene {
            id: "replay-audit",
            issue: 548,
            task: "Read durable history",
            consuming_path: "docs-site/src/content/docs/user/operations/history-and-recovery.md",
            expected: &["run run-docs-001", "file.read", "README.md read"],
            state: replay_audit(),
        },
        Scene {
            id: "daemon-recovery",
            issue: 548,
            task: "Reconnect a TUI",
            consuming_path: "docs-site/src/content/docs/user/operations/history-and-recovery.md",
            expected: &["daemon unavailable", "connection refused", "r to reconnect"],
            state: daemon_recovery(),
        },
    ]
}

fn first_useful_thread() -> TuiState {
    connected_state(finished_transcript())
}

fn thread_status() -> TuiState {
    let mut state = connected_state(finished_transcript());
    state.status_modal = Some(
        serde_json::from_value::<DaemonStatusResult>(json!({
            "model": {
                "requested_alias": "~openai/gpt-latest",
                "served_model": null,
                "provider_kind": "open_router",
                "key_present": true
            },
            "daemon": {
                "package_version": "0.2.0",
                "build_commit": null,
                "build_date_utc": null,
                "uptime_ms": 42000,
                "endpoint_path": "/run/platonic/host/agent.sock",
                "workspace_id": "work-docs"
            },
            "session": {
                "session_id": "session-docs-001",
                "latest_run_id": "run-docs-001",
                "human_turn_count": 1,
                "ledger_path": "/state/platonic/workspaces/work-docs/ledger.db",
                "core_event_count": 9
            },
            "usage": {
                "last_run": { "input_tokens": 48, "output_tokens": 19, "unknown_response_count": 0 },
                "session": { "input_tokens": 48, "output_tokens": 19, "unknown_response_count": 0 }
            },
            "trust": {
                "approval_granted_count": 0,
                "approval_denied_count": 0,
                "shell_session_grant": false,
                "approval_profile": "prompt"
            }
        }))
        .unwrap(),
    );
    state
}

fn approval() -> TuiState {
    let prompt = "Create notes.txt containing the workspace summary.";
    let mut state = connected_state(transcript(
        "run-docs-002",
        RunStateName::Running,
        format!("run_id: run-docs-002\n[turn-docs-002] user: {prompt}\n"),
        vec![TypedTranscriptEntry::User {
            text: prompt.into(),
        }],
        None,
    ));
    state.active_run = Some(ActiveRunView::new(
        "run-docs-002".into(),
        RunStateName::Running,
    ));
    state.active_run_elapsed_secs = Some(12);
    state.approval = Some(ApprovalModalView {
        run_id: "run-docs-002".into(),
        tool_call_id: "call-docs-write".into(),
        tool_name: "file.write".into(),
        effect: "workspace_write".into(),
        reason: "file.write requires approval".into(),
        input_preview: "{\n  \"path\": \"notes.txt\",\n  \"content\": \"First-run notes\\n\"\n}"
            .into(),
        approval_preview: None,
        diff_preview: None,
    });
    state
}

fn replay_audit() -> TuiState {
    let mut state = connected_state(finished_transcript());
    state.toggle_display_mode();
    state
}

fn daemon_recovery() -> TuiState {
    let mut state = TuiState::disconnected(
        "/workspace/first-run".into(),
        "/run/platonic/host/agent.sock".into(),
        "connection refused at configured host endpoint".into(),
    );
    state.set_reduced_motion(true);
    state
}

fn finished_transcript() -> TranscriptReadResult {
    let prompt = "Use only file.read to read README.md. Reply with its heading and purpose. Do not change files.";
    let answer = "# First-run workspace\n\nThis repository is a harmless, read-only target for the Plato Agent first run.";
    transcript(
        "run-docs-001",
        RunStateName::Finished,
        format!(
            "run_id: run-docs-001\n[turn-docs-001] user: {prompt}\n[turn-docs-001] tool_call call-docs-read file.read {{\"path\":\"README.md\"}}\ntool_result call-docs-read README.md read\n[turn-docs-001] assistant: {answer}\n"
        ),
        vec![
            TypedTranscriptEntry::User {
                text: prompt.into(),
            },
            TypedTranscriptEntry::ToolCall {
                call_id: "call-docs-read".into(),
                tool: "file.read".into(),
                input: json!({"path": "README.md"}),
            },
            TypedTranscriptEntry::ToolResult {
                call_id: "call-docs-read".into(),
                summary: "README.md read".into(),
            },
            TypedTranscriptEntry::Assistant {
                text: answer.into(),
            },
        ],
        Some(answer.into()),
    )
}

fn transcript(
    run_id: &str,
    status: RunStateName,
    content: String,
    entries: Vec<TypedTranscriptEntry>,
    final_answer: Option<String>,
) -> TranscriptReadResult {
    TranscriptReadResult {
        run_id: run_id.into(),
        status,
        final_answer,
        transcript: content,
        typed: Some(TypedTranscript {
            runs: vec![TypedRun {
                run_id: run_id.into(),
                session_index: 0,
                status,
                model_status: None,
                entries,
            }],
        }),
        pending_approval: None,
        completion_claim: None,
    }
}

fn connected_state(transcript: TranscriptReadResult) -> TuiState {
    let run_id = transcript.run_id.clone();
    let status = transcript.status;
    let mut state = TuiState::connected(
        "/workspace/first-run".into(),
        "/run/platonic/host/agent.sock".into(),
        HelloResult {
            daemon_version: "0.2.0 unknown unknown".into(),
            workspace_id: "work-docs".into(),
            ledger_path: "/state/platonic/workspaces/work-docs/ledger.db".into(),
            capabilities: vec![],
            daemon_scope: None,
        },
        vec![SessionSummary {
            session_id: "session-docs-001".into(),
            run_id,
            status,
            latest_question: "Read README.md".into(),
            first_question: "Read README.md".into(),
            updated_at_ms: FIXED_NOW_MS,
            ledger_path: "/state/platonic/workspaces/work-docs/ledger.db".into(),
        }],
        TranscriptState::Loaded(transcript.into()),
    );
    state.selected_thread_id = Some("thread-docs-001".into());
    state.set_reduced_motion(true);
    state
}

// ponytail: SVG preserves styled terminal cells without adding a raster or font dependency.
fn buffer_svg(buffer: &Buffer) -> String {
    let mut svg = format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{IMAGE_WIDTH}\" height=\"{IMAGE_HEIGHT}\" viewBox=\"0 0 {IMAGE_WIDTH} {IMAGE_HEIGHT}\" preserveAspectRatio=\"xMinYMin meet\">\n<rect width=\"{IMAGE_WIDTH}\" height=\"{IMAGE_HEIGHT}\" fill=\"{}\"/>\n<g font-family=\"DejaVu Sans Mono, Liberation Mono, monospace\" font-size=\"16\" font-variant-ligatures=\"none\">\n",
        hex(BACKGROUND)
    );

    for y in 0..ROWS {
        let mut x = 0;
        while x < COLUMNS {
            let background = svg_style(&buffer[(x, y)]).background;
            let start = x;
            x += 1;
            while x < COLUMNS && svg_style(&buffer[(x, y)]).background == background {
                x += 1;
            }
            if background != BACKGROUND {
                svg.push_str(&format!(
                    "<rect x=\"{}\" y=\"{}\" width=\"{}\" height=\"{CELL_HEIGHT}\" fill=\"{}\"/>\n",
                    PADDING + start * CELL_WIDTH,
                    PADDING + y * CELL_HEIGHT,
                    (x - start) * CELL_WIDTH,
                    hex(background)
                ));
            }
        }
    }

    for y in 0..ROWS {
        let mut x = 0;
        while x < COLUMNS {
            let style = svg_style(&buffer[(x, y)]);
            let start = x;
            x += 1;
            while x < COLUMNS && svg_style(&buffer[(x, y)]) == style {
                x += 1;
            }
            let mut end = x;
            while end > start && cell_text(&buffer[(end - 1, y)], style).trim().is_empty() {
                end -= 1;
            }
            if end == start {
                continue;
            }
            let text = (start..end)
                .map(|column| cell_text(&buffer[(column, y)], style))
                .collect::<String>();
            let decoration = match (
                style.modifiers.contains(Modifier::UNDERLINED),
                style.modifiers.contains(Modifier::CROSSED_OUT),
            ) {
                (true, true) => " text-decoration=\"underline line-through\"",
                (true, false) => " text-decoration=\"underline\"",
                (false, true) => " text-decoration=\"line-through\"",
                (false, false) => "",
            };
            svg.push_str(&format!(
                "<text x=\"{}\" y=\"{}\" textLength=\"{}\" lengthAdjust=\"spacingAndGlyphs\" fill=\"{}\" font-weight=\"{}\" font-style=\"{}\" opacity=\"{}\"{decoration} xml:space=\"preserve\">{}</text>\n",
                PADDING + start * CELL_WIDTH,
                PADDING + y * CELL_HEIGHT + 16,
                (end - start) * CELL_WIDTH,
                hex(style.foreground),
                if style.modifiers.contains(Modifier::BOLD) { "700" } else { "400" },
                if style.modifiers.contains(Modifier::ITALIC) { "italic" } else { "normal" },
                if style.modifiers.contains(Modifier::DIM) { "0.64" } else { "1" },
                escape_xml(&text)
            ));
        }
    }
    svg.push_str("</g>\n</svg>\n");
    svg
}

fn cell_text(cell: &Cell, style: SvgStyle) -> String {
    if style.modifiers.contains(Modifier::HIDDEN) || cell.symbol().is_empty() {
        " ".into()
    } else {
        cell.symbol().into()
    }
}

fn svg_style(cell: &Cell) -> SvgStyle {
    let mut foreground = color_rgb(cell.fg, FOREGROUND);
    let mut background = color_rgb(cell.bg, BACKGROUND);
    if cell.modifier.contains(Modifier::REVERSED) {
        std::mem::swap(&mut foreground, &mut background);
    }
    SvgStyle {
        foreground,
        background,
        modifiers: cell.modifier,
    }
}

fn color_rgb(color: Color, reset: (u8, u8, u8)) -> (u8, u8, u8) {
    match color {
        Color::Reset => reset,
        Color::Yellow => (229, 229, 16),
        Color::Rgb(red, green, blue) => (red, green, blue),
        other => panic!("fixed true-color capture emitted {other:?}"),
    }
}

fn buffer_text(buffer: &Buffer) -> String {
    let mut output = String::new();
    for y in 0..ROWS {
        let line = (0..COLUMNS)
            .map(|x| buffer[(x, y)].symbol())
            .collect::<String>();
        output.push_str(line.trim_end());
        output.push('\n');
    }
    output
}

fn manifest_json(
    source_commit: &str,
    binary_sha256: &str,
    scenes: &[RenderedScene],
    digest: impl Fn(&[u8]) -> String,
) -> String {
    let scenes = scenes
        .iter()
        .map(|scene| {
            json!({
                "id": scene.id,
                "file": format!("{}.svg", scene.id),
                "issue": format!("#{}", scene.issue),
                "task": scene.task,
                "consuming_path": scene.consuming_path,
                "bytes": scene.svg.len(),
                "sha256": digest(scene.svg.as_bytes()),
                "semantic_sha256": digest(scene.text.as_bytes())
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string_pretty(&json!({
        "schema_version": 1,
        "fixture_revision": FIXTURE_REVISION,
        "source_commit": source_commit,
        "plato_tui_binary_sha256": binary_sha256,
        "capture_command": CAPTURE_COMMAND,
        "geometry": {
            "columns": COLUMNS,
            "rows": ROWS,
            "width_px": IMAGE_WIDTH,
            "height_px": IMAGE_HEIGHT,
            "cell_width_px": CELL_WIDTH,
            "cell_height_px": CELL_HEIGHT
        },
        "theme": {
            "name": "plato-docs-dark-v1",
            "color_capability": "truecolor",
            "background": hex(BACKGROUND),
            "foreground": hex(FOREGROUND),
            "accent": "#00ffff",
            "primary": "#7dd3fc",
            "warning": "#facc15",
            "error": "#f87171",
            "success": "#4ade80",
            "muted": "#94a3b8",
            "syntax_heading": "#e5e510",
            "font_family": "DejaVu Sans Mono, Liberation Mono, monospace"
        },
        "motion": "reduced",
        "clock": {
            "unix_ms": FIXED_NOW_MS,
            "utc": "2026-08-12T12:00:00Z"
        },
        "identities": {
            "workspace": "work-docs",
            "thread": "thread-docs-001",
            "session": "session-docs-001",
            "runs": ["run-docs-001", "run-docs-002"]
        },
        "encoding_variance": "none for UTF-8 SVG bytes; proof rasterization may vary by installed font rasterizer",
        "metadata": "no SVG metadata or comments",
        "scenes": scenes
    }))
    .unwrap()
}

fn sha256(bytes: &[u8]) -> String {
    let mut command = Command::new("sha256sum");
    command.stdin(Stdio::piped()).stdout(Stdio::piped());
    let mut child = command
        .spawn()
        .or_else(|error| {
            if error.kind() != io::ErrorKind::NotFound {
                return Err(error);
            }
            let mut fallback = Command::new("shasum");
            fallback
                .args(["-a", "256"])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
        })
        .expect("sha256sum or shasum is available");
    child.stdin.take().unwrap().write_all(bytes).unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "SHA-256 command failed");
    let digest = String::from_utf8(output.stdout).unwrap();
    let digest = digest.split_whitespace().next().unwrap().to_owned();
    assert_hex(&digest, 64, "SHA-256 digest");
    digest
}

fn assert_sanitized(value: &str) {
    for forbidden in FORBIDDEN {
        assert!(
            !value
                .to_ascii_lowercase()
                .contains(&forbidden.to_ascii_lowercase()),
            "capture contains forbidden text {forbidden:?}"
        );
    }
}

fn assert_hex(value: &str, length: usize, label: &str) {
    assert_eq!(value.len(), length, "{label} has the wrong length");
    assert!(
        value.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "{label} is not hexadecimal"
    );
}

fn hex((red, green, blue): (u8, u8, u8)) -> String {
    format!("#{red:02x}{green:02x}{blue:02x}")
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
