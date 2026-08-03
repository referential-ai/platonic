use ratatui::style::{Color, Modifier, Style};
use std::{env, ffi::OsString, sync::OnceLock};

#[cfg(any(
    test,
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "netbsd",
    target_os = "openbsd"
))]
use std::{
    io::{self, Read},
    time::{Duration, Instant},
};

pub(crate) type Rgb = (u8, u8, u8);

#[cfg(any(
    test,
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "netbsd",
    target_os = "openbsd"
))]
pub(crate) const OSC_11_TIMEOUT: Duration = Duration::from_millis(100);
const LIGHT_BG_ACCENT: Rgb = (0, 95, 135);
const DARK_BG_ACCENT: Rgb = (0, 255, 255);
const XTERM_CUBE_LEVELS: [u8; 6] = [0, 95, 135, 175, 215, 255];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ColorCapability {
    TrueColor,
    Ansi256,
    Ansi16,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SemanticRole {
    Primary,
    Error,
    Warning,
    Success,
    Muted,
    Border,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ColorEnvironment {
    no_color: Option<OsString>,
    colorterm: Option<OsString>,
    term: Option<OsString>,
    wt_session: Option<OsString>,
    term_program: Option<OsString>,
}

impl ColorEnvironment {
    fn process() -> Self {
        Self {
            no_color: env::var_os("NO_COLOR"),
            colorterm: env::var_os("COLORTERM"),
            term: env::var_os("TERM"),
            wt_session: env::var_os("WT_SESSION"),
            term_program: env::var_os("TERM_PROGRAM"),
        }
    }

    fn no_color(&self) -> bool {
        self.no_color
            .as_ref()
            .is_some_and(|value| !value.is_empty())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TerminalColors {
    capability: ColorCapability,
    background: Option<Rgb>,
    no_color: bool,
}

impl TerminalColors {
    pub(crate) fn detect(background: Option<Rgb>) -> Self {
        Self::from_environment(&ColorEnvironment::process(), background)
    }

    fn from_environment(environment: &ColorEnvironment, background: Option<Rgb>) -> Self {
        Self {
            capability: capability_from_environment(environment),
            background,
            no_color: environment.no_color(),
        }
    }

    #[cfg(test)]
    pub(crate) fn forced(capability: ColorCapability, background: Option<Rgb>) -> Self {
        Self {
            capability,
            background,
            no_color: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn forced_no_color(capability: ColorCapability, background: Option<Rgb>) -> Self {
        Self {
            capability,
            background,
            no_color: true,
        }
    }

    pub(crate) fn should_probe_background(self) -> bool {
        !self.no_color
            && matches!(
                self.capability,
                ColorCapability::TrueColor | ColorCapability::Ansi256
            )
    }

    pub(crate) fn best_color(self, target: Rgb) -> Option<Color> {
        if self.no_color {
            return None;
        }
        match self.capability {
            ColorCapability::TrueColor => Some(Color::Rgb(target.0, target.1, target.2)),
            ColorCapability::Ansi256 => Some(Color::Indexed(nearest_xterm_256(target))),
            ColorCapability::Ansi16 | ColorCapability::Unknown => None,
        }
    }

    pub(crate) fn semantic_style(self, role: SemanticRole) -> Style {
        self.best_color(semantic_rgb(role, self.background.is_some_and(is_light)))
            .map_or_else(
                || Style::default().add_modifier(Modifier::DIM),
                |color| Style::default().fg(color),
            )
    }

    pub(crate) fn accent_style(self) -> Style {
        let target = if self.background.is_some_and(is_light) {
            LIGHT_BG_ACCENT
        } else {
            DARK_BG_ACCENT
        };
        self.best_color(target)
            .map_or_else(Style::default, |color| Style::default().fg(color))
            .add_modifier(Modifier::BOLD)
    }

    pub(crate) fn user_message_style(self) -> Style {
        let Some(background) = self.background else {
            return Style::default();
        };
        self.best_color(user_message_tint(background))
            .map_or_else(Style::default, |color| Style::default().bg(color))
    }
}

static TERMINAL_COLORS: OnceLock<TerminalColors> = OnceLock::new();

pub(crate) fn install(colors: TerminalColors) {
    if colors.no_color {
        crossterm::style::force_color_output(false);
    }
    let _ = TERMINAL_COLORS.set(colors);
}

pub(crate) fn active() -> TerminalColors {
    #[cfg(test)]
    if let Some(colors) = TEST_COLORS.with(Cell::get) {
        return colors;
    }
    *TERMINAL_COLORS.get_or_init(|| TerminalColors::detect(None))
}

pub(crate) fn is_light(bg: Rgb) -> bool {
    let (r, g, b) = bg;
    let luma = 0.299 * r as f32 + 0.587 * g as f32 + 0.114 * b as f32;
    luma > 128.0
}

pub(crate) fn blend(fg: Rgb, bg: Rgb, alpha: f32) -> Rgb {
    let r = (fg.0 as f32 * alpha + bg.0 as f32 * (1.0 - alpha)) as u8;
    let g = (fg.1 as f32 * alpha + bg.1 as f32 * (1.0 - alpha)) as u8;
    let b = (fg.2 as f32 * alpha + bg.2 as f32 * (1.0 - alpha)) as u8;
    (r, g, b)
}

pub(crate) fn perceptual_distance(a: Rgb, b: Rgb) -> f32 {
    let (l1, a1, b1) = rgb_to_lab(a);
    let (l2, a2, b2) = rgb_to_lab(b);
    let dl = l1 - l2;
    let da = a1 - a2;
    let db = b1 - b2;
    (dl * dl + da * da + db * db).sqrt()
}

fn rgb_to_lab((r, g, b): Rgb) -> (f32, f32, f32) {
    fn linear(component: u8) -> f32 {
        let component = component as f32 / 255.0;
        if component <= 0.04045 {
            component / 12.92
        } else {
            ((component + 0.055) / 1.055).powf(2.4)
        }
    }

    fn lab_component(component: f32) -> f32 {
        if component > 0.008856 {
            component.powf(1.0 / 3.0)
        } else {
            7.787 * component + 16.0 / 116.0
        }
    }

    let r = linear(r);
    let g = linear(g);
    let b = linear(b);
    let x = (r * 0.4124 + g * 0.3576 + b * 0.1805) / 0.95047;
    let y = r * 0.2126 + g * 0.7152 + b * 0.0722;
    let z = (r * 0.0193 + g * 0.1192 + b * 0.9505) / 1.08883;
    let x = lab_component(x);
    let y = lab_component(y);
    let z = lab_component(z);
    (116.0 * y - 16.0, 500.0 * (x - y), 200.0 * (y - z))
}

fn capability_from_environment(environment: &ColorEnvironment) -> ColorCapability {
    let colorterm = environment
        .colorterm
        .as_deref()
        .map(|value| value.to_string_lossy().to_ascii_lowercase());
    let term = environment
        .term
        .as_deref()
        .map(|value| value.to_string_lossy().to_ascii_lowercase());
    let windows_terminal = environment.wt_session.is_some()
        || environment.term_program.as_deref().is_some_and(|value| {
            value
                .to_string_lossy()
                .eq_ignore_ascii_case("windows_terminal")
        });

    if windows_terminal
        || colorterm
            .as_deref()
            .is_some_and(|value| matches!(value, "truecolor" | "24bit"))
    {
        ColorCapability::TrueColor
    } else if term
        .as_deref()
        .is_some_and(|value| value.contains("256color"))
    {
        ColorCapability::Ansi256
    } else if term
        .as_deref()
        .is_some_and(|value| !value.is_empty() && value != "dumb")
        || colorterm.as_deref().is_some_and(|value| !value.is_empty())
    {
        ColorCapability::Ansi16
    } else {
        ColorCapability::Unknown
    }
}

fn semantic_rgb(role: SemanticRole, light_background: bool) -> Rgb {
    match (role, light_background) {
        (SemanticRole::Primary, false) => (125, 211, 252),
        (SemanticRole::Primary, true) => LIGHT_BG_ACCENT,
        (SemanticRole::Error, false) => (248, 113, 113),
        (SemanticRole::Error, true) => (185, 28, 28),
        (SemanticRole::Warning, false) => (250, 204, 21),
        (SemanticRole::Warning, true) => (161, 98, 7),
        (SemanticRole::Success, false) => (74, 222, 128),
        (SemanticRole::Success, true) => (21, 128, 61),
        (SemanticRole::Muted, false) => (148, 163, 184),
        (SemanticRole::Muted, true) => (71, 85, 105),
        (SemanticRole::Border, false) => (71, 85, 105),
        (SemanticRole::Border, true) => (148, 163, 184),
    }
}

fn user_message_tint(background: Rgb) -> Rgb {
    let (foreground, alpha) = if is_light(background) {
        ((0, 0, 0), 0.04)
    } else {
        ((255, 255, 255), 0.12)
    };
    blend(foreground, background, alpha)
}

fn nearest_xterm_256(target: Rgb) -> u8 {
    xterm_fixed_colors()
        .min_by(|(_, left), (_, right)| {
            perceptual_distance(*left, target)
                .partial_cmp(&perceptual_distance(*right, target))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map_or(16, |(index, _)| index)
}

fn xterm_fixed_colors() -> impl Iterator<Item = (u8, Rgb)> {
    let cube = (0_u8..216).map(|offset| {
        let red = XTERM_CUBE_LEVELS[usize::from(offset / 36)];
        let green = XTERM_CUBE_LEVELS[usize::from((offset % 36) / 6)];
        let blue = XTERM_CUBE_LEVELS[usize::from(offset % 6)];
        (16 + offset, (red, green, blue))
    });
    let grayscale = (0_u8..24).map(|offset| {
        let level = 8 + offset * 10;
        (232 + offset, (level, level, level))
    });
    cube.chain(grayscale)
}

pub(crate) fn probe_background() -> Option<Rgb> {
    probe_background_impl()
}

#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "netbsd",
    target_os = "openbsd"
))]
fn probe_background_impl() -> Option<Rgb> {
    use std::{fs::OpenOptions, io::Write, os::unix::fs::OpenOptionsExt};

    #[cfg(any(target_os = "linux", target_os = "android"))]
    const O_NONBLOCK: i32 = 0o4000;
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    const O_NONBLOCK: i32 = 0x0004;

    let deadline = Instant::now() + OSC_11_TIMEOUT;
    let mut terminal = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(O_NONBLOCK)
        .open("/dev/tty")
        .ok()?;
    if Instant::now() >= deadline {
        return None;
    }
    terminal.write_all(b"\x1b]11;?\x1b\\").ok()?;
    terminal.flush().ok()?;
    read_background_until(&mut terminal, deadline)
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "netbsd",
    target_os = "openbsd"
)))]
fn probe_background_impl() -> Option<Rgb> {
    None
}

#[cfg(any(
    test,
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "netbsd",
    target_os = "openbsd"
))]
fn read_background_until(reader: &mut impl Read, deadline: Instant) -> Option<Rgb> {
    let mut response = Vec::with_capacity(128);
    let mut chunk = [0_u8; 256];
    loop {
        match reader.read(&mut chunk) {
            Ok(0) => return None,
            Ok(count) => {
                response.extend_from_slice(&chunk[..count]);
                if let Some(background) = parse_osc_11(&response) {
                    return Some(background);
                }
                if response.len() > 4096 {
                    return None;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(_) => return None,
        }
        let now = Instant::now();
        if now >= deadline {
            return None;
        }
        std::thread::sleep(
            deadline
                .saturating_duration_since(now)
                .min(Duration::from_millis(1)),
        );
    }
}

#[cfg(any(
    test,
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "netbsd",
    target_os = "openbsd"
))]
fn parse_osc_11(buffer: &[u8]) -> Option<Rgb> {
    let start = buffer.windows(5).position(|bytes| bytes == b"\x1b]11;")? + 5;
    let rest = &buffer[start..];
    let end = rest
        .iter()
        .enumerate()
        .find_map(|(index, byte)| match byte {
            0x07 => Some(index),
            0x1b if rest.get(index + 1) == Some(&b'\\') => Some(index),
            _ => None,
        })?;
    parse_osc_rgb(std::str::from_utf8(&rest[..end]).ok()?)
}

#[cfg(any(
    test,
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "netbsd",
    target_os = "openbsd"
))]
fn parse_osc_rgb(payload: &str) -> Option<Rgb> {
    let (prefix, components) = payload.trim().split_once(':')?;
    if !prefix.eq_ignore_ascii_case("rgb") && !prefix.eq_ignore_ascii_case("rgba") {
        return None;
    }
    let mut components = components.split('/');
    let red = parse_osc_component(components.next()?)?;
    let green = parse_osc_component(components.next()?)?;
    let blue = parse_osc_component(components.next()?)?;
    if prefix.eq_ignore_ascii_case("rgba") {
        parse_osc_component(components.next()?)?;
    }
    components.next().is_none().then_some((red, green, blue))
}

#[cfg(any(
    test,
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "netbsd",
    target_os = "openbsd"
))]
fn parse_osc_component(component: &str) -> Option<u8> {
    match component.len() {
        2 => u8::from_str_radix(component, 16).ok(),
        4 => u16::from_str_radix(component, 16)
            .ok()
            .map(|value| (value / 257) as u8),
        _ => None,
    }
}

#[cfg(test)]
use std::cell::Cell;

#[cfg(test)]
thread_local! {
    static TEST_COLORS: Cell<Option<TerminalColors>> = const { Cell::new(None) };
}

#[cfg(test)]
pub(crate) fn with_test_colors<T>(colors: TerminalColors, run: impl FnOnce() -> T) -> T {
    struct Reset(Option<TerminalColors>);

    impl Drop for Reset {
        fn drop(&mut self) {
            TEST_COLORS.with(|active| active.set(self.0));
        }
    }

    let previous = TEST_COLORS.with(|active| active.replace(Some(colors)));
    let _reset = Reset(previous);
    run()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn environment() -> ColorEnvironment {
        ColorEnvironment::default()
    }

    #[test]
    fn luma_boundary_is_literal_and_strict() {
        assert!(!is_light((128, 128, 128)));
        assert!(is_light((129, 128, 128)));
        assert!(!is_light((255, 0, 0)));
        assert!(is_light((0, 255, 0)));
    }

    #[test]
    fn blend_uses_literal_alpha_boundaries_and_tints() {
        assert_eq!(blend((255, 255, 255), (0, 0, 0), 0.0), (0, 0, 0));
        assert_eq!(blend((255, 255, 255), (0, 0, 0), 1.0), (255, 255, 255));
        assert_eq!(blend((255, 255, 255), (0, 0, 0), 0.12), (30, 30, 30));
        assert_eq!(blend((0, 0, 0), (255, 255, 255), 0.04), (244, 244, 244));
    }

    #[test]
    fn cie_lab_distance_matches_literal_fixtures() {
        assert_eq!(perceptual_distance((12, 34, 56), (12, 34, 56)), 0.0);
        assert!((perceptual_distance((0, 0, 0), (255, 255, 255)) - 100.0).abs() < 0.01);
        assert!((perceptual_distance((255, 0, 0), (0, 255, 0)) - 170.58).abs() < 0.02);
    }

    #[test]
    fn injected_environment_selects_the_capability_ladder() {
        let mut input = environment();
        input.colorterm = Some("truecolor".into());
        assert_eq!(
            capability_from_environment(&input),
            ColorCapability::TrueColor
        );

        let mut input = environment();
        input.term = Some("screen-256color".into());
        assert_eq!(
            capability_from_environment(&input),
            ColorCapability::Ansi256
        );

        let mut input = environment();
        input.term = Some("xterm".into());
        assert_eq!(capability_from_environment(&input), ColorCapability::Ansi16);

        assert_eq!(
            capability_from_environment(&environment()),
            ColorCapability::Unknown
        );
    }

    #[test]
    fn windows_terminal_is_true_color_without_colorterm() {
        let mut wt_session = environment();
        wt_session.wt_session = Some(OsString::new());
        assert_eq!(
            capability_from_environment(&wt_session),
            ColorCapability::TrueColor
        );

        let mut term_program = environment();
        term_program.term_program = Some("Windows_Terminal".into());
        assert_eq!(
            capability_from_environment(&term_program),
            ColorCapability::TrueColor
        );
    }

    #[test]
    fn forced_capabilities_select_exact_rgb_nearest_256_and_dim_only() {
        let target = (0, 95, 135);
        let true_color = TerminalColors::forced(ColorCapability::TrueColor, Some((0, 0, 0)));
        assert_eq!(true_color.best_color(target), Some(Color::Rgb(0, 95, 135)));

        let ansi_256 = TerminalColors::forced(ColorCapability::Ansi256, Some((0, 0, 0)));
        assert_eq!(ansi_256.best_color(target), Some(Color::Indexed(24)));
        assert_eq!(ansi_256.best_color((8, 8, 8)), Some(Color::Indexed(232)));

        let dim = Style::default().add_modifier(Modifier::DIM);
        for capability in [ColorCapability::Ansi16, ColorCapability::Unknown] {
            let colors = TerminalColors::forced(capability, Some((0, 0, 0)));
            assert_eq!(colors.best_color(target), None);
            assert_eq!(colors.semantic_style(SemanticRole::Primary), dim);
        }
    }

    #[test]
    fn dark_and_light_backgrounds_flip_tint_accent_and_semantic_pairs() {
        assert_eq!(user_message_tint((0, 0, 0)), (30, 30, 30));
        assert_eq!(user_message_tint((255, 255, 255)), (244, 244, 244));

        let dark = TerminalColors::forced(ColorCapability::TrueColor, Some((0, 0, 0)));
        let light = TerminalColors::forced(ColorCapability::TrueColor, Some((255, 255, 255)));
        assert_eq!(dark.accent_style().fg, Some(Color::Rgb(0, 255, 255)));
        assert_eq!(light.accent_style().fg, Some(Color::Rgb(0, 95, 135)));
        for role in [
            SemanticRole::Primary,
            SemanticRole::Error,
            SemanticRole::Warning,
            SemanticRole::Success,
            SemanticRole::Muted,
            SemanticRole::Border,
        ] {
            assert_ne!(dark.semantic_style(role), light.semantic_style(role));
        }
    }

    #[test]
    fn no_color_is_authoritative_above_true_color() {
        let input = ColorEnvironment {
            no_color: Some("1".into()),
            colorterm: Some("truecolor".into()),
            ..environment()
        };
        let colors = TerminalColors::from_environment(&input, Some((0, 0, 0)));

        assert_eq!(colors.capability, ColorCapability::TrueColor);
        assert_eq!(colors.best_color((1, 2, 3)), None);
        assert_eq!(colors.user_message_style(), Style::default());
        assert_eq!(
            colors.semantic_style(SemanticRole::Error),
            Style::default().add_modifier(Modifier::DIM)
        );
        assert!(!colors.should_probe_background());
    }

    #[test]
    fn osc_11_parser_accepts_bel_st_and_component_widths() {
        assert_eq!(
            parse_osc_11(b"\x1b]11;rgb:ffff/8000/0000\x07"),
            Some((255, 127, 0))
        );
        assert_eq!(
            parse_osc_11(b"noise\x1b]11;rgba:00/80/ff/ff\x1b\\tail"),
            Some((0, 128, 255))
        );
        assert_eq!(parse_osc_11(b"\x1b]11;rgb:nope\x07"), None);
        assert_eq!(parse_osc_11(b"\x1b]11;rgb:00/00/00"), None);
    }

    struct NeverAnswers;

    impl Read for NeverAnswers {
        fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::WouldBlock))
        }
    }

    #[test]
    fn osc_11_nonanswer_uses_one_deadline_without_helper_threads() {
        let start = Instant::now();
        let answer = read_background_until(&mut NeverAnswers, start + OSC_11_TIMEOUT);
        let elapsed = start.elapsed();

        assert_eq!(answer, None);
        assert!(elapsed >= OSC_11_TIMEOUT);
        assert!(elapsed < Duration::from_secs(1));
    }

    struct ReadError;

    impl Read for ReadError {
        fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("read failed"))
        }
    }

    #[test]
    fn osc_11_read_error_is_none() {
        assert_eq!(
            read_background_until(&mut ReadError, Instant::now() + OSC_11_TIMEOUT),
            None
        );
    }
}
