use std::sync::OnceLock;

use pulldown_cmark::{CodeBlockKind, Event, HeadingLevel, Options, Parser, Tag, TagEnd};
use ratatui::{
    style::{Color, Modifier, Style},
    text::{Line, Span},
};
use syntect::{
    easy::HighlightLines,
    highlighting::{FontStyle, Style as SyntectStyle, ThemeSet},
    parsing::SyntaxSet,
    util::LinesWithEndings,
};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

const MESSAGE_INDENT: &str = "  ";
const CODE_RAIL: &str = "| ";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SyntaxTheme {
    name: &'static str,
    revision: u64,
}

pub(super) const DEFAULT_SYNTAX_THEME: SyntaxTheme = SyntaxTheme {
    name: "base16-ocean.dark",
    revision: 0,
};

#[cfg(test)]
impl SyntaxTheme {
    pub(super) fn with_revision(self, revision: u64) -> Self {
        Self { revision, ..self }
    }
}

#[derive(Default)]
pub(super) struct MarkdownRenderer {
    syntax_assets: OnceLock<SyntaxAssets>,
    #[cfg(test)]
    render_calls: std::sync::atomic::AtomicUsize,
}

impl MarkdownRenderer {
    pub(super) fn render(
        &self,
        source: &str,
        width: u16,
        theme: SyntaxTheme,
    ) -> Vec<Line<'static>> {
        #[cfg(test)]
        self.render_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        if source.is_empty() {
            return Vec::new();
        }
        if source.chars().all(char::is_whitespace) {
            return literal_whitespace_rows(source, usize::from(width));
        }

        let mut builder = MarkdownBuilder::new(self, usize::from(width), theme);
        for event in Parser::new_ext(source, Options::empty()) {
            builder.push(event);
        }
        let mut lines = builder.finish();
        if lines.is_empty() {
            lines = literal_whitespace_rows(source, usize::from(width));
        }
        lines
    }

    fn syntax_assets(&self) -> &SyntaxAssets {
        self.syntax_assets.get_or_init(|| SyntaxAssets {
            syntaxes: SyntaxSet::load_defaults_newlines(),
            themes: ThemeSet::load_defaults(),
        })
    }

    #[cfg(test)]
    fn syntax_assets_loaded(&self) -> bool {
        self.syntax_assets.get().is_some()
    }

    #[cfg(test)]
    pub(super) fn render_calls(&self) -> usize {
        self.render_calls.load(std::sync::atomic::Ordering::Relaxed)
    }
}

struct SyntaxAssets {
    syntaxes: SyntaxSet,
    themes: ThemeSet,
}

struct MarkdownBuilder<'a> {
    renderer: &'a MarkdownRenderer,
    width: usize,
    theme: SyntaxTheme,
    lines: Vec<Line<'static>>,
    last_block_compact: bool,
    prose: Option<ProseBlock>,
    styles: Vec<Style>,
    quote_depth: usize,
    lists: Vec<ListState>,
    pending_item_marker: Option<String>,
    code: Option<CodeBlock>,
}

impl<'a> MarkdownBuilder<'a> {
    fn new(renderer: &'a MarkdownRenderer, width: usize, theme: SyntaxTheme) -> Self {
        Self {
            renderer,
            width,
            theme,
            lines: Vec::new(),
            last_block_compact: false,
            prose: None,
            styles: Vec::new(),
            quote_depth: 0,
            lists: Vec::new(),
            pending_item_marker: None,
            code: None,
        }
    }

    fn push(&mut self, event: Event<'_>) {
        if self.push_code_event(&event) {
            return;
        }

        match event {
            Event::Start(tag) => self.start(tag),
            Event::End(tag) => self.end(tag),
            Event::Text(text) | Event::Html(text) | Event::InlineHtml(text) => {
                self.push_text(text.as_ref(), None);
            }
            Event::Code(text) => {
                self.push_text(text.as_ref(), Some(Style::default().fg(Color::Yellow)));
            }
            Event::InlineMath(text) | Event::DisplayMath(text) => {
                self.push_text(text.as_ref(), None);
            }
            Event::FootnoteReference(label) => {
                self.push_text(&format!("[^{label}]"), None);
            }
            Event::SoftBreak => self.push_text(" ", None),
            Event::HardBreak => self.push_text("\n", None),
            Event::Rule => self.push_rule(),
            Event::TaskListMarker(checked) => {
                self.push_text(if checked { "[x] " } else { "[ ] " }, None);
            }
        }
    }

    fn push_code_event(&mut self, event: &Event<'_>) -> bool {
        let Some(code) = self.code.as_mut() else {
            return false;
        };
        match event {
            Event::End(TagEnd::CodeBlock) => {
                self.flush_code();
            }
            Event::Text(text)
            | Event::Code(text)
            | Event::Html(text)
            | Event::InlineHtml(text)
            | Event::InlineMath(text)
            | Event::DisplayMath(text) => code.source.push_str(text.as_ref()),
            Event::SoftBreak | Event::HardBreak => code.source.push('\n'),
            Event::FootnoteReference(label) => {
                code.source.push_str("[^");
                code.source.push_str(label.as_ref());
                code.source.push(']');
            }
            Event::TaskListMarker(checked) => {
                code.source.push_str(if *checked { "[x] " } else { "[ ] " });
            }
            Event::Rule => code.source.push_str("---"),
            Event::Start(_) | Event::End(_) => {}
        }
        true
    }

    fn start(&mut self, tag: Tag<'_>) {
        match tag {
            Tag::Paragraph => self.start_prose(Style::default()),
            Tag::Heading { level, .. } => self.start_prose(heading_style(level)),
            Tag::BlockQuote(_) => {
                self.flush_prose();
                self.quote_depth += 1;
            }
            Tag::CodeBlock(kind) => self.start_code(kind),
            Tag::HtmlBlock => self.start_prose(Style::default()),
            Tag::List(start) => {
                self.flush_prose();
                self.lists.push(ListState {
                    next: start.unwrap_or(1),
                    ordered: start.is_some(),
                });
            }
            Tag::Item => {
                self.flush_prose();
                let Some(list) = self.lists.last_mut() else {
                    return;
                };
                self.pending_item_marker = Some(if list.ordered {
                    let marker = format!("{}. ", list.next);
                    list.next = list.next.saturating_add(1);
                    marker
                } else {
                    "* ".to_owned()
                });
            }
            Tag::Emphasis => self.push_style(Style::default().add_modifier(Modifier::ITALIC)),
            Tag::Strong => self.push_style(Style::default().add_modifier(Modifier::BOLD)),
            Tag::Strikethrough => {
                self.push_style(Style::default().add_modifier(Modifier::CROSSED_OUT));
            }
            Tag::Superscript | Tag::Subscript => self.push_style(Style::default()),
            Tag::Link { .. }
            | Tag::Image { .. }
            | Tag::FootnoteDefinition(_)
            | Tag::DefinitionList
            | Tag::DefinitionListTitle
            | Tag::DefinitionListDefinition
            | Tag::Table(_)
            | Tag::TableHead
            | Tag::TableRow
            | Tag::TableCell
            | Tag::MetadataBlock(_) => {}
        }
    }

    fn end(&mut self, tag: TagEnd) {
        match tag {
            TagEnd::Paragraph | TagEnd::Heading(_) | TagEnd::HtmlBlock => self.flush_prose(),
            TagEnd::BlockQuote(_) => {
                self.flush_prose();
                self.quote_depth = self.quote_depth.saturating_sub(1);
            }
            TagEnd::CodeBlock => self.flush_code(),
            TagEnd::List(_) => {
                self.flush_prose();
                self.lists.pop();
            }
            TagEnd::Item => {
                self.flush_prose();
                if let Some(marker) = self.pending_item_marker.take() {
                    let (first, _) = self.content_prefixes(Some(marker));
                    self.append_block(vec![Line::from(first)], true);
                }
            }
            TagEnd::Emphasis
            | TagEnd::Strong
            | TagEnd::Strikethrough
            | TagEnd::Superscript
            | TagEnd::Subscript => self.pop_style(),
            TagEnd::Link
            | TagEnd::Image
            | TagEnd::FootnoteDefinition
            | TagEnd::DefinitionList
            | TagEnd::DefinitionListTitle
            | TagEnd::DefinitionListDefinition
            | TagEnd::Table
            | TagEnd::TableHead
            | TagEnd::TableRow
            | TagEnd::TableCell
            | TagEnd::MetadataBlock(_) => {}
        }
    }

    fn start_prose(&mut self, base_style: Style) {
        self.flush_prose();
        let marker = self.pending_item_marker.take();
        let (first_prefix, continuation_prefix) = self.content_prefixes(marker);
        self.prose = Some(ProseBlock {
            spans: Vec::new(),
            first_prefix,
            continuation_prefix,
            compact: !self.lists.is_empty(),
        });
        self.styles.clear();
        self.styles.push(base_style);
    }

    fn ensure_prose(&mut self) {
        if self.prose.is_none() {
            self.start_prose(Style::default());
        }
    }

    fn push_style(&mut self, style: Style) {
        self.ensure_prose();
        let current = self.styles.last().copied().unwrap_or_default();
        self.styles.push(current.patch(style));
    }

    fn pop_style(&mut self) {
        if self.styles.len() > 1 {
            self.styles.pop();
        }
    }

    fn push_text(&mut self, text: &str, style: Option<Style>) {
        self.ensure_prose();
        let current = self.styles.last().copied().unwrap_or_default();
        let style = style.map_or(current, |style| current.patch(style));
        if let Some(prose) = self.prose.as_mut() {
            prose.spans.push(Span::styled(text.to_owned(), style));
        }
    }

    fn flush_prose(&mut self) {
        let Some(prose) = self.prose.take() else {
            return;
        };
        self.styles.clear();
        if prose.spans.is_empty() {
            return;
        }
        let rows = wrap_prose(
            &prose.spans,
            &prose.first_prefix,
            &prose.continuation_prefix,
            self.width,
        );
        self.append_block(rows, prose.compact);
    }

    fn start_code(&mut self, kind: CodeBlockKind<'_>) {
        self.flush_prose();
        let marker = self.pending_item_marker.take();
        let (mut first_prefix, mut continuation_prefix) = self.content_prefixes(marker);
        let rail = Span::styled(CODE_RAIL, Style::default().fg(Color::DarkGray));
        first_prefix.push(rail.clone());
        continuation_prefix.push(rail);
        let (fenced, language) = match kind {
            CodeBlockKind::Fenced(info) => (true, declared_language(info.as_ref())),
            CodeBlockKind::Indented => (false, None),
        };
        self.code = Some(CodeBlock {
            source: String::new(),
            fenced,
            language,
            first_prefix,
            continuation_prefix,
            compact: !self.lists.is_empty(),
        });
    }

    fn flush_code(&mut self) {
        let Some(code) = self.code.take() else {
            return;
        };
        let styled_lines = if code.fenced && is_unified_diff(code.language.as_deref(), &code.source)
        {
            diff_lines(&code.source)
        } else if code.fenced {
            code.language
                .as_deref()
                .and_then(|language| {
                    highlighted_lines(self.renderer, &code.source, language, self.theme)
                })
                .unwrap_or_else(|| plain_code_lines(&code.source))
        } else {
            plain_code_lines(&code.source)
        };

        let mut rows = Vec::new();
        for (index, spans) in styled_lines.iter().enumerate() {
            let prefix = if index == 0 {
                &code.first_prefix
            } else {
                &code.continuation_prefix
            };
            rows.extend(wrap_exact(
                spans,
                prefix,
                &code.continuation_prefix,
                self.width,
            ));
        }
        self.append_block(rows, code.compact);
    }

    fn push_rule(&mut self) {
        self.flush_prose();
        let marker = self.pending_item_marker.take();
        let (mut prefix, _) = self.content_prefixes(marker);
        let available = self.width.saturating_sub(spans_width(&prefix));
        prefix.push(Span::styled(
            "-".repeat(available.clamp(1, 24)),
            Style::default().fg(Color::DarkGray),
        ));
        self.append_block(vec![Line::from(prefix)], !self.lists.is_empty());
    }

    fn content_prefixes(
        &self,
        item_marker: Option<String>,
    ) -> (Vec<Span<'static>>, Vec<Span<'static>>) {
        let mut first = vec![Span::raw(MESSAGE_INDENT)];
        let mut continuation = first.clone();
        for _ in 0..self.quote_depth {
            let rail = Span::styled("| ", Style::default().fg(Color::DarkGray));
            first.push(rail.clone());
            continuation.push(rail);
        }
        if !self.lists.is_empty() {
            let depth = self.lists.len();
            if let Some(marker) = item_marker {
                first.push(Span::raw("  ".repeat(depth.saturating_sub(1))));
                first.push(Span::styled(marker, Style::default().fg(Color::DarkGray)));
            } else {
                first.push(Span::raw("  ".repeat(depth)));
            }
            continuation.push(Span::raw("  ".repeat(depth)));
        }
        (first, continuation)
    }

    fn append_block(&mut self, rows: Vec<Line<'static>>, compact: bool) {
        if rows.is_empty() {
            return;
        }
        if !(self.lines.is_empty() || (compact && self.last_block_compact)) {
            self.lines.push(Line::from(""));
        }
        self.lines.extend(rows);
        self.last_block_compact = compact;
    }

    fn finish(mut self) -> Vec<Line<'static>> {
        self.flush_prose();
        self.flush_code();
        while self.lines.first().is_some_and(|line| line.width() == 0) {
            self.lines.remove(0);
        }
        while self.lines.last().is_some_and(|line| line.width() == 0) {
            self.lines.pop();
        }
        self.lines
    }
}

struct ProseBlock {
    spans: Vec<Span<'static>>,
    first_prefix: Vec<Span<'static>>,
    continuation_prefix: Vec<Span<'static>>,
    compact: bool,
}

struct ListState {
    next: u64,
    ordered: bool,
}

struct CodeBlock {
    source: String,
    fenced: bool,
    language: Option<String>,
    first_prefix: Vec<Span<'static>>,
    continuation_prefix: Vec<Span<'static>>,
    compact: bool,
}

#[derive(Clone)]
struct StyledGlyph {
    text: String,
    style: Style,
}

fn heading_style(level: HeadingLevel) -> Style {
    let color = match level {
        HeadingLevel::H1 => Color::Yellow,
        HeadingLevel::H2 => Color::Cyan,
        HeadingLevel::H3 | HeadingLevel::H4 | HeadingLevel::H5 | HeadingLevel::H6 => Color::Reset,
    };
    Style::default().fg(color).add_modifier(Modifier::BOLD)
}

fn declared_language(info: &str) -> Option<String> {
    info.split_whitespace()
        .next()
        .map(|language| {
            language
                .trim_matches(|character: char| matches!(character, '{' | '}' | '.' | ','))
                .split(',')
                .next()
                .unwrap_or(language)
                .to_ascii_lowercase()
        })
        .filter(|language| !language.is_empty())
}

fn highlighted_lines(
    renderer: &MarkdownRenderer,
    source: &str,
    language: &str,
    theme: SyntaxTheme,
) -> Option<Vec<Vec<Span<'static>>>> {
    let assets = renderer.syntax_assets();
    let syntax = assets.syntaxes.find_syntax_by_token(language)?;
    let theme = assets
        .themes
        .themes
        .get(theme.name)
        .or_else(|| assets.themes.themes.get(DEFAULT_SYNTAX_THEME.name))?;
    let mut highlighter = HighlightLines::new(syntax, theme);
    let mut lines = Vec::new();
    for line in LinesWithEndings::from(source) {
        let visible_len = line.strip_suffix('\n').unwrap_or(line).len();
        let regions = highlighter.highlight_line(line, &assets.syntaxes).ok()?;
        let mut remaining = visible_len;
        let mut spans = Vec::new();
        for (style, text) in regions {
            if remaining == 0 {
                break;
            }
            let length = remaining.min(text.len());
            if length > 0 {
                spans.push(Span::styled(
                    text[..length].to_owned(),
                    terminal_safe_syntect_style(style),
                ));
                remaining -= length;
            }
        }
        lines.push(spans);
    }
    if lines.is_empty() {
        lines.push(Vec::new());
    }
    Some(lines)
}

fn terminal_safe_syntect_style(style: SyntectStyle) -> Style {
    let color = style.foreground;
    let max = color.r.max(color.g).max(color.b);
    let min = color.r.min(color.g).min(color.b);
    let foreground = if max.saturating_sub(min) < 24 {
        Color::Reset
    } else if color.r == max && color.g.saturating_add(24) >= max {
        Color::Yellow
    } else if color.r == max && color.b.saturating_add(24) >= max {
        Color::Magenta
    } else if color.g == max && color.b.saturating_add(24) >= max {
        Color::Cyan
    } else if color.r == max {
        Color::Red
    } else if color.g == max {
        Color::Green
    } else {
        Color::Blue
    };
    let mut rendered = Style::default().fg(foreground);
    if style.font_style.contains(FontStyle::BOLD) {
        rendered = rendered.add_modifier(Modifier::BOLD);
    }
    if style.font_style.contains(FontStyle::ITALIC) {
        rendered = rendered.add_modifier(Modifier::ITALIC);
    }
    if style.font_style.contains(FontStyle::UNDERLINE) {
        rendered = rendered.add_modifier(Modifier::UNDERLINED);
    }
    rendered
}

fn is_unified_diff(language: Option<&str>, source: &str) -> bool {
    if language.is_some_and(|language| matches!(language, "diff" | "patch" | "udiff")) {
        return true;
    }
    let mut old_header = false;
    let mut new_header = false;
    let mut hunk = false;
    for line in logical_lines(source) {
        old_header |= line.starts_with("--- ");
        new_header |= line.starts_with("+++ ");
        hunk |= line.starts_with("@@");
    }
    source.lines().any(|line| line.starts_with("diff --git ")) || (old_header && new_header && hunk)
}

fn diff_lines(source: &str) -> Vec<Vec<Span<'static>>> {
    logical_lines(source)
        .into_iter()
        .map(|line| {
            let style = if line.starts_with("diff --git ")
                || line.starts_with("index ")
                || line.starts_with("--- ")
                || line.starts_with("+++ ")
            {
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else if line.starts_with("@@") {
                Style::default().fg(Color::Magenta)
            } else if line.starts_with('+') {
                Style::default().fg(Color::Green)
            } else if line.starts_with('-') {
                Style::default().fg(Color::Red)
            } else if line.starts_with("\\ No newline at end of file") {
                Style::default().fg(Color::Yellow)
            } else {
                Style::default()
            };
            vec![Span::styled(line.to_owned(), style)]
        })
        .collect()
}

fn plain_code_lines(source: &str) -> Vec<Vec<Span<'static>>> {
    logical_lines(source)
        .into_iter()
        .map(|line| vec![Span::raw(line.to_owned())])
        .collect()
}

fn logical_lines(source: &str) -> Vec<&str> {
    if source.is_empty() {
        return vec![""];
    }
    source.split_terminator('\n').collect()
}

fn wrap_prose(
    spans: &[Span<'static>],
    first_prefix: &[Span<'static>],
    continuation_prefix: &[Span<'static>],
    width: usize,
) -> Vec<Line<'static>> {
    let glyphs = styled_glyphs(spans);
    let mut segments = vec![Vec::new()];
    for glyph in glyphs {
        if glyph.text == "\n" {
            segments.push(Vec::new());
        } else {
            segments.last_mut().unwrap().push(glyph);
        }
    }

    let mut rows = Vec::new();
    let mut first = true;
    for segment in segments {
        let prefix = if first {
            first_prefix
        } else {
            continuation_prefix
        };
        let mut wrapped = wrap_prose_segment(&segment, prefix, continuation_prefix, width);
        first = false;
        rows.append(&mut wrapped);
    }
    rows
}

fn wrap_prose_segment(
    glyphs: &[StyledGlyph],
    first_prefix: &[Span<'static>],
    continuation_prefix: &[Span<'static>],
    width: usize,
) -> Vec<Line<'static>> {
    if glyphs.is_empty() {
        return vec![Line::from(first_prefix.to_vec())];
    }

    let mut rows = Vec::new();
    let mut remaining = glyphs;
    let mut prefix = first_prefix;
    while !remaining.is_empty() {
        let available = width.saturating_sub(spans_width(prefix)).max(1);
        let mut used = 0;
        let mut take = 0;
        while take < remaining.len() {
            let next = glyph_width(&remaining[take]);
            if used + next > available {
                break;
            }
            used += next;
            take += 1;
        }
        if take == remaining.len() {
            rows.push(line_from_glyphs(prefix, remaining));
            break;
        }
        if take == 0 {
            take = 1;
        }

        let break_at = remaining[..take]
            .iter()
            .rposition(glyph_is_whitespace)
            .filter(|index| *index > 0);
        if let Some(break_at) = break_at {
            let end = remaining[..break_at]
                .iter()
                .rposition(|glyph| !glyph_is_whitespace(glyph))
                .map_or(0, |index| index + 1);
            rows.push(line_from_glyphs(prefix, &remaining[..end]));
            let mut next = break_at;
            while next < remaining.len() && glyph_is_whitespace(&remaining[next]) {
                next += 1;
            }
            remaining = &remaining[next..];
        } else {
            rows.push(line_from_glyphs(prefix, &remaining[..take]));
            remaining = &remaining[take..];
        }
        prefix = continuation_prefix;
    }
    rows
}

fn wrap_exact(
    spans: &[Span<'static>],
    first_prefix: &[Span<'static>],
    continuation_prefix: &[Span<'static>],
    width: usize,
) -> Vec<Line<'static>> {
    let glyphs = styled_glyphs(spans);
    if glyphs.is_empty() {
        return vec![Line::from(first_prefix.to_vec())];
    }
    let mut rows = Vec::new();
    let mut remaining = glyphs.as_slice();
    let mut prefix = first_prefix;
    while !remaining.is_empty() {
        let available = width.saturating_sub(spans_width(prefix)).max(1);
        let mut used = 0;
        let mut take = 0;
        while take < remaining.len() {
            let next = glyph_width(&remaining[take]);
            if used + next > available {
                break;
            }
            used += next;
            take += 1;
        }
        if take == 0 {
            take = 1;
        }
        rows.push(line_from_glyphs(prefix, &remaining[..take]));
        remaining = &remaining[take..];
        prefix = continuation_prefix;
    }
    rows
}

fn styled_glyphs(spans: &[Span<'static>]) -> Vec<StyledGlyph> {
    spans
        .iter()
        .flat_map(|span| {
            UnicodeSegmentation::graphemes(span.content.as_ref(), true).map(|grapheme| {
                StyledGlyph {
                    text: grapheme.to_owned(),
                    style: span.style,
                }
            })
        })
        .collect()
}

fn line_from_glyphs(prefix: &[Span<'static>], glyphs: &[StyledGlyph]) -> Line<'static> {
    let mut spans = prefix.to_vec();
    let prefix_len = spans.len();
    for glyph in glyphs {
        if spans.len() > prefix_len {
            let last = spans.last_mut().expect("body span should exist");
            if last.style == glyph.style {
                last.content.to_mut().push_str(&glyph.text);
                continue;
            }
        }
        spans.push(Span::styled(glyph.text.clone(), glyph.style));
    }
    Line::from(spans)
}

fn literal_whitespace_rows(source: &str, width: usize) -> Vec<Line<'static>> {
    let prefix = vec![Span::raw(MESSAGE_INDENT)];
    logical_lines(source)
        .into_iter()
        .flat_map(|line| wrap_exact(&[Span::raw(line.to_owned())], &prefix, &prefix, width))
        .collect()
}

fn spans_width(spans: &[Span<'_>]) -> usize {
    spans
        .iter()
        .map(|span| UnicodeWidthStr::width(span.content.as_ref()))
        .sum()
}

fn glyph_width(glyph: &StyledGlyph) -> usize {
    UnicodeWidthStr::width(glyph.text.as_str())
}

fn glyph_is_whitespace(glyph: &StyledGlyph) -> bool {
    glyph.text.chars().all(char::is_whitespace)
}

#[cfg(test)]
mod tests {
    use super::*;

    const WIDTH_FIXTURE: &str = r#"# Build safely

Use **cargo** with *care* and `--locked` for deterministic builds that remain readable.

> Keep the ledger exact across every reload.

1. Check the inputs
2. Run the command

```rust
fn main() {
    let message = "this deliberately long code line must wrap without losing indentation or order";
}
```

```diff
--- a/src/main.rs
+++ b/src/main.rs
@@ -1,2 +1,2 @@
-old value that is deliberately long enough to wrap at a narrow width
+new value that is deliberately long enough to wrap at a narrow width
 context line
```
"#;

    #[test]
    fn renders_named_markdown_subset_without_delimiters() {
        let renderer = MarkdownRenderer::default();
        let source =
            "# Heading\n\nA **strong** and *soft* `value`.\n\n- first\n- second\n\n> quoted\n\n---";
        let rows = renderer.render(source, 80, DEFAULT_SYNTAX_THEME);
        let rendered = plain(&rows);

        assert_eq!(
            rendered,
            "  Heading\n\n  A strong and soft value.\n\n  * first\n  * second\n\n  | quoted\n\n  ------------------------"
        );
        for delimiter in ["# Heading", "**strong**", "*soft*", "`value`", "> quoted"] {
            assert!(!rendered.contains(delimiter));
        }
        assert!(rows.iter().any(|line| {
            line.spans.iter().any(|span| {
                span.content == "strong" && span.style.add_modifier.contains(Modifier::BOLD)
            })
        }));
        assert!(rows.iter().any(|line| {
            line.spans.iter().any(|span| {
                span.content == "soft" && span.style.add_modifier.contains(Modifier::ITALIC)
            })
        }));
        assert!(rows.iter().all(|line| line.width() <= 80));
    }

    #[test]
    fn syntect_is_lazy_and_unknown_or_absent_languages_fall_back_legibly() {
        let prose = MarkdownRenderer::default();
        prose.render("plain prose", 80, DEFAULT_SYNTAX_THEME);
        assert!(!prose.syntax_assets_loaded());

        let absent = MarkdownRenderer::default();
        let absent_rows = absent.render("```\nlet value = 1;\n```", 80, DEFAULT_SYNTAX_THEME);
        assert!(!absent.syntax_assets_loaded());
        assert_eq!(code_bodies(&absent_rows), vec!["let value = 1;"]);
        assert!(
            body_styles(&absent_rows)
                .iter()
                .all(|style| *style == Style::default())
        );

        let unknown = MarkdownRenderer::default();
        let unknown_rows = unknown.render(
            "```definitely-unknown\nlet value = 1;\n```",
            80,
            DEFAULT_SYNTAX_THEME,
        );
        assert!(unknown.syntax_assets_loaded());
        assert_eq!(code_bodies(&unknown_rows), vec!["let value = 1;"]);
        assert!(
            body_styles(&unknown_rows)
                .iter()
                .all(|style| *style == Style::default())
        );

        let rust = MarkdownRenderer::default();
        let rust_rows = rust.render(
            "```rust\nfn main() { let value = 1; }\n```",
            80,
            DEFAULT_SYNTAX_THEME,
        );
        assert!(rust.syntax_assets_loaded());
        assert_eq!(
            code_bodies(&rust_rows),
            vec!["fn main() { let value = 1; }"]
        );
        assert!(
            body_styles(&rust_rows)
                .iter()
                .any(|style| style.fg.is_some_and(|color| color != Color::Reset))
        );
        assert!(
            body_styles(&rust_rows)
                .iter()
                .all(|style| !matches!(style.fg, Some(Color::Rgb(..) | Color::Indexed(_))))
        );
    }

    #[test]
    fn unified_diff_rows_preserve_bytes_and_have_distinct_styles() {
        let renderer = MarkdownRenderer::default();
        let source = "```diff\ndiff --git a/a b/a\n--- a/a\n+++ b/a\n@@ -1 +1 @@\n-old bytes  \n+new bytes\n context\n\\ No newline at end of file\n```";
        let rows = renderer.render(source, 120, DEFAULT_SYNTAX_THEME);

        assert!(!renderer.syntax_assets_loaded());
        assert_eq!(
            code_bodies(&rows),
            vec![
                "diff --git a/a b/a",
                "--- a/a",
                "+++ b/a",
                "@@ -1 +1 @@",
                "-old bytes  ",
                "+new bytes",
                " context",
                "\\ No newline at end of file",
            ]
        );
        let styles = body_styles(&rows);
        assert_eq!(styles[0].fg, Some(Color::Cyan));
        assert_eq!(styles[3].fg, Some(Color::Magenta));
        assert_eq!(styles[4].fg, Some(Color::Red));
        assert_eq!(styles[5].fg, Some(Color::Green));
        assert_eq!(styles[6], Style::default());
        assert_eq!(styles[7].fg, Some(Color::Yellow));
    }

    #[test]
    fn malformed_and_streaming_partial_markdown_is_deterministic() {
        let renderer = MarkdownRenderer::default();
        for source in [
            "**partial",
            "`partial",
            "# heading\n\n> **quote",
            "```rust\nfn main() {",
            "```unknown\nbytes **stay literal",
        ] {
            let first = renderer.render(source, 40, DEFAULT_SYNTAX_THEME);
            let second = renderer.render(source, 40, DEFAULT_SYNTAX_THEME);
            assert_eq!(first, second, "partial fixture changed: {source:?}");
            assert!(first.iter().all(|line| line.width() <= 40));
        }
        assert!(
            plain(&renderer.render("**partial", 40, DEFAULT_SYNTAX_THEME)).contains("**partial")
        );
        let unclosed = plain(&renderer.render("```rust\nfn main() {", 40, DEFAULT_SYNTAX_THEME));
        assert!(unclosed.contains("fn main() {"));
        assert!(!unclosed.contains("```"));

        let committed = "A **bold** result with `code` and a café.";
        let mut boundaries = committed
            .char_indices()
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        boundaries.push(committed.len());
        for boundary in boundaries {
            let rows = renderer.render(&committed[..boundary], 40, DEFAULT_SYNTAX_THEME);
            assert!(rows.iter().all(|line| line.width() <= 40));
        }
    }

    #[test]
    fn viewport_snapshots_at_40_80_120_columns_are_bounded() {
        let renderer = MarkdownRenderer::default();
        let forty = plain(&renderer.render(WIDTH_FIXTURE, 40, DEFAULT_SYNTAX_THEME));
        let eighty = plain(&renderer.render(WIDTH_FIXTURE, 80, DEFAULT_SYNTAX_THEME));
        let one_twenty = plain(&renderer.render(WIDTH_FIXTURE, 120, DEFAULT_SYNTAX_THEME));

        assert_eq!(
            forty,
            "  Build safely\n\n  Use cargo with care and --locked for\n  deterministic builds that remain\n  readable.\n\n  | Keep the ledger exact across every\n  | reload.\n\n  1. Check the inputs\n  2. Run the command\n\n  | fn main() {\n  |     let message = \"this deliberately\n  |  long code line must wrap without lo\n  | sing indentation or order\";\n  | }\n\n  | --- a/src/main.rs\n  | +++ b/src/main.rs\n  | @@ -1,2 +1,2 @@\n  | -old value that is deliberately long\n  |  enough to wrap at a narrow width\n  | +new value that is deliberately long\n  |  enough to wrap at a narrow width\n  |  context line"
        );
        assert_eq!(
            eighty,
            "  Build safely\n\n  Use cargo with care and --locked for deterministic builds that remain\n  readable.\n\n  | Keep the ledger exact across every reload.\n\n  1. Check the inputs\n  2. Run the command\n\n  | fn main() {\n  |     let message = \"this deliberately long code line must wrap without losing\n  |  indentation or order\";\n  | }\n\n  | --- a/src/main.rs\n  | +++ b/src/main.rs\n  | @@ -1,2 +1,2 @@\n  | -old value that is deliberately long enough to wrap at a narrow width\n  | +new value that is deliberately long enough to wrap at a narrow width\n  |  context line"
        );
        assert_eq!(
            one_twenty,
            "  Build safely\n\n  Use cargo with care and --locked for deterministic builds that remain readable.\n\n  | Keep the ledger exact across every reload.\n\n  1. Check the inputs\n  2. Run the command\n\n  | fn main() {\n  |     let message = \"this deliberately long code line must wrap without losing indentation or order\";\n  | }\n\n  | --- a/src/main.rs\n  | +++ b/src/main.rs\n  | @@ -1,2 +1,2 @@\n  | -old value that is deliberately long enough to wrap at a narrow width\n  | +new value that is deliberately long enough to wrap at a narrow width\n  |  context line"
        );
        for (width, rows) in [
            (40, renderer.render(WIDTH_FIXTURE, 40, DEFAULT_SYNTAX_THEME)),
            (80, renderer.render(WIDTH_FIXTURE, 80, DEFAULT_SYNTAX_THEME)),
            (
                120,
                renderer.render(WIDTH_FIXTURE, 120, DEFAULT_SYNTAX_THEME),
            ),
        ] {
            assert!(rows.iter().all(|line| line.width() <= width));
        }
    }

    fn plain(rows: &[Line<'_>]) -> String {
        rows.iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn code_bodies(rows: &[Line<'_>]) -> Vec<String> {
        rows.iter()
            .filter(|line| {
                line.spans
                    .get(1)
                    .is_some_and(|span| span.content == CODE_RAIL)
            })
            .map(|line| {
                line.spans
                    .iter()
                    .skip(2)
                    .map(|span| span.content.as_ref())
                    .collect()
            })
            .collect()
    }

    fn body_styles(rows: &[Line<'_>]) -> Vec<Style> {
        rows.iter()
            .filter(|line| {
                line.spans
                    .get(1)
                    .is_some_and(|span| span.content == CODE_RAIL)
            })
            .map(|line| {
                line.spans
                    .get(2)
                    .map_or(Style::default(), |span| span.style)
            })
            .collect()
    }
}
