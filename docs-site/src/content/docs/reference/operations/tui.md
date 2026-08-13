---
title: TUI controls
description: Source-checked Plato Agent keyboard controls, slash commands, and contextual overlays.
sidebar:
  order: 2
---

<p class="section-kicker reference">Reference</p>

This page is part of the [unreleased 0.2.0 operating guide](../../../user/operations/).

Press `?` with an empty composer for the controls implemented by the running client. Text keys remain composer input when the composer is nonempty unless noted below.

## Main keys

| Key | Action |
| --- | --- |
| `Enter` | Submit the composer |
| `Shift+Enter`, `Alt+Enter`, `Ctrl+J/M` | Insert a newline |
| `Tab` | Complete the selected slash command, submit, or queue an unattached busy-session message |
| `v` | Toggle conversation and audit views when the composer is empty |
| `PgUp/PgDown` | Scroll the focused audit or approval view |
| `Up/Down` | Recall composer history at its first or last line; otherwise move within the composer |
| `Esc` | Cancel an active run, close the focused overlay, or exit from the idle main view |
| `Ctrl+C` | Cancel an active run; press again to quit |
| `q` | Quit with an empty composer; close help while help is focused |
| `r` | Reconnect and reload when offline |
| `?` | Open shortcuts with an empty composer |

An attached durable thread sends a submitted message to the server as a start or steer request. The local queue applies to a busy unattached session, not to an attached thread.

## Composer editing

| Key | Action |
| --- | --- |
| Arrow keys, `Home`, `End` | Move the cursor; hold `Shift` to select |
| `Alt+B/F`, `Ctrl+Left/Right` | Move backward or forward by word |
| `Ctrl+A/E`, `Ctrl+B/F` | Move to the line start or end, or one character backward or forward |
| `Backspace`, `Delete` | Delete before or after the cursor, or delete the selection |
| `Ctrl+W`, `Ctrl+K`, `Ctrl+U` | Delete the previous word, to the line end, or to the start of the composer |
| `Ctrl+Y` | Insert the most recently deleted text |
| `Ctrl+Z`, `Ctrl+R`, `Ctrl+Shift+Z` | Undo or redo |
| `Ctrl+P/N` | Recall the previous or next submitted composer entry |

A backslash immediately before `Enter` is consumed and inserts a newline instead of submitting. Bracketed paste inserts literal text as one undoable edit.

## Slash commands

| Command | Action |
| --- | --- |
| `/help` | Open help |
| `/clear` | Clear the visible transcript without deleting durable history |
| `/threads` | Open the durable thread picker |
| `/sessions` | Compatibility alias for `/threads` |
| `/new` | Select a fresh session and turn voice off; unavailable while attached to a thread |
| `/issue-prep ROUGH_ISSUE` | Prepare and review an issue; unavailable while attached to a thread |
| `/status` | Read authoritative runtime status without starting a run |
| `/voice on\|off` | Enable or disable optional local voice for this client session |
| `/yolo on\|off` | Set the selected session or next fresh session's daemon-lifetime approval profile |
| `/reconnect` | Reconnect and reload when offline |
| `/quit`, `/exit` | Close the TUI |

Typing `/` opens a case-insensitive subsequence-matched command popup. Use `Up` and `Down` or `Ctrl+P` and `Ctrl+N` to select, `Tab` to complete, `Enter` to execute, and `Esc` to close it.

## Thread picker

Type to filter by a case-insensitive subsequence of thread ID or `active`, `loaded`, or `unloaded` state.

| Key | Action |
| --- | --- |
| `Up`, `Down`, `Ctrl+P`, `Ctrl+N` | Wrap through matches |
| `Backspace` | Edit the filter |
| `Enter` | Attach the focused match; stays open when there is no match |
| `Esc` | Close the picker |

## Approval pane

| Key | Action |
| --- | --- |
| `g` | Grant once |
| `s` | Grant this `shell.exec` and later shell calls in the selected session until server exit; unavailable for other tools |
| `d` | Deny |
| `Up`, `Down`, `PageUp`, `PageDown` | Scroll the preview |
| `Esc` | Cancel the active run |
| `q` | Exit the TUI and leave the request pending for durable readback |

Read the [approval guide](../../../user/operations/approvals/) before using session grants or yolo mode.
