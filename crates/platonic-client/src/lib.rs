//! Bounded client calls and local IPC transport for `platonic serve`.
//!
//! This crate owns daemon connection mechanics, request deadlines, endpoint
//! discovery, lock metadata, and the Windows installer startup gate. Daemon
//! run and server semantics remain in `plato-agent`.

#![deny(unsafe_code)]
#![warn(missing_docs)]

pub mod client;
mod error;
#[cfg(windows)]
pub mod installer_gate;
pub mod lock;
pub mod paths;
pub mod transport;
#[cfg(windows)]
mod windows_security;

pub use error::{ClientError, ClientResult};
#[cfg(windows)]
pub use windows_security::{CurrentUserProcess, current_user_sid_string};
