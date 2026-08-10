//! Bounded client calls and local IPC transport for `platonic serve`.
//!
//! This crate owns daemon connection mechanics, request deadlines, endpoint
//! discovery and lock metadata. Daemon
//! run and server semantics remain in `plato-agent`.

#![deny(unsafe_code)]
#![warn(missing_docs)]

pub mod client;
mod error;
pub mod lock;
pub mod paths;
pub mod transport;

pub use error::{ClientError, ClientResult};
