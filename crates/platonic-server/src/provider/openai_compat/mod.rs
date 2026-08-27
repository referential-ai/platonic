mod client;
mod response;
mod responses;
mod stream;
mod types;

pub use client::OpenAiCompatibleClient;

#[cfg(test)]
pub(crate) use response::with_response_read_cancel_observer;
