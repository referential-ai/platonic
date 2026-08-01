use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use ort::{
    environment::Environment,
    session::{Session, builder::GraphOptimizationLevel},
};
use serde::Serialize;

use crate::OrtRuntimeError;

/// Pinned Rust wrapper and ONNX Runtime line shared by local ONNX engines.
pub const ORT_RUNTIME_VERSION: &str = "ort 2.0.0-rc.13 / ONNX Runtime 1.28";

const MAX_DIAGNOSTIC_CHARS: usize = 2_048;
static NEXT_RUNTIME_OWNER_ID: AtomicU64 = AtomicU64::new(1);

/// Resident inference provider selected during ONNX session construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InferenceBackend {
    /// ONNX Runtime CUDA execution provider, device zero.
    Cuda,
    /// ONNX Runtime CPU execution provider.
    Cpu,
}

/// Process-runtime and resident-session counts for one shared owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct OrtRuntimeMetrics {
    /// Stable process-local identity shared by every clone of this owner.
    pub owner_id: u64,
    /// ONNX environments acquired by this owner. This remains one.
    pub environment_instances: u64,
    /// Successfully constructed resident model sessions.
    pub session_loads: u64,
    /// Resident sessions using the CUDA execution provider.
    pub cuda_sessions: u64,
    /// Resident sessions using the CPU execution provider.
    pub cpu_sessions: u64,
}

struct OrtRuntimeCounters {
    owner_id: u64,
    session_loads: AtomicU64,
    cuda_sessions: AtomicU64,
    cpu_sessions: AtomicU64,
}

/// Cloneable read-only metrics for the shared ONNX runtime owner.
#[derive(Clone)]
pub struct OrtRuntimeMetricsReader {
    counters: Arc<OrtRuntimeCounters>,
}

impl OrtRuntimeMetricsReader {
    /// Reads monotonic environment and session residency counters.
    pub fn snapshot(&self) -> OrtRuntimeMetrics {
        OrtRuntimeMetrics {
            owner_id: self.counters.owner_id,
            environment_instances: 1,
            session_loads: self.counters.session_loads.load(Ordering::Relaxed),
            cuda_sessions: self.counters.cuda_sessions.load(Ordering::Relaxed),
            cpu_sessions: self.counters.cpu_sessions.load(Ordering::Relaxed),
        }
    }
}

/// One explicit owner for the process-global ONNX environment and its sessions.
#[derive(Clone)]
pub struct OrtRuntime {
    environment: Arc<Environment>,
    counters: Arc<OrtRuntimeCounters>,
}

impl OrtRuntime {
    /// Acquires the process-global environment exactly once for this owner.
    pub fn acquire() -> Result<Self, OrtRuntimeError> {
        let environment = Environment::current().map_err(|error| OrtRuntimeError::Environment {
            reason: bounded(&error.to_string()),
        })?;
        Ok(Self {
            environment,
            counters: Arc::new(OrtRuntimeCounters {
                owner_id: NEXT_RUNTIME_OWNER_ID.fetch_add(1, Ordering::Relaxed),
                session_loads: AtomicU64::new(0),
                cuda_sessions: AtomicU64::new(0),
                cpu_sessions: AtomicU64::new(0),
            }),
        })
    }

    /// Returns the stable process-local identity shared by cloned handles.
    pub fn owner_id(&self) -> u64 {
        self.counters.owner_id
    }

    /// Returns a reader that remains valid after model ownership moves to workers.
    pub fn metrics_reader(&self) -> OrtRuntimeMetricsReader {
        OrtRuntimeMetricsReader {
            counters: Arc::clone(&self.counters),
        }
    }

    pub(crate) fn load_session(&self, path: &Path) -> Result<ResidentSession, SessionLoadError> {
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        {
            let cuda = self.cuda_session(path);
            match cuda {
                Ok(session) => Ok(self.record_session(session, InferenceBackend::Cuda, None)),
                Err(cuda) => self
                    .cpu_session(path)
                    .map(|session| {
                        self.record_session(session, InferenceBackend::Cpu, Some(cuda.clone()))
                    })
                    .map_err(|cpu| SessionLoadError::Fallback { cuda, cpu }),
            }
        }

        #[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
        {
            self.cpu_session(path)
                .map(|session| self.record_session(session, InferenceBackend::Cpu, None))
                .map_err(|reason| SessionLoadError::Backend {
                    backend: InferenceBackend::Cpu,
                    reason,
                })
        }
    }

    fn session_builder(&self) -> Result<ort::session::builder::SessionBuilder, String> {
        let current = Environment::current().map_err(|error| bounded(&error.to_string()))?;
        if !Arc::ptr_eq(&self.environment, &current) {
            return Err("ONNX process environment changed after runtime acquisition".to_owned());
        }
        let builder = Session::builder().map_err(|error| bounded(&error.to_string()))?;
        builder
            .with_optimization_level(GraphOptimizationLevel::Level3)
            .map_err(|error| bounded(&error.to_string()))
    }

    fn cpu_session(&self, path: &Path) -> Result<Session, String> {
        let mut builder = self.session_builder()?;
        builder
            .commit_from_file(path)
            .map_err(|error| bounded(&error.to_string()))
    }

    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    fn cuda_session(&self, path: &Path) -> Result<Session, String> {
        let builder = self.session_builder()?;
        let mut builder = builder
            .with_execution_providers([ort::ep::CUDA::default().build().error_on_failure()])
            .map_err(|error| bounded(&error.to_string()))?;
        builder
            .commit_from_file(path)
            .map_err(|error| bounded(&error.to_string()))
    }

    fn record_session(
        &self,
        session: Session,
        backend: InferenceBackend,
        fallback_reason: Option<String>,
    ) -> ResidentSession {
        self.counters.session_loads.fetch_add(1, Ordering::Relaxed);
        match backend {
            InferenceBackend::Cuda => {
                self.counters.cuda_sessions.fetch_add(1, Ordering::Relaxed);
            }
            InferenceBackend::Cpu => {
                self.counters.cpu_sessions.fetch_add(1, Ordering::Relaxed);
            }
        }
        ResidentSession {
            session,
            backend,
            fallback_reason,
        }
    }
}

pub(crate) struct ResidentSession {
    pub(crate) session: Session,
    pub(crate) backend: InferenceBackend,
    pub(crate) fallback_reason: Option<String>,
}

pub(crate) enum SessionLoadError {
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    Fallback { cuda: String, cpu: String },
    #[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
    Backend {
        backend: InferenceBackend,
        reason: String,
    },
}

fn bounded(value: &str) -> String {
    value.chars().take(MAX_DIAGNOSTIC_CHARS).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloned_owner_retains_one_environment_identity_and_counter_set() {
        let runtime = OrtRuntime::acquire().unwrap();
        let clone = runtime.clone();
        assert_eq!(runtime.owner_id(), clone.owner_id());
        assert!(Arc::ptr_eq(&runtime.environment, &clone.environment));
        assert_eq!(
            runtime.metrics_reader().snapshot(),
            OrtRuntimeMetrics {
                owner_id: runtime.owner_id(),
                environment_instances: 1,
                session_loads: 0,
                cuda_sessions: 0,
                cpu_sessions: 0,
            }
        );
    }
}
