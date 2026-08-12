mod child;
mod messages;
mod supervisor;

pub use child::run_stdio_child;
#[allow(unused_imports)]
pub(super) use supervisor::{ChildLifecycleLimits, SupervisedRunCompletion, run_supervised};

#[cfg(all(test, target_os = "linux"))]
pub(super) use supervisor::{SupervisedTestLaunch, TerminalStageBarriers, run_supervised_for_test};
