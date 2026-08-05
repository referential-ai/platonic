#![allow(unsafe_code)]

//! Current-user Windows installer and daemon-startup exclusion gate.

use crate::windows_security;
use interprocess::os::windows::security_descriptor::AsSecurityDescriptorExt;
use std::{
    io, mem,
    os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
    ptr,
};
use widestring::U16CString;
use windows_sys::Win32::{
    Foundation::{
        ERROR_ALREADY_EXISTS, ERROR_SUCCESS, GetLastError, SetLastError, WAIT_ABANDONED_0,
        WAIT_FAILED, WAIT_OBJECT_0, WAIT_TIMEOUT,
    },
    Security::SECURITY_ATTRIBUTES,
    System::Threading::{CreateMutexW, ReleaseMutex, WaitForSingleObject},
};

const INSTALLER_GATE_PREFIX: &str = r"Global\plato-agent-installer";
const DAEMON_STARTUP_WAIT_MS: u32 = 5_000;

#[derive(Debug)]
/// Owned current-user installer startup gate.
pub struct InstallerStartupGate {
    handle: OwnedHandle,
}

impl InstallerStartupGate {
    /// Acquires the gate immediately for installer or desktop startup.
    pub fn acquire() -> io::Result<Self> {
        Self::acquire_with_wait(0)
    }

    /// Waits up to the existing daemon-startup bound to acquire the gate.
    pub fn acquire_for_daemon_startup() -> io::Result<Self> {
        Self::acquire_with_wait(DAEMON_STARTUP_WAIT_MS)
    }

    fn acquire_with_wait(wait_ms: u32) -> io::Result<Self> {
        let descriptor = windows_security::current_user_pipe_descriptor()?;
        let mut attributes = SECURITY_ATTRIBUTES {
            nLength: mem::size_of::<SECURITY_ATTRIBUTES>()
                .try_into()
                .expect("SECURITY_ATTRIBUTES size fits u32"),
            lpSecurityDescriptor: ptr::null_mut(),
            bInheritHandle: 0,
        };
        descriptor.write_to_security_attributes(&mut attributes);
        let sid = windows_security::current_user_sid_string()?;
        let name = U16CString::from_str(format!("{INSTALLER_GATE_PREFIX}-{sid}"))
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;

        // Clear stale thread state so a new mutex cannot be misclassified as existing.
        unsafe { SetLastError(ERROR_SUCCESS) };
        // SAFETY: attributes and name remain live for this call; the returned handle is owned.
        let handle = unsafe { CreateMutexW(&attributes, 1, name.as_ptr()) };
        // SAFETY: this immediately captures the status from CreateMutexW above.
        let create_error = unsafe { GetLastError() };
        if handle.is_null() {
            return Err(io::Error::from_raw_os_error(create_error as i32));
        }
        // SAFETY: CreateMutexW returned a new owned handle.
        let handle = unsafe { OwnedHandle::from_raw_handle(handle) };

        if create_error == ERROR_ALREADY_EXISTS {
            windows_security::validate_current_user_kernel_object(handle.as_raw_handle())?;
            // SAFETY: handle is a live mutex handle. Existing mutexes ignore initial ownership.
            match unsafe { WaitForSingleObject(handle.as_raw_handle(), wait_ms) } {
                WAIT_OBJECT_0 | WAIT_ABANDONED_0 => {}
                WAIT_TIMEOUT => {
                    return Err(io::Error::new(
                        if wait_ms == 0 {
                            io::ErrorKind::WouldBlock
                        } else {
                            io::ErrorKind::TimedOut
                        },
                        "Plato Agent installation or update is in progress",
                    ));
                }
                WAIT_FAILED => return Err(io::Error::last_os_error()),
                result => {
                    return Err(io::Error::other(format!(
                        "unexpected installer gate wait result: {result}"
                    )));
                }
            }
        }

        Ok(Self { handle })
    }
}

impl Drop for InstallerStartupGate {
    fn drop(&mut self) {
        // SAFETY: acquire returns only while this thread owns the live mutex.
        unsafe {
            ReleaseMutex(self.handle.as_raw_handle());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn installer_gate_excludes_another_startup_thread() {
        let gate = InstallerStartupGate::acquire().unwrap();
        let error = std::thread::spawn(|| InstallerStartupGate::acquire().unwrap_err())
            .join()
            .unwrap();

        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        drop(gate);

        std::thread::spawn(|| InstallerStartupGate::acquire().unwrap())
            .join()
            .unwrap();
    }
}
