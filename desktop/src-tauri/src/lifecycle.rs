#![allow(unsafe_code)]

use interprocess::os::windows::security_descriptor::{AsSecurityDescriptorExt, SecurityDescriptor};
use std::{
    fmt, io, mem,
    os::windows::{
        ffi::OsStrExt,
        io::{AsRawHandle, FromRawHandle, OwnedHandle},
        process::CommandExt,
    },
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    ptr,
};
use widestring::U16CString;
use windows_sys::Win32::{
    Foundation::{
        ERROR_ALREADY_EXISTS, ERROR_INSUFFICIENT_BUFFER, ERROR_SUCCESS, GetLastError, HANDLE,
        LocalFree, SetLastError,
    },
    Security::{
        Authorization::{ConvertSidToStringSidW, GetSecurityInfo, SE_KERNEL_OBJECT},
        EqualSid, GetTokenInformation, OWNER_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR,
        SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER, TokenUser,
    },
    Storage::FileSystem::{MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW},
    System::{
        IO::CancelSynchronousIo,
        Threading::{
            CREATE_NEW_PROCESS_GROUP, CreateMutexW, DETACHED_PROCESS, GetCurrentProcess,
            OpenProcessToken,
        },
    },
};

const MUTEX_PREFIX: &str = r"Global\plato-desktop-";
const DAEMON_EXECUTABLE: &str = "platonic.exe";

#[derive(Debug)]
pub(crate) struct WorkspaceInstance {
    workspace_id: String,
    _handle: OwnedHandle,
}

impl WorkspaceInstance {
    pub(crate) fn acquire(workspace_id: &str) -> Result<Self, WorkspaceInstanceError> {
        let current_user = current_user()?;
        let sid = current_user.sid_string()?;
        let descriptor = current_user_descriptor(&sid)?;
        let mut attributes = SECURITY_ATTRIBUTES {
            nLength: mem::size_of::<SECURITY_ATTRIBUTES>()
                .try_into()
                .expect("SECURITY_ATTRIBUTES size fits u32"),
            lpSecurityDescriptor: ptr::null_mut(),
            bInheritHandle: 0,
        };
        descriptor.write_to_security_attributes(&mut attributes);
        let name = wide(
            format!("{MUTEX_PREFIX}{workspace_id}"),
            "workspace ID contains a NUL",
        )?;

        // CreateMutexW reports an existing object through the thread's last-error value.
        // Clear stale state first so a newly created mutex cannot be misclassified.
        unsafe { SetLastError(ERROR_SUCCESS) };
        // SAFETY: name is NUL-terminated and attributes borrows the live descriptor above.
        let handle = unsafe { CreateMutexW(&attributes, 0, name.as_ptr()) };
        // SAFETY: this immediately captures the status from CreateMutexW above.
        let create_error = unsafe { GetLastError() };
        if handle.is_null() {
            return Err(io::Error::from_raw_os_error(create_error as i32).into());
        }
        // SAFETY: CreateMutexW returned a new owned handle.
        let handle = unsafe { OwnedHandle::from_raw_handle(handle) };

        if create_error == ERROR_ALREADY_EXISTS {
            validate_owner(&handle, current_user.sid())?;
            return Err(WorkspaceInstanceError::AlreadyOpen {
                workspace_id: workspace_id.to_owned(),
            });
        }

        Ok(Self {
            workspace_id: workspace_id.to_owned(),
            _handle: handle,
        })
    }

    pub(crate) fn workspace_id(&self) -> &str {
        &self.workspace_id
    }
}

#[derive(Debug)]
pub(crate) enum WorkspaceInstanceError {
    AlreadyOpen { workspace_id: String },
    Io(io::Error),
}

impl fmt::Display for WorkspaceInstanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyOpen { workspace_id } => {
                write!(formatter, "workspace {workspace_id} is already open")
            }
            Self::Io(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for WorkspaceInstanceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::AlreadyOpen { .. } => None,
            Self::Io(error) => Some(error),
        }
    }
}

impl From<io::Error> for WorkspaceInstanceError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub(crate) fn sibling_daemon_executable() -> io::Result<PathBuf> {
    let executable = std::env::current_exe()?;
    let parent = executable
        .parent()
        .ok_or_else(|| io::Error::other("desktop executable has no parent directory"))?;
    Ok(parent.join(DAEMON_EXECUTABLE))
}

pub(crate) fn spawn_detached_daemon(
    executable: &Path,
    canonical_workspace_root: &Path,
) -> io::Result<Child> {
    if !executable.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "daemon executable path must be absolute",
        ));
    }
    if !canonical_workspace_root.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "workspace root must be absolute",
        ));
    }

    let mut command = Command::new(executable);
    command
        .arg("serve")
        .current_dir(canonical_workspace_root)
        .creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command.spawn()
}

pub(crate) fn replace_file(from: &Path, to: &Path) -> io::Result<()> {
    let from = wide_path(from, "temporary workspace path contains a NUL")?;
    let to = wide_path(to, "saved workspace path contains a NUL")?;
    // SAFETY: both paths are live NUL-terminated UTF-16 strings for this call.
    if unsafe {
        MoveFileExW(
            from.as_ptr(),
            to.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

pub(crate) fn cancel_synchronous_io<T>(thread: &std::thread::JoinHandle<T>) -> io::Result<()> {
    // SAFETY: the join handle keeps its native thread handle live for this call.
    if unsafe { CancelSynchronousIo(thread.as_raw_handle()) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn current_user_descriptor(sid: &str) -> io::Result<SecurityDescriptor> {
    let descriptor = wide(
        format!("O:{sid}D:P(A;;GA;;;{sid})"),
        "current-user security descriptor contains a NUL",
    )?;
    SecurityDescriptor::deserialize(&descriptor)
}

fn current_user() -> io::Result<TokenUserBuffer> {
    let mut token: HANDLE = ptr::null_mut();
    // SAFETY: token points to writable storage and is wrapped immediately on success.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: OpenProcessToken returned a new owned handle.
    let token = unsafe { OwnedHandle::from_raw_handle(token) };
    let mut bytes = 0;
    // SAFETY: the documented zero-length query writes only the required byte count.
    let result = unsafe {
        GetTokenInformation(
            token.as_raw_handle(),
            TokenUser,
            ptr::null_mut(),
            0,
            &mut bytes,
        )
    };
    if result != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "token size query unexpectedly succeeded",
        ));
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() != Some(ERROR_INSUFFICIENT_BUFFER as i32) {
        return Err(error);
    }
    if bytes == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "token size query returned no bytes",
        ));
    }

    let words = (bytes as usize).div_ceil(mem::size_of::<usize>());
    let mut buffer = vec![0usize; words];
    // SAFETY: the aligned buffer has at least the byte count returned by the size query.
    if unsafe {
        GetTokenInformation(
            token.as_raw_handle(),
            TokenUser,
            buffer.as_mut_ptr().cast(),
            bytes,
            &mut bytes,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(TokenUserBuffer { buffer })
}

fn validate_owner(handle: &OwnedHandle, current_user: *mut core::ffi::c_void) -> io::Result<()> {
    let mut owner = ptr::null_mut();
    let mut security_descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
    // SAFETY: handle is live and all requested output pointers refer to writable storage.
    let result = unsafe {
        GetSecurityInfo(
            handle.as_raw_handle(),
            SE_KERNEL_OBJECT,
            OWNER_SECURITY_INFORMATION,
            &mut owner,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            &mut security_descriptor,
        )
    };
    if result != ERROR_SUCCESS {
        return Err(io::Error::from_raw_os_error(result as i32));
    }
    if security_descriptor.is_null() || owner.is_null() {
        if !security_descriptor.is_null() {
            // SAFETY: GetSecurityInfo allocated this descriptor with LocalAlloc.
            unsafe { LocalFree(security_descriptor.cast()) };
        }
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "workspace mutex has no owner",
        ));
    }
    let security_descriptor = LocalSecurityDescriptor(security_descriptor);
    // SAFETY: both SID pointers remain live for this comparison.
    if unsafe { EqualSid(current_user, owner) } == 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "workspace mutex is not owned by the current user",
        ));
    }
    drop(security_descriptor);
    Ok(())
}

fn wide(value: String, nul_message: &'static str) -> io::Result<U16CString> {
    U16CString::from_str(value)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, nul_message))
}

fn wide_path(path: &Path, nul_message: &'static str) -> io::Result<Vec<u16>> {
    let mut value = path.as_os_str().encode_wide().collect::<Vec<_>>();
    if value.contains(&0) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, nul_message));
    }
    value.push(0);
    Ok(value)
}

struct TokenUserBuffer {
    buffer: Vec<usize>,
}

impl TokenUserBuffer {
    fn sid(&self) -> *mut core::ffi::c_void {
        // SAFETY: GetTokenInformation initialized the aligned buffer as TOKEN_USER.
        unsafe { (*(self.buffer.as_ptr().cast::<TOKEN_USER>())).User.Sid }
    }

    fn sid_string(&self) -> io::Result<String> {
        let mut raw = ptr::null_mut();
        // SAFETY: self.sid() belongs to this live token buffer; raw is writable output storage.
        if unsafe { ConvertSidToStringSidW(self.sid(), &mut raw) } == 0 {
            return Err(io::Error::last_os_error());
        }
        if raw.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "SID conversion returned a null string",
            ));
        }
        let raw = LocalWideString(raw);
        let mut len = 0;
        // SAFETY: ConvertSidToStringSidW returns a NUL-terminated LocalAlloc string.
        while unsafe { *raw.0.add(len) } != 0 {
            len += 1;
        }
        // SAFETY: len was measured within the API-owned NUL-terminated string.
        String::from_utf16(unsafe { std::slice::from_raw_parts(raw.0, len) })
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }
}

struct LocalSecurityDescriptor(PSECURITY_DESCRIPTOR);

impl Drop for LocalSecurityDescriptor {
    fn drop(&mut self) {
        // SAFETY: GetSecurityInfo allocated this descriptor with LocalAlloc.
        unsafe { LocalFree(self.0.cast()) };
    }
}

struct LocalWideString(*mut u16);

impl Drop for LocalWideString {
    fn drop(&mut self) {
        // SAFETY: ConvertSidToStringSidW allocated this string with LocalAlloc.
        unsafe { LocalFree(self.0.cast()) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn same_workspace_is_already_open() {
        let workspace_id = unique_workspace_id();
        let instance = WorkspaceInstance::acquire(&workspace_id).unwrap();

        let error = WorkspaceInstance::acquire(&workspace_id).unwrap_err();

        assert_eq!(instance.workspace_id(), workspace_id);
        assert!(matches!(
            error,
            WorkspaceInstanceError::AlreadyOpen {
                workspace_id: existing
            } if existing == workspace_id
        ));
    }

    #[test]
    fn different_workspaces_have_independent_instances() {
        let first_id = unique_workspace_id();
        let second_id = unique_workspace_id();

        let first = WorkspaceInstance::acquire(&first_id).unwrap();
        let second = WorkspaceInstance::acquire(&second_id).unwrap();

        assert_eq!(first.workspace_id(), first_id);
        assert_eq!(second.workspace_id(), second_id);
    }

    #[test]
    fn dropped_instance_can_be_reacquired() {
        let workspace_id = unique_workspace_id();
        drop(WorkspaceInstance::acquire(&workspace_id).unwrap());

        let replacement = WorkspaceInstance::acquire(&workspace_id).unwrap();

        assert_eq!(replacement.workspace_id(), workspace_id);
    }

    #[test]
    fn daemon_executable_is_an_absolute_sibling() {
        let current = std::env::current_exe().unwrap();

        let daemon = sibling_daemon_executable().unwrap();

        assert!(daemon.is_absolute());
        assert_eq!(daemon.parent(), current.parent());
        assert_eq!(daemon.file_name().unwrap(), DAEMON_EXECUTABLE);
    }

    #[test]
    fn saved_workspace_file_is_replaced() {
        let directory = tempfile::tempdir().unwrap();
        let saved = directory.path().join("workspace.json");
        let temporary = directory.path().join("workspace.json.tmp");
        std::fs::write(&saved, "old").unwrap();
        std::fs::write(&temporary, "new").unwrap();

        replace_file(&temporary, &saved).unwrap();

        assert_eq!(std::fs::read_to_string(saved).unwrap(), "new");
        assert!(!temporary.exists());
    }

    fn unique_workspace_id() -> String {
        format!(
            "test-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        )
    }
}
