#![allow(unsafe_code)]

use interprocess::os::windows::security_descriptor::{AsSecurityDescriptorExt, SecurityDescriptor};
use std::{
    fs::File,
    io, mem,
    os::windows::{
        ffi::{OsStrExt, OsStringExt},
        io::{AsRawHandle, FromRawHandle},
    },
    path::{Component, Path, PathBuf, Prefix},
    ptr,
    time::{Duration, Instant},
};
use widestring::U16CString;
use windows_sys::Win32::{
    Foundation::{
        CloseHandle, ERROR_INSUFFICIENT_BUFFER, ERROR_INVALID_PARAMETER, ERROR_PIPE_BUSY,
        ERROR_SEM_TIMEOUT, GENERIC_READ, GENERIC_WRITE, HANDLE, INVALID_HANDLE_VALUE, LocalFree,
        WAIT_FAILED, WAIT_OBJECT_0, WAIT_TIMEOUT,
    },
    Security::{
        Authorization::{ConvertSidToStringSidW, GetSecurityInfo, SE_KERNEL_OBJECT},
        EqualSid, GetTokenInformation, OWNER_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR,
        SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER, TokenUser,
    },
    Storage::FileSystem::{
        CREATE_NEW, CreateFileW, DELETE, FILE_ATTRIBUTE_NORMAL, FILE_FLAG_DELETE_ON_CLOSE,
        FILE_FLAG_OPEN_REPARSE_POINT, FILE_ID_INFO, FILE_SHARE_DELETE, FILE_SHARE_READ,
        FILE_SHARE_WRITE, FileIdInfo, GetDriveTypeW, GetFileInformationByHandleEx, OPEN_EXISTING,
        SECURITY_IDENTIFICATION, SECURITY_SQOS_PRESENT,
    },
    System::{
        Pipes::{
            GetNamedPipeServerProcessId, PIPE_NOWAIT, PIPE_WAIT, SetNamedPipeHandleState,
            WaitNamedPipeW,
        },
        SystemInformation::GetSystemDirectoryW,
        Threading::{
            GetCurrentProcess, OpenProcess, OpenProcessToken, PROCESS_QUERY_LIMITED_INFORMATION,
            PROCESS_SYNCHRONIZE, QueryFullProcessImageNameW, WaitForSingleObject,
        },
        WindowsProgramming::{DRIVE_CDROM, DRIVE_FIXED, DRIVE_RAMDISK, DRIVE_REMOVABLE},
    },
};

pub(crate) struct CurrentUserProcess {
    handle: WinHandle,
}

impl CurrentUserProcess {
    pub(crate) fn open(pid: u32) -> io::Result<Option<Self>> {
        // SAFETY: the returned process handle is checked and owned on success.
        let handle = unsafe {
            OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
                0,
                pid,
            )
        };
        if handle.is_null() {
            let error = io::Error::last_os_error();
            return if error.raw_os_error() == Some(ERROR_INVALID_PARAMETER as i32) {
                Ok(None)
            } else {
                Err(error)
            };
        }
        let process = Self {
            handle: WinHandle(handle),
        };
        if !process.is_running()? {
            return Ok(None);
        }

        let current_user = process_user(current_process())?;
        let process_user = match process_user(process.handle.0) {
            Ok(user) => user,
            Err(_) if !process.is_running()? => return Ok(None),
            Err(error) => return Err(error),
        };
        // SAFETY: both SID pointers borrow live TOKEN_USER buffers.
        if unsafe { EqualSid(current_user.sid(), process_user.sid()) } == 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "process is not owned by the current user",
            ));
        }
        if !process.is_running()? {
            return Ok(None);
        }
        Ok(Some(process))
    }

    pub(crate) fn executable(&self) -> io::Result<PathBuf> {
        let mut buffer = vec![0u16; 260];
        loop {
            let mut len = buffer
                .len()
                .try_into()
                .map_err(|_| io::Error::other("process image path is too long"))?;
            // SAFETY: self holds a live queryable process handle and buffer is writable.
            if unsafe {
                QueryFullProcessImageNameW(self.handle.0, 0, buffer.as_mut_ptr(), &mut len)
            } != 0
            {
                let len = len as usize;
                return Ok(PathBuf::from(std::ffi::OsString::from_wide(&buffer[..len])));
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(ERROR_INSUFFICIENT_BUFFER as i32) {
                return Err(error);
            }
            buffer.resize(buffer.len().saturating_mul(2), 0);
        }
    }

    pub(crate) fn is_running(&self) -> io::Result<bool> {
        // SAFETY: self owns a process handle opened with PROCESS_SYNCHRONIZE.
        match unsafe { WaitForSingleObject(self.handle.0, 0) } {
            WAIT_TIMEOUT => Ok(true),
            WAIT_OBJECT_0 => Ok(false),
            WAIT_FAILED => Err(io::Error::last_os_error()),
            result => Err(io::Error::other(format!(
                "unexpected process wait result: {result}"
            ))),
        }
    }

    pub(crate) fn wait_until(&self, deadline: Instant) -> io::Result<bool> {
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(!self.is_running()?);
            }
            let wait_ms = remaining.as_millis().clamp(1, u32::MAX as u128) as u32;
            // SAFETY: self owns a process handle opened with PROCESS_SYNCHRONIZE.
            match unsafe { WaitForSingleObject(self.handle.0, wait_ms) } {
                WAIT_OBJECT_0 => return Ok(true),
                WAIT_TIMEOUT => {
                    if Instant::now() >= deadline {
                        return Ok(false);
                    }
                }
                WAIT_FAILED => return Err(io::Error::last_os_error()),
                result => {
                    return Err(io::Error::other(format!(
                        "unexpected process wait result: {result}"
                    )));
                }
            }
        }
    }
}

#[cfg(test)]
pub(crate) fn same_file(left: &Path, right: &Path) -> io::Result<bool> {
    let left = open_file_for_identity(left)?;
    let right = open_file_for_identity(right)?;
    same_file_handles(&left, &right)
}

pub(crate) fn same_file_handles(left: &File, right: &File) -> io::Result<bool> {
    let left = file_identity(left)?;
    let right = file_identity(right)?;
    Ok(left.VolumeSerialNumber == right.VolumeSerialNumber
        && left.FileId.Identifier == right.FileId.Identifier)
}

pub(crate) fn same_file_handle_path(file: &File, path: &Path) -> io::Result<bool> {
    let current = open_file_for_identity(path)?;
    same_file_handles(file, &current)
}

fn file_identity(file: &File) -> io::Result<FILE_ID_INFO> {
    let mut info = FILE_ID_INFO::default();
    // SAFETY: file is live and info is writable storage of the declared size.
    if unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle(),
            FileIdInfo,
            (&mut info as *mut FILE_ID_INFO).cast(),
            mem::size_of::<FILE_ID_INFO>()
                .try_into()
                .expect("FILE_ID_INFO size fits u32"),
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(info)
}

pub(crate) fn is_local_disk_path(path: &Path) -> io::Result<bool> {
    let drive = match path.components().next() {
        Some(Component::Prefix(prefix)) => match prefix.kind() {
            Prefix::Disk(drive) | Prefix::VerbatimDisk(drive) => drive,
            _ => return Ok(false),
        },
        _ => return Ok(false),
    };
    let root = format!("{}:\\", drive as char);
    let root = U16CString::from_str(&root)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "drive path contains a NUL"))?;
    // SAFETY: root is a live NUL-terminated drive-root path.
    Ok(matches!(
        unsafe { GetDriveTypeW(root.as_ptr()) },
        DRIVE_REMOVABLE | DRIVE_FIXED | DRIVE_CDROM | DRIVE_RAMDISK
    ))
}

pub(crate) fn current_user_pipe_descriptor() -> io::Result<SecurityDescriptor> {
    current_user_descriptor("GA")
}

pub(crate) fn validate_current_user_kernel_object(handle: HANDLE) -> io::Result<()> {
    let current_user = process_user(current_process())?;
    let mut owner = ptr::null_mut();
    let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
    // SAFETY: handle is live and both requested output pointers refer to writable storage.
    let result = unsafe {
        GetSecurityInfo(
            handle,
            SE_KERNEL_OBJECT,
            OWNER_SECURITY_INFORMATION,
            &mut owner,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            &mut descriptor,
        )
    };
    if result != 0 {
        return Err(io::Error::from_raw_os_error(result as i32));
    }
    if descriptor.is_null() || owner.is_null() {
        if !descriptor.is_null() {
            // SAFETY: GetSecurityInfo allocated this descriptor with LocalAlloc.
            unsafe { LocalFree(descriptor.cast()) };
        }
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "kernel object has no owner",
        ));
    }
    let descriptor = LocalSecurityDescriptor(descriptor);
    // SAFETY: both SID pointers remain live for this comparison.
    if unsafe { EqualSid(current_user.sid(), owner) } == 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "kernel object is not owned by the current user",
        ));
    }
    drop(descriptor);
    Ok(())
}

pub(crate) fn create_current_user_file(path: &Path) -> io::Result<File> {
    let descriptor = current_user_descriptor("FA")?;
    let mut attributes = SECURITY_ATTRIBUTES {
        nLength: mem::size_of::<SECURITY_ATTRIBUTES>()
            .try_into()
            .expect("SECURITY_ATTRIBUTES size fits u32"),
        lpSecurityDescriptor: ptr::null_mut(),
        bInheritHandle: 0,
    };
    descriptor.write_to_security_attributes(&mut attributes);

    let mut path_wide: Vec<u16> = path.as_os_str().encode_wide().collect();
    if path_wide.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Windows lock path contains a NUL",
        ));
    }
    path_wide.push(0);

    // SAFETY: path_wide is NUL-terminated, attributes borrows a live descriptor,
    // and the returned owned handle is checked before conversion to File.
    let handle = unsafe {
        CreateFileW(
            path_wide.as_ptr(),
            GENERIC_WRITE | DELETE,
            FILE_SHARE_READ,
            &attributes,
            CREATE_NEW,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_DELETE_ON_CLOSE,
            ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: CreateFileW returned a new owned handle and File assumes that ownership.
    Ok(unsafe { File::from_raw_handle(handle) })
}

pub(crate) fn open_lock_file_for_read(path: &Path) -> io::Result<File> {
    open_file_for_identity(path)
}

pub(crate) fn open_file_for_identity(path: &Path) -> io::Result<File> {
    let path_wide = path_wide(path, "Windows lock path contains a NUL")?;
    // SAFETY: path_wide is NUL-terminated and the returned owned handle is checked.
    let handle = unsafe {
        CreateFileW(
            path_wide.as_ptr(),
            GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            ptr::null_mut(),
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT,
            ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: CreateFileW returned a new owned handle and File assumes that ownership.
    Ok(unsafe { File::from_raw_handle(handle) })
}

pub(crate) fn connect_current_user_pipe(path: &Path) -> io::Result<File> {
    connect_current_user_pipe_inner(path, None, Duration::from_secs(1))
}

pub(crate) fn connect_current_user_pipe_with_timeout(
    path: &Path,
    timeout: Duration,
) -> io::Result<File> {
    let pipe = connect_current_user_pipe_inner(path, None, timeout)?;
    set_pipe_nowait(&pipe)?;
    Ok(pipe)
}

pub(crate) fn connect_current_user_pipe_for_pid(
    path: &Path,
    expected_server_pid: u32,
) -> io::Result<File> {
    let pipe =
        connect_current_user_pipe_inner(path, Some(expected_server_pid), Duration::from_secs(1))?;
    set_pipe_nowait(&pipe)?;
    Ok(pipe)
}

fn set_pipe_nowait(pipe: &File) -> io::Result<()> {
    set_pipe_mode(pipe, PIPE_NOWAIT)
}

pub(crate) fn set_pipe_wait(pipe: &File) -> io::Result<()> {
    set_pipe_mode(pipe, PIPE_WAIT)
}

fn set_pipe_mode(pipe: &File, mode: u32) -> io::Result<()> {
    // SAFETY: pipe is a live client pipe handle and mode is readable for the call.
    if unsafe {
        SetNamedPipeHandleState(
            pipe.as_raw_handle(),
            &mode,
            ptr::null_mut(),
            ptr::null_mut(),
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn connect_current_user_pipe_inner(
    path: &Path,
    expected_server_pid: Option<u32>,
    timeout: Duration,
) -> io::Result<File> {
    let path_wide = path_wide(path, "Windows pipe path contains a NUL")?;
    let deadline = Instant::now() + timeout;
    let handle = loop {
        // SAFETY: path_wide is NUL-terminated; the returned owned handle is checked below.
        let handle = unsafe {
            CreateFileW(
                path_wide.as_ptr(),
                GENERIC_READ | GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                ptr::null_mut(),
                OPEN_EXISTING,
                SECURITY_SQOS_PRESENT | SECURITY_IDENTIFICATION,
                ptr::null_mut(),
            )
        };
        if handle != INVALID_HANDLE_VALUE {
            // SAFETY: CreateFileW returned a new owned handle.
            break unsafe { File::from_raw_handle(handle) };
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(ERROR_PIPE_BUSY as i32) {
            return Err(error);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(pipe_connect_timeout());
        }
        let wait_ms = remaining.as_millis().clamp(1, u32::MAX as u128) as u32;
        // SAFETY: path_wide remains a live NUL-terminated string for this call.
        if unsafe { WaitNamedPipeW(path_wide.as_ptr(), wait_ms) } == 0 {
            let error = io::Error::last_os_error();
            return if error.raw_os_error() == Some(ERROR_SEM_TIMEOUT as i32) {
                Err(pipe_connect_timeout())
            } else {
                Err(error)
            };
        }
    };

    validate_pipe_server(&handle, expected_server_pid)?;
    Ok(handle)
}

fn pipe_connect_timeout() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "named-pipe connection timed out")
}

pub(crate) fn system_cmd_path() -> io::Result<PathBuf> {
    let mut buffer = vec![0u16; 260];
    loop {
        // SAFETY: buffer exposes writable UTF-16 storage for its reported capacity.
        let len = unsafe {
            GetSystemDirectoryW(
                buffer.as_mut_ptr(),
                buffer
                    .len()
                    .try_into()
                    .map_err(|_| io::Error::other("Windows system path is too long"))?,
            )
        };
        if len == 0 {
            return Err(io::Error::last_os_error());
        }
        let len = len as usize;
        if len < buffer.len() {
            let root = std::ffi::OsString::from_wide(&buffer[..len]);
            return Ok(PathBuf::from(root).join("cmd.exe"));
        }
        buffer.resize(len + 1, 0);
    }
}

fn current_user_descriptor(rights: &str) -> io::Result<SecurityDescriptor> {
    let sid = current_user_sid_string()?;
    let descriptor = U16CString::from_str(format!("O:{sid}D:P(A;;{rights};;;{sid})"))
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    SecurityDescriptor::deserialize(&descriptor)
}

pub(crate) fn current_user_sid_string() -> io::Result<String> {
    let user = process_user(current_process())?;
    user.sid_string()
}

fn process_user(process: HANDLE) -> io::Result<TokenUserBuffer> {
    let mut token: HANDLE = ptr::null_mut();
    // SAFETY: token points to writable storage and is wrapped immediately on success.
    if unsafe { OpenProcessToken(process, TOKEN_QUERY, &mut token) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let token = WinHandle(token);
    let mut bytes = 0;
    // SAFETY: the documented zero-length query writes only the required byte count.
    let result = unsafe { GetTokenInformation(token.0, TokenUser, ptr::null_mut(), 0, &mut bytes) };
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

    let words = (bytes as usize).div_ceil(std::mem::size_of::<usize>());
    let mut buffer = vec![0usize; words];
    // SAFETY: the aligned buffer has at least the byte count returned by the size query.
    if unsafe {
        GetTokenInformation(
            token.0,
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

fn validate_pipe_server(handle: &File, expected_server_pid: Option<u32>) -> io::Result<()> {
    let raw = handle.as_raw_handle();
    let mut first_pid = 0;
    // SAFETY: raw is a live connected pipe handle and first_pid is writable output storage.
    if unsafe { GetNamedPipeServerProcessId(raw, &mut first_pid) } == 0 || first_pid == 0 {
        return Err(pipe_server_identity_error());
    }
    if expected_server_pid.is_some_and(|expected| expected != first_pid) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "named-pipe server process does not match lock metadata",
        ));
    }
    let Some(process) =
        CurrentUserProcess::open(first_pid).map_err(|_| pipe_server_identity_error())?
    else {
        return Err(pipe_server_identity_error());
    };
    let mut second_pid = 0;
    // SAFETY: raw remains live and second_pid is writable output storage.
    if unsafe { GetNamedPipeServerProcessId(raw, &mut second_pid) } == 0 || second_pid != first_pid
    {
        return Err(pipe_server_identity_error());
    }
    if !process.is_running()? {
        return Err(pipe_server_identity_error());
    }
    Ok(())
}

fn current_process() -> HANDLE {
    // SAFETY: GetCurrentProcess always returns the calling process's pseudo-handle.
    unsafe { GetCurrentProcess() }
}

fn pipe_server_identity_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "named-pipe server is not owned by the current user",
    )
}

fn path_wide(path: &Path, nul_message: &'static str) -> io::Result<Vec<u16>> {
    let mut path_wide: Vec<u16> = path.as_os_str().encode_wide().collect();
    if path_wide.contains(&0) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, nul_message));
    }
    path_wide.push(0);
    Ok(path_wide)
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

struct WinHandle(HANDLE);

impl Drop for WinHandle {
    fn drop(&mut self) {
        // SAFETY: this wrapper owns a successful Win32 handle.
        unsafe {
            CloseHandle(self.0);
        }
    }
}

struct LocalWideString(*mut u16);

impl Drop for LocalWideString {
    fn drop(&mut self) {
        // SAFETY: this wrapper owns the successful ConvertSidToStringSidW allocation.
        unsafe {
            LocalFree(self.0.cast());
        }
    }
}

struct LocalSecurityDescriptor(PSECURITY_DESCRIPTOR);

impl Drop for LocalSecurityDescriptor {
    fn drop(&mut self) {
        // SAFETY: this wrapper owns the successful GetSecurityInfo allocation.
        unsafe {
            LocalFree(self.0.cast());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_user_sid_is_a_sid_string() {
        assert!(current_user_sid_string().unwrap().starts_with("S-1-"));
    }

    #[test]
    fn creates_current_user_file_atomically_and_deletes_on_close() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent.lock");

        let file = create_current_user_file(&path).unwrap();
        assert!(path.exists());
        let error = create_current_user_file(&path).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        drop(file);
        assert!(!path.exists());
    }

    #[test]
    fn current_user_process_reports_the_current_executable() {
        let process = CurrentUserProcess::open(std::process::id())
            .unwrap()
            .unwrap();

        assert!(process.is_running().unwrap());
        assert!(
            same_file(
                &process.executable().unwrap(),
                &std::env::current_exe().unwrap()
            )
            .unwrap()
        );
        assert!(
            !process
                .wait_until(Instant::now() + Duration::from_millis(10))
                .unwrap()
        );
    }

    #[test]
    fn same_file_uses_file_identity_instead_of_path_spelling() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first.exe");
        let alias = dir.path().join("alias.exe");
        let other = dir.path().join("other.exe");
        std::fs::write(&first, b"first").unwrap();
        std::fs::hard_link(&first, &alias).unwrap();
        std::fs::write(&other, b"other").unwrap();

        assert!(same_file(&first, &alias).unwrap());
        assert!(!same_file(&first, &other).unwrap());
    }

    #[test]
    fn resolves_the_system_command_host() {
        let path = system_cmd_path().unwrap();

        assert_eq!(path.file_name().unwrap(), "cmd.exe");
        assert!(path.is_absolute());
        assert!(path.exists());
    }
}
