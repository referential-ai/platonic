#![allow(unsafe_code)]

use interprocess::os::windows::security_descriptor::{AsSecurityDescriptorExt, SecurityDescriptor};
pub(crate) use plato_daemon_client::CurrentUserProcess;
use std::{
    fs::File,
    io, mem,
    os::windows::{
        ffi::{OsStrExt, OsStringExt},
        io::{AsRawHandle, FromRawHandle},
    },
    path::{Component, Path, PathBuf, Prefix},
    ptr,
};
use widestring::U16CString;
use windows_sys::Win32::{
    Foundation::{GENERIC_READ, GENERIC_WRITE, INVALID_HANDLE_VALUE},
    Security::SECURITY_ATTRIBUTES,
    Storage::FileSystem::{
        CREATE_NEW, CreateFileW, DELETE, FILE_ATTRIBUTE_NORMAL, FILE_FLAG_DELETE_ON_CLOSE,
        FILE_FLAG_OPEN_REPARSE_POINT, FILE_ID_INFO, FILE_SHARE_DELETE, FILE_SHARE_READ,
        FILE_SHARE_WRITE, FileIdInfo, GetDriveTypeW, GetFileInformationByHandleEx, OPEN_EXISTING,
    },
    System::{
        SystemInformation::GetSystemDirectoryW,
        WindowsProgramming::{DRIVE_CDROM, DRIVE_FIXED, DRIVE_RAMDISK, DRIVE_REMOVABLE},
    },
};

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
    let sid = plato_daemon_client::current_user_sid_string()?;
    let descriptor = U16CString::from_str(format!("O:{sid}D:P(A;;{rights};;;{sid})"))
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    SecurityDescriptor::deserialize(&descriptor)
}

fn path_wide(path: &Path, nul_message: &'static str) -> io::Result<Vec<u16>> {
    let mut path_wide: Vec<u16> = path.as_os_str().encode_wide().collect();
    if path_wide.contains(&0) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, nul_message));
    }
    path_wide.push(0);
    Ok(path_wide)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

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
