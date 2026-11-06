use std::io::{Error, Result};
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::os::windows::process::CommandExt;
use std::process::{Child, Command, ExitStatus};
use std::thread;

use windows::Win32::Foundation::{ERROR_NO_MORE_FILES, HANDLE};
use windows::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
};
use windows::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
    JobObjectBasicAccountingInformation, JobObjectExtendedLimitInformation,
    QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
};
use windows::Win32::System::Threading::{
    CREATE_SUSPENDED, OpenThread, ResumeThread, THREAD_SUSPEND_RESUME,
};
use windows::core::HRESULT;

pub(super) struct AdapterChild {
    child: Child,
    job: OwnedHandle,
    assigned: bool,
    finished: bool,
}

impl AdapterChild {
    pub(super) fn spawn(mut command: Command) -> Result<Self> {
        let job = own(unsafe { CreateJobObjectW(None, None) }.map_err(Error::other)?);
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        unsafe {
            SetInformationJobObject(
                HANDLE(job.as_raw_handle()),
                JobObjectExtendedLimitInformation,
                std::ptr::from_ref(&limits).cast(),
                std::mem::size_of_val(&limits) as u32,
            )
        }
        .map_err(Error::other)?;
        // Assign before the primary thread can run, so even an immediately
        // spawned virtualenv worker inherits the job.
        let child = command.creation_flags(CREATE_SUSPENDED.0).spawn()?;
        let mut owned = Self {
            child,
            job,
            assigned: false,
            finished: false,
        };
        unsafe { AssignProcessToJobObject(owned.handle(), HANDLE(owned.child.as_raw_handle())) }
            .map_err(Error::other)?;
        owned.assigned = true;
        owned.resume_primary_thread()?;
        Ok(owned)
    }

    pub(super) fn try_wait(&mut self) -> Result<Option<ExitStatus>> {
        let status = self.child.try_wait()?;
        if status.is_some() && self.active_processes()? == 0 {
            self.finished = true;
            Ok(status)
        } else {
            Ok(None)
        }
    }

    pub(super) fn terminate_and_reap(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }
        if self.assigned {
            unsafe { TerminateJobObject(self.handle(), 1) }.map_err(Error::other)?;
        } else if self.child.try_wait()?.is_none() {
            self.child.kill()?;
        }
        self.child.wait()?;
        // A completion-port message or a reaped launcher alone does not prove
        // that the job's worker descendants have finished terminating.
        while self.active_processes()? != 0 {
            thread::sleep(super::POLL_INTERVAL);
        }
        self.finished = true;
        Ok(())
    }

    fn handle(&self) -> HANDLE {
        HANDLE(self.job.as_raw_handle())
    }

    fn active_processes(&self) -> Result<u32> {
        let mut accounting = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
        unsafe {
            QueryInformationJobObject(
                Some(self.handle()),
                JobObjectBasicAccountingInformation,
                std::ptr::from_mut(&mut accounting).cast(),
                std::mem::size_of_val(&accounting) as u32,
                None,
            )
        }
        .map_err(Error::other)?;
        Ok(accounting.ActiveProcesses)
    }

    fn resume_primary_thread(&self) -> Result<()> {
        // std::process::Child exposes the process handle, not the primary
        // thread handle. The still-suspended process has only its initial thread.
        let snapshot =
            own(unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) }.map_err(Error::other)?);
        let mut entry = THREADENTRY32 {
            dwSize: std::mem::size_of::<THREADENTRY32>() as u32,
            ..Default::default()
        };
        unsafe { Thread32First(HANDLE(snapshot.as_raw_handle()), &mut entry) }
            .map_err(Error::other)?;
        loop {
            if entry.th32OwnerProcessID == self.child.id() {
                let thread =
                    own(
                        unsafe { OpenThread(THREAD_SUSPEND_RESUME, false, entry.th32ThreadID) }
                            .map_err(Error::other)?,
                    );
                return match unsafe { ResumeThread(HANDLE(thread.as_raw_handle())) } {
                    1 => Ok(()),
                    u32::MAX => Err(Error::last_os_error()),
                    _ => Err(Error::other(
                        "adapter thread has unexpected suspension count",
                    )),
                };
            }
            match unsafe { Thread32Next(HANDLE(snapshot.as_raw_handle()), &mut entry) } {
                Ok(()) => {}
                Err(error) if error.code() == HRESULT::from_win32(ERROR_NO_MORE_FILES.0) => {
                    return Err(Error::other("adapter primary thread not found"));
                }
                Err(error) => return Err(Error::other(error)),
            }
        }
    }
}

impl Drop for AdapterChild {
    fn drop(&mut self) {
        if self.terminate_and_reap().is_err() {
            eprintln!("whisper subprocess cleanup failed: io");
        }
    }
}

fn own(handle: HANDLE) -> OwnedHandle {
    // Only successful Win32 handle-creation calls reach this ownership transfer.
    unsafe { OwnedHandle::from_raw_handle(handle.0) }
}
