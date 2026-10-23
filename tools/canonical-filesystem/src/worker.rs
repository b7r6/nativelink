// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Private Worker-role staging primitives. No worker factory consumes these.
//!
//! Inputs are copied into separate local inodes before action hardlinking;
//! canonical CAS/sink single-link rules remain unchanged. This is filesystem
//! custody, not digest validation, execution authority or durable publication.
//! Participants must honor Ownership and serialize namespace mutations. Named
//! identity checks are not atomic compare-and-link against hostile writers.
//! Dropping a handle closes descriptors and preserves every partial artifact.

use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::OwnedFd;
use std::os::unix::fs::FileExt;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use nativelink_canonical_config::Error;
use rustix::fs::{
    AtFlags, Dir, FileType, Mode, OFlags, RenameFlags, Stat, Timespec, Timestamps, UTIME_OMIT,
    fchmod, fstat, futimens, linkat, mkdirat, openat, renameat_with, statat,
};
use rustix::io::Errno;

use super::{DIRECTORY_FLAGS, OWNERSHIP_RECORD, Ownership, Role, failure};

/// Finite per-file qualification bound, not production or aggregate admission.
pub(super) const QUALIFICATION_MAX_INPUT_BYTES: u64 = 65_536;
const COPY_CHUNK: usize = 8192;
const CHILDREN: [&str; 3] = ["inputs", "staging", "actions"];
const READ_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::NOFOLLOW)
    .union(OFlags::NONBLOCK)
    .union(OFlags::CLOEXEC);
const CREATE_FLAGS: OFlags = OFlags::RDWR
    .union(OFlags::CREATE)
    .union(OFlags::EXCL)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::NONBLOCK)
    .union(OFlags::CLOEXEC);

/// Exclusive caller-chosen leaf identity, never an incarnation or generation.
#[derive(Clone, Copy)]
pub(super) struct ActionId(pub(super) u128);
#[derive(Clone, Copy)]
pub(super) struct InputId(pub(super) u128);

pub(super) struct EntryName(String);

impl EntryName {
    pub(super) fn new(name: &str) -> Result<Self, Error> {
        if name.is_empty() || matches!(name, "." | "..") || name.contains(['/', '\0']) {
            return Err(failure("invalid_worker_entry_name"));
        }
        Ok(Self(name.to_owned()))
    }
}

struct WorkerDirectories {
    inputs: OwnedFd,
    staging: OwnedFd,
    actions: OwnedFd,
    // Last field: every child descriptor closes before the final role locks.
    _ownership: Arc<Ownership>,
}

pub(super) struct OwnedWorkerFs(Arc<WorkerDirectories>);

impl OwnedWorkerFs {
    /// Cold layout only. Unexpected or partially created state requires review;
    /// this operation does not adopt, remove, sync or reconstruct old state.
    pub(super) fn create(ownership: &Arc<Ownership>) -> Result<Self, Error> {
        let root = &ownership
            .roots
            .iter()
            .find(|root| root.role == Role::Worker)
            .expect("Ownership always retains all four roles")
            .descriptor;
        let mut entries = Dir::read_from(root).map_err(|_| failure("read_worker_root"))?;
        for entry in &mut entries {
            let entry = entry.map_err(|_| failure("read_worker_root"))?;
            if !matches!(entry.file_name().to_bytes(), b"." | b"..")
                && entry.file_name().to_bytes() != OWNERSHIP_RECORD.as_bytes()
            {
                return Err(failure("worker_root_not_empty"));
            }
        }
        let mut opened = Vec::with_capacity(CHILDREN.len());
        for name in CHILDREN {
            mkdirat(root, name, Mode::RWXU).map_err(|_| failure("create_worker_child"))?;
            opened.push(
                openat(root, name, DIRECTORY_FLAGS, Mode::empty())
                    .map_err(|_| failure("open_worker_child"))?,
            );
        }
        let [inputs, staging, actions]: [OwnedFd; 3] = opened
            .try_into()
            .expect("exactly three fixed Worker children were opened");
        Ok(Self(Arc::new(WorkerDirectories {
            inputs,
            staging,
            actions,
            _ownership: Arc::clone(ownership),
        })))
    }

    pub(super) fn create_action(&self, id: ActionId) -> Result<ActionWorkspace, Error> {
        let name = format!("action-{:032x}", id.0);
        mkdirat(&self.0.actions, name.as_str(), Mode::RWXU)
            .map_err(|_| failure("create_worker_action"))?;
        let directory = openat(
            &self.0.actions,
            name.as_str(),
            DIRECTORY_FLAGS,
            Mode::empty(),
        )
        .map_err(|_| failure("open_worker_action"))?;
        Ok(ActionWorkspace {
            directory,
            directories: Arc::clone(&self.0),
        })
    }

    pub(super) fn begin_input(
        &self,
        id: InputId,
        declared_size: u64,
    ) -> Result<PendingWorkerInput, Error> {
        if declared_size > QUALIFICATION_MAX_INPUT_BYTES {
            return Err(failure("worker_input_too_large"));
        }
        let name = format!("pending-{:032x}", id.0);
        let descriptor = openat(
            &self.0.staging,
            name.as_str(),
            CREATE_FLAGS,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|_| failure("create_worker_pending"))?;
        regular(
            &fstat(&descriptor).map_err(|_| failure("stat_worker_pending"))?,
            true,
        )?;
        Ok(PendingWorkerInput {
            file: File::from(descriptor),
            name,
            input_name: format!("input-{:032x}", id.0),
            size: declared_size,
            directories: Arc::clone(&self.0),
        })
    }
}

pub(super) struct ActionWorkspace {
    directory: OwnedFd,
    directories: Arc<WorkerDirectories>,
}

impl ActionWorkspace {
    fn create_file(&self, name: &EntryName) -> Result<File, Error> {
        absent(&self.directory, &name.0, "worker_destination_exists")?;
        let descriptor = openat(
            &self.directory,
            name.0.as_str(),
            CREATE_FLAGS,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|error| {
            failure(if error == Errno::EXIST {
                "worker_destination_exists"
            } else {
                "create_worker_file"
            })
        })?;
        regular(
            &fstat(&descriptor).map_err(|_| failure("stat_worker_file"))?,
            true,
        )?;
        Ok(File::from(descriptor))
    }

    pub(super) fn create_empty(
        &self,
        name: &EntryName,
        metadata: WorkerFileMetadata,
    ) -> Result<(), Error> {
        let file = self.create_file(name)?;
        apply_metadata(&file, metadata)
    }
}

pub(super) struct PendingWorkerInput {
    file: File,
    name: String,
    input_name: String,
    size: u64,
    directories: Arc<WorkerDirectories>,
}

impl PendingWorkerInput {
    pub(super) fn copy_from(self, source: &mut impl Read) -> Result<WorkerInput, Error> {
        self.copy_inner(
            source,
            #[cfg(test)]
            None,
        )
    }

    fn check_named(&self) -> Result<Stat, Error> {
        named_file(
            &self.directories.staging,
            &self.name,
            &self.file,
            true,
            "worker_pending_replaced",
        )
    }

    fn copy_inner(
        mut self,
        source: &mut impl Read,
        #[cfg(test)] fault: Option<(TestIoFault, &AtomicBool)>,
    ) -> Result<WorkerInput, Error> {
        self.check_named()?;
        #[cfg(test)]
        let mut wrote_chunk = false;
        copy_exact(source, self.size, |bytes| {
            #[cfg(test)]
            if wrote_chunk
                && let Some((TestIoFault::ReadOnlyWriterAfterFirstChunk, reached)) = fault
            {
                let descriptor = open_same(
                    &self.directories.staging,
                    &self.name,
                    &self.file,
                    READ_FLAGS,
                )?;
                self.file = File::from(descriptor);
                reached.store(true, Ordering::Release);
            }
            self.file
                .write_all(bytes)
                .map_err(|_| failure("write_worker_input"))?;
            #[cfg(test)]
            {
                wrote_chunk = true;
            }
            Ok(())
        })?;
        let copied = self.check_named()?;
        exact_size(&copied, self.size)?;
        self.make_readonly(
            #[cfg(test)]
            fault,
        )?;
        let reader = open_same(
            &self.directories.staging,
            &self.name,
            &self.file,
            READ_FLAGS,
        )?;
        self.check_named()?;
        // The RDWR descriptor must close even though chmod(0444) succeeded:
        // permissions do not revoke access through an already writable FD.
        let Self {
            file,
            name,
            input_name,
            size,
            directories,
        } = self;
        drop(file);
        // Keep the readonly descriptor and role custody in one local value so
        // failure paths also close the descriptor before releasing ownership.
        // This value cannot escape until the exclusive rename succeeds.
        let input = WorkerInput {
            file: File::from(reader),
            name: input_name,
            size,
            directories,
        };
        absent(
            &input.directories.inputs,
            &input.name,
            "worker_input_exists",
        )?;
        renameat_with(
            &input.directories.staging,
            name.as_str(),
            &input.directories.inputs,
            input.name.as_str(),
            RenameFlags::NOREPLACE,
        )
        .map_err(|error| {
            failure(if error == Errno::EXIST {
                "worker_input_exists"
            } else {
                "publish_worker_input"
            })
        })?;
        Ok(input)
    }

    fn make_readonly(
        &self,
        #[cfg(test)] fault: Option<(TestIoFault, &AtomicBool)>,
    ) -> Result<(), Error> {
        #[cfg(test)]
        if let Some((TestIoFault::PathOnlyModeDescriptor, reached)) = fault {
            let descriptor = open_same(
                &self.directories.staging,
                &self.name,
                &self.file,
                OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            )?;
            reached.store(true, Ordering::Release);
            return fchmod(&descriptor, Mode::RUSR | Mode::RGRP | Mode::ROTH)
                .map_err(|_| failure("mode_worker_input"));
        }
        fchmod(&self.file, Mode::RUSR | Mode::RGRP | Mode::ROTH)
            .map_err(|_| failure("mode_worker_input"))
    }

    #[cfg(test)]
    pub(super) fn copy_from_with_fault(
        self,
        source: &mut impl Read,
        fault: TestIoFault,
        reached: &AtomicBool,
    ) -> Result<WorkerInput, Error> {
        self.copy_inner(source, Some((fault, reached)))
    }
}

/// Completed local filesystem staging, not a verified digest or retention grant.
/// No File, raw descriptor, path, writable or clone interface escapes this type.
pub(super) struct WorkerInput {
    file: File,
    name: String,
    size: u64,
    directories: Arc<WorkerDirectories>,
}

impl WorkerInput {
    fn check_for(&self, action: &ActionWorkspace) -> Result<(), Error> {
        if !Arc::ptr_eq(&self.directories, &action.directories) {
            return Err(failure("different_worker_capability"));
        }
        let stat = named_file(
            &self.directories.inputs,
            &self.name,
            &self.file,
            false,
            "worker_input_replaced",
        )?;
        exact_size(&stat, self.size)
    }

    pub(super) fn link_into(
        &self,
        action: &ActionWorkspace,
        name: &EntryName,
    ) -> Result<(), Error> {
        self.check_for(action)?;
        absent(&action.directory, &name.0, "worker_destination_exists")?;
        // Both borrows retain the input inode and all directory/role custody
        // until the actual syscall returns, including in a detached closure.
        linkat(
            &self.directories.inputs,
            self.name.as_str(),
            &action.directory,
            name.0.as_str(),
            AtFlags::empty(),
        )
        .map_err(|error| {
            failure(if error == Errno::EXIST {
                "worker_destination_exists"
            } else {
                "link_worker_input"
            })
        })
    }

    pub(super) fn copy_into(
        &self,
        action: &ActionWorkspace,
        name: &EntryName,
        metadata: WorkerFileMetadata,
    ) -> Result<(), Error> {
        self.check_for(action)?;
        let mut file = action.create_file(name)?;
        let mut source = PositionedReader {
            file: &self.file,
            offset: 0,
        };
        copy_exact(&mut source, self.size, |bytes| {
            file.write_all(bytes)
                .map_err(|_| failure("write_worker_file"))
        })?;
        exact_size(
            &fstat(&file).map_err(|_| failure("stat_worker_file"))?,
            self.size,
        )?;
        apply_metadata(&file, metadata)
    }

    #[cfg(test)]
    pub(super) fn test_write_attempt(&mut self) -> io::Result<usize> {
        self.file.write(b"!")
    }
}

#[derive(Clone, Copy, Default)]
pub(super) struct WorkerFileMetadata {
    pub(super) executable: bool,
    pub(super) unix_mode: Option<u32>,
    pub(super) mtime: Option<Timespec>,
}

fn apply_metadata(file: &File, metadata: WorkerFileMetadata) -> Result<(), Error> {
    let mode = match (metadata.unix_mode, metadata.executable) {
        (Some(mode), true) => Some(mode | 0o111),
        (Some(mode), false) => Some(mode),
        (None, true) => Some(0o555),
        (None, false) => None,
    };
    if let Some(mode) = mode {
        fchmod(file, Mode::from_raw_mode(mode)).map_err(|_| failure("mode_worker_file"))?;
    }
    if let Some(mtime) = metadata.mtime {
        futimens(
            file,
            &Timestamps {
                last_access: Timespec {
                    tv_sec: 0,
                    tv_nsec: UTIME_OMIT,
                },
                last_modification: mtime,
            },
        )
        .map_err(|_| failure("mtime_worker_file"))?;
    }
    Ok(())
}

/// Worker inputs allow managed local hardlinks; canonical CAS `regular()` still
/// requires exactly one. Pending and action-private files require one here too.
fn regular(stat: &Stat, single_link: bool) -> Result<(), Error> {
    if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile
        || stat.st_nlink == 0
        || (single_link && stat.st_nlink != 1)
    {
        return Err(failure("unsafe_worker_file_type_or_links"));
    }
    Ok(())
}

fn entry(directory: &OwnedFd, name: &str) -> Result<Option<Stat>, Error> {
    match statat(directory, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => Ok(Some(stat)),
        Err(Errno::NOENT) => Ok(None),
        Err(_) => Err(failure("stat_worker_entry")),
    }
}

fn absent(directory: &OwnedFd, name: &str, code: &'static str) -> Result<(), Error> {
    if entry(directory, name)?.is_some() {
        return Err(failure(code));
    }
    Ok(())
}

fn named_file(
    directory: &OwnedFd,
    name: &str,
    file: &File,
    single_link: bool,
    replaced: &'static str,
) -> Result<Stat, Error> {
    let opened = fstat(file).map_err(|_| failure("stat_worker_file"))?;
    let named = entry(directory, name)?.ok_or_else(|| failure(replaced))?;
    regular(&opened, single_link)?;
    regular(&named, single_link)?;
    if (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino) {
        return Err(failure(replaced));
    }
    Ok(opened)
}

fn exact_size(stat: &Stat, size: u64) -> Result<(), Error> {
    if u64::try_from(stat.st_size).ok() != Some(size) {
        return Err(failure("worker_input_size_mismatch"));
    }
    Ok(())
}

fn open_same(
    directory: &OwnedFd,
    name: &str,
    file: &File,
    flags: OFlags,
) -> Result<OwnedFd, Error> {
    let descriptor =
        openat(directory, name, flags, Mode::empty()).map_err(|_| failure("open_worker_input"))?;
    let opened = fstat(&descriptor).map_err(|_| failure("stat_worker_file"))?;
    let original = fstat(file).map_err(|_| failure("stat_worker_file"))?;
    regular(&opened, true)?;
    regular(&original, true)?;
    if (opened.st_dev, opened.st_ino) != (original.st_dev, original.st_ino) {
        return Err(failure("worker_pending_replaced"));
    }
    Ok(descriptor)
}

fn read_retry(source: &mut impl Read, bytes: &mut [u8]) -> io::Result<usize> {
    loop {
        match source.read(bytes) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            result => return result,
        }
    }
}

fn copy_exact(
    source: &mut impl Read,
    size: u64,
    mut write: impl FnMut(&[u8]) -> Result<(), Error>,
) -> Result<(), Error> {
    let mut remaining = size;
    let mut buffer = [0; COPY_CHUNK];
    while remaining != 0 {
        let wanted = usize::try_from(remaining.min(COPY_CHUNK as u64))
            .expect("copy size is bounded by the fixed buffer");
        let count =
            read_retry(source, &mut buffer[..wanted]).map_err(|_| failure("read_worker_input"))?;
        if count == 0 || count > wanted {
            return Err(failure("worker_input_size_mismatch"));
        }
        write(&buffer[..count])?;
        remaining -= u64::try_from(count).expect("read count fits the fixed buffer");
    }
    if read_retry(source, &mut buffer[..1]).map_err(|_| failure("read_worker_input"))? != 0 {
        return Err(failure("worker_input_size_mismatch"));
    }
    Ok(())
}

struct PositionedReader<'a> {
    file: &'a File,
    offset: u64,
}

impl Read for PositionedReader<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let count = self.file.read_at(bytes, self.offset)?;
        self.offset = self
            .offset
            .checked_add(u64::try_from(count).expect("file read fits the supplied buffer"))
            .ok_or_else(|| io::Error::other("Worker input read offset overflow"))?;
        Ok(count)
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(super) enum TestIoFault {
    ReadOnlyWriterAfterFirstChunk,
    PathOnlyModeDescriptor,
}
