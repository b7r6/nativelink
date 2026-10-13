// Copyright 2024 The NativeLink Authors. All rights reserved.
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

//! The on-disk write-ahead log behind the raft log store.
//!
//! One append-only file of framed records:
//!
//! ```text
//! [len: u32 LE][crc32 of payload: u32 LE][payload: len bytes of JSON]
//! ```
//!
//! Records are written in order and `fdatasync`ed before the caller is told
//! they are durable, so only the tail can be torn by a crash. On open, the
//! first record whose frame does not fit or whose checksum does not match ends
//! the log; the file is truncated there and everything after it is dropped.
//! A record that was never acknowledged as durable may be lost; one that was
//! acknowledged never is.
//!
//! The log is compacted by rewriting the live records to a temporary file,
//! syncing it, renaming it over the log and syncing the directory, so a crash
//! leaves either the old file or the new one, never a mix.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use openraft::{LogId, Vote};
use serde::{Deserialize, Serialize};
use tracing::warn;

use super::types::{Entry, NodeId};

const LOG_FILE: &str = "raft.log";
const FRAME_HEADER_LEN: usize = 8;
/// A single record larger than this is treated as corruption, not allocated.
const MAX_RECORD_LEN: u32 = 256 * 1024 * 1024;

/// One durable fact about the raft log. Replaying the records in order
/// reconstructs the log, the vote and the committed id.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum WalRecord {
    Entry(Box<Entry>),
    Vote(Vote<NodeId>),
    /// Delete every entry at or after this index.
    Truncate(u64),
    /// Delete every entry at or before this log id.
    Purge(LogId<NodeId>),
    Committed(Option<LogId<NodeId>>),
}

#[derive(Debug)]
pub(crate) struct Wal {
    dir: PathBuf,
    file: File,
}

fn encode(records: &[WalRecord]) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    for record in records {
        let payload = serde_json::to_vec(record).map_err(io::Error::other)?;
        let len = u32::try_from(payload.len())
            .ok()
            .filter(|len| *len <= MAX_RECORD_LEN)
            .ok_or_else(|| io::Error::other("raft log record too large"))?;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
        out.extend_from_slice(&payload);
    }
    Ok(out)
}

/// Decode records from the front of `bytes`. Returns the records and the
/// length of the valid prefix; anything after that prefix is a torn tail.
fn decode(bytes: &[u8]) -> (Vec<WalRecord>, usize) {
    let mut records = Vec::new();
    let mut pos = 0;
    while let Some(header) = bytes.get(pos..pos + FRAME_HEADER_LEN) {
        let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        if len > MAX_RECORD_LEN {
            break;
        }
        let start = pos + FRAME_HEADER_LEN;
        let Some(payload) = bytes.get(start..start + len as usize) else {
            break;
        };
        if crc32fast::hash(payload) != crc {
            break;
        }
        let Ok(record) = serde_json::from_slice::<WalRecord>(payload) else {
            break;
        };
        records.push(record);
        pos = start + len as usize;
    }
    (records, pos)
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Write `bytes` to `dir/name` so that after a crash the file holds either
/// its previous contents or exactly `bytes`.
pub(crate) fn write_atomic(dir: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    {
        let mut file = File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, dir.join(name))?;
    sync_dir(dir)
}

/// Read `dir/name`, or `None` if it does not exist.
pub(crate) fn read_optional(dir: &Path, name: &str) -> io::Result<Option<Vec<u8>>> {
    match fs::read(dir.join(name)) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

impl Wal {
    /// Open (creating if needed) the log in `dir` and return every durable
    /// record in it. A torn tail is truncated away before returning.
    pub(crate) fn open(dir: &Path) -> io::Result<(Self, Vec<WalRecord>)> {
        fs::create_dir_all(dir)?;
        let path = dir.join(LOG_FILE);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let (records, valid_len) = decode(&bytes);
        if valid_len < bytes.len() {
            warn!(
                path = %path.display(),
                valid_len,
                file_len = bytes.len(),
                "raft log has a torn or corrupt tail; truncating it",
            );
            file.set_len(valid_len as u64)?;
            file.sync_all()?;
        }
        file.seek(SeekFrom::End(0))?;
        Ok((
            Self {
                dir: dir.to_path_buf(),
                file,
            },
            records,
        ))
    }

    /// Append `records` and return once they are on disk.
    pub(crate) fn append(&mut self, records: &[WalRecord]) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let bytes = encode(records)?;
        self.file.write_all(&bytes)?;
        self.file.sync_data()
    }

    /// Replace the whole log with `records`, atomically.
    pub(crate) fn rewrite(&mut self, records: &[WalRecord]) -> io::Result<()> {
        let bytes = encode(records)?;
        write_atomic(&self.dir, LOG_FILE, &bytes)?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.dir.join(LOG_FILE))?;
        file.seek(SeekFrom::End(0))?;
        self.file = file;
        Ok(())
    }

    pub(crate) fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }
}

#[cfg(test)]
mod tests {
    use openraft::{CommittedLeaderId, EntryPayload};

    use super::*;

    fn entry(index: u64) -> Entry {
        Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Blank,
        }
    }

    #[test]
    fn torn_tail_is_truncated_and_prefix_survives() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        {
            let (mut wal, records) = Wal::open(dir.path())?;
            assert!(records.is_empty());
            wal.append(&[
                WalRecord::Entry(Box::new(entry(1))),
                WalRecord::Entry(Box::new(entry(2))),
            ])?;
        }
        // Simulate a crash mid-write: half a frame at the end of the file.
        let path = dir.path().join(LOG_FILE);
        let mut bytes = fs::read(&path)?;
        let full_len = bytes.len();
        bytes.extend_from_slice(&encode(&[WalRecord::Entry(Box::new(entry(3)))])?[..10]);
        fs::write(&path, &bytes)?;

        let (mut wal, records) = Wal::open(dir.path())?;
        assert_eq!(records.len(), 2);
        assert_eq!(fs::metadata(&path)?.len(), full_len as u64);
        // The log keeps working after the truncation.
        wal.append(&[WalRecord::Entry(Box::new(entry(3)))])?;
        drop(wal);
        let (_wal, records) = Wal::open(dir.path())?;
        assert_eq!(records.len(), 3);
        Ok(())
    }

    #[test]
    fn corrupt_byte_ends_the_log_at_that_record() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        {
            let (mut wal, _) = Wal::open(dir.path())?;
            wal.append(&[WalRecord::Entry(Box::new(entry(1)))])?;
            wal.append(&[WalRecord::Entry(Box::new(entry(2)))])?;
        }
        let path = dir.path().join(LOG_FILE);
        let mut bytes = fs::read(&path)?;
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;
        fs::write(&path, &bytes)?;
        let (_wal, records) = Wal::open(dir.path())?;
        assert_eq!(
            records.len(),
            1,
            "the record with the flipped byte is dropped"
        );
        Ok(())
    }

    #[test]
    fn rewrite_replaces_contents() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let (mut wal, _) = Wal::open(dir.path())?;
        wal.append(&[
            WalRecord::Entry(Box::new(entry(1))),
            WalRecord::Entry(Box::new(entry(2))),
        ])?;
        wal.rewrite(&[WalRecord::Entry(Box::new(entry(2)))])?;
        wal.append(&[WalRecord::Entry(Box::new(entry(3)))])?;
        drop(wal);
        let (_wal, records) = Wal::open(dir.path())?;
        let indexes: Vec<u64> = records
            .iter()
            .filter_map(|r| match r {
                WalRecord::Entry(e) => Some(e.log_id.index),
                _ => None,
            })
            .collect();
        assert_eq!(indexes, vec![2, 3]);
        Ok(())
    }
}
