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

//! Bounds snapshot accumulation before allocation, including sparse seeks.

use std::io::{self, Cursor, Seek, SeekFrom, Write};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};

/// Experimental transport/metadata ceiling, not a calibrated production limit.
/// Admission must separately reserve serialized completion/snapshot headroom.
pub const MAX_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;

/// A failed write/seek poisons the buffer so ignoring a streaming error cannot
/// install a valid-looking earlier prefix. Only a new receive attempt clears it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotBuffer {
    cursor: Cursor<Vec<u8>>,
    limit: usize,
    failed: bool,
}

impl Default for SnapshotBuffer {
    fn default() -> Self {
        Self {
            cursor: Cursor::new(Vec::new()),
            limit: MAX_SNAPSHOT_BYTES,
            failed: false,
        }
    }
}

impl SnapshotBuffer {
    pub fn from_bytes(bytes: Vec<u8>) -> io::Result<Self> {
        if bytes.len() > MAX_SNAPSHOT_BYTES {
            return Err(io::Error::other("snapshot exceeds byte bound"));
        }
        Ok(Self {
            cursor: Cursor::new(bytes),
            ..Self::default()
        })
    }

    pub fn get_ref(&self) -> &[u8] {
        self.cursor.get_ref()
    }

    pub fn into_inner(self) -> io::Result<Vec<u8>> {
        self.valid()?;
        Ok(self.cursor.into_inner())
    }

    fn valid(&self) -> io::Result<()> {
        if self.failed {
            Err(io::Error::other("snapshot accumulation previously failed"))
        } else {
            Ok(())
        }
    }

    fn fail(&mut self) -> io::Error {
        self.failed = true;
        io::Error::other("snapshot write/seek exceeds byte bound")
    }
}

impl Write for SnapshotBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.valid()?;
        if bytes.is_empty() {
            return Ok(0);
        }
        let end = usize::try_from(self.cursor.position())
            .ok()
            .and_then(|p| p.checked_add(bytes.len()));
        let Some(end) = end.filter(|end| *end <= self.limit) else {
            return Err(self.fail());
        };
        // Avoid Vec's geometric growth crossing the declared capacity ceiling.
        let buffer = self.cursor.get_mut();
        if end > buffer.capacity() {
            let capacity = end
                .max(buffer.capacity().saturating_mul(2))
                .max(4096)
                .min(self.limit);
            if buffer.try_reserve_exact(capacity - buffer.len()).is_err() {
                return Err(self.fail());
            }
        }
        self.cursor.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.valid()
    }
}

impl Seek for SnapshotBuffer {
    fn seek(&mut self, offset: SeekFrom) -> io::Result<u64> {
        self.valid()?;
        let position = match offset {
            SeekFrom::Start(value) => i128::from(value),
            SeekFrom::Current(value) => i128::from(self.cursor.position()) + i128::from(value),
            SeekFrom::End(value) => self.cursor.get_ref().len() as i128 + i128::from(value),
        };
        let Some(position) = u64::try_from(position)
            .ok()
            .filter(|p| u128::from(*p) <= self.limit as u128)
        else {
            return Err(self.fail());
        };
        self.cursor.set_position(position);
        Ok(position)
    }
}

impl AsyncRead for SnapshotBuffer {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Err(error) = self.valid() {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.cursor).poll_read(cx, buffer)
    }
}

impl AsyncWrite for SnapshotBuffer {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(self.write(bytes))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.valid())
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.valid())
    }
}

impl AsyncSeek for SnapshotBuffer {
    fn start_seek(mut self: Pin<&mut Self>, offset: SeekFrom) -> io::Result<()> {
        self.seek(offset).map(|_| ())
    }
    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(self.valid().map(|()| self.cursor.position()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_bound_sparse_seek_and_failed_prefix_are_checked_before_growth() {
        let mut buffer = SnapshotBuffer {
            limit: 8,
            ..SnapshotBuffer::default()
        };
        buffer.write_all(b"abcd").unwrap();
        buffer.seek(SeekFrom::Start(7)).unwrap();
        buffer.write_all(b"z").unwrap();
        assert_eq!(buffer.get_ref(), b"abcd\0\0\0z");
        assert_eq!(buffer.cursor.get_ref().capacity(), 8);
        assert!(buffer.write_all(b"!").is_err());
        assert_eq!(buffer.get_ref(), b"abcd\0\0\0z");
        assert!(buffer.seek(SeekFrom::Start(0)).is_err());
        assert!(buffer.into_inner().is_err());

        for seek in [
            SeekFrom::Start(u64::MAX),
            SeekFrom::End(-1),
            SeekFrom::Current(i64::MAX),
        ] {
            let mut buffer = SnapshotBuffer::default();
            assert!(buffer.seek(seek).is_err());
            assert_eq!(buffer.get_ref(), b"");
            assert_eq!(buffer.cursor.get_ref().capacity(), 0);
            assert!(buffer.into_inner().is_err());
        }
        let mut buffer = SnapshotBuffer::default();
        buffer
            .seek(SeekFrom::Start(MAX_SNAPSHOT_BYTES as u64))
            .unwrap();
        assert_eq!(buffer.write(&[]).unwrap(), 0);
        assert_eq!(buffer.cursor.get_ref().capacity(), 0);
    }
}
