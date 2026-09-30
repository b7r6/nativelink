// Copyright 2026 The NativeLink Authors. All rights reserved.
//! Embedded ClickHouse Keeper, thin and safe-ish. The point of this crate
//! is one property: SESSIONS AS LIVENESS — an ephemeral node vanishes when
//! its owning session dies, enforced by the coordination kernel rather
//! than by bookkeeping code that can race.

#![cfg(feature = "embedded")]
#[allow(non_camel_case_types, non_upper_case_globals, dead_code)]
mod ffi {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

use core::time::Duration;
use std::ffi::{CStr, CString};
use std::path::Path;

pub struct Server(*mut ffi::nlk_server);
unsafe impl Send for Server {}
unsafe impl Sync for Server {}

pub struct Session<'srv> {
    raw: *mut ffi::nlk_session,
    _srv: core::marker::PhantomData<&'srv Server>,
}
unsafe impl Send for Session<'_> {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchEvent {
    Created,
    Deleted,
    Changed,
    SessionLost,
}

#[derive(Debug, PartialEq, Eq)]
pub enum KeeperError {
    NoNode,
    NodeExists,
    BadVersion,
    SessionExpired,
    Timeout,
    Other(String),
}

fn rc(code: ffi::nlk_rc) -> Result<(), KeeperError> {
    use ffi::nlk_rc as R;
    match code {
        R::NLK_OK => Ok(()),
        R::NLK_NO_NODE => Err(KeeperError::NoNode),
        R::NLK_NODE_EXISTS => Err(KeeperError::NodeExists),
        R::NLK_BAD_VERSION => Err(KeeperError::BadVersion),
        R::NLK_SESSION_EXPIRED => Err(KeeperError::SessionExpired),
        R::NLK_TIMEOUT => Err(KeeperError::Timeout),
        _ => Err(KeeperError::Other(last_error())),
    }
}

fn last_error() -> String {
    unsafe { CStr::from_ptr(ffi::nlk_last_error()) }
        .to_string_lossy()
        .into_owned()
}

impl Server {
    pub fn start(dir: &Path, tick: Duration) -> Result<Self, KeeperError> {
        let c = CString::new(dir.to_str().expect("utf8 path")).unwrap();
        let p = unsafe { ffi::nlk_server_start(c.as_ptr(), tick.as_millis() as u32) };
        if p.is_null() {
            return Err(KeeperError::Other(last_error()));
        }
        Ok(Self(p))
    }

    /// Multi-node raft. `ensemble` lists every member as `(id, "host:port")`,
    /// including this node (`my_id`), whose entry determines the local bind.
    pub fn start_ensemble(
        dir: &Path,
        tick: Duration,
        my_id: u32,
        ensemble: &[(u32, &str)],
    ) -> Result<Self, KeeperError> {
        let c = CString::new(dir.to_str().expect("utf8 path")).unwrap();
        let spec = ensemble
            .iter()
            .map(|(id, hp)| format!("{id}={hp}"))
            .collect::<Vec<_>>()
            .join(",");
        let spec = CString::new(spec).unwrap();
        let p = unsafe {
            ffi::nlk_server_start_ensemble(c.as_ptr(), tick.as_millis() as u32, my_id, spec.as_ptr())
        };
        if p.is_null() {
            return Err(KeeperError::Other(last_error()));
        }
        Ok(Self(p))
    }

    pub fn session(&self, timeout: Duration) -> Result<Session<'_>, KeeperError> {
        let p = unsafe { ffi::nlk_session_create(self.0, timeout.as_millis() as u32) };
        if p.is_null() {
            return Err(KeeperError::Other(last_error()));
        }
        Ok(Session { raw: p, _srv: core::marker::PhantomData })
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        unsafe { ffi::nlk_server_shutdown(self.0) }
    }
}

impl Session<'_> {
    pub fn id(&self) -> i64 {
        unsafe { ffi::nlk_session_id(self.raw) }
    }

    pub fn create(&self, path: &str, data: &[u8], ephemeral: bool) -> Result<(), KeeperError> {
        let c = CString::new(path).unwrap();
        rc(unsafe { ffi::nlk_create(self.raw, c.as_ptr(), data.as_ptr(), data.len(), ephemeral) })
    }

    pub fn exists(&self, path: &str) -> Result<Option<i32>, KeeperError> {
        let c = CString::new(path).unwrap();
        let mut v: i32 = -1;
        match rc(unsafe { ffi::nlk_exists(self.raw, c.as_ptr(), &raw mut v) }) {
            Ok(()) => Ok(Some(v)),
            Err(KeeperError::NoNode) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn set_cas(
        &self,
        path: &str,
        data: &[u8],
        expected_version: i32,
    ) -> Result<i32, KeeperError> {
        let c = CString::new(path).unwrap();
        let mut nv: i32 = -1;
        rc(unsafe {
            ffi::nlk_set(self.raw, c.as_ptr(), data.as_ptr(), data.len(), expected_version, &raw mut nv)
        })?;
        Ok(nv)
    }

    pub fn get(&self, path: &str) -> Result<(Vec<u8>, i32), KeeperError> {
        let c = CString::new(path).unwrap();
        let mut data: *mut u8 = core::ptr::null_mut();
        let mut len: usize = 0;
        let mut ver: i32 = -1;
        rc(unsafe { ffi::nlk_get(self.raw, c.as_ptr(), &raw mut data, &raw mut len, &raw mut ver) })?;
        let out = unsafe { core::slice::from_raw_parts(data, len) }.to_vec();
        unsafe { ffi::nlk_free(data.cast()) };
        Ok((out, ver))
    }

    /// One-shot watch on `path`. The callback fires at most once (re-subscribe
    /// from within it to re-arm). The callback allocation is intentionally
    /// leaked; watches are expected to live for the process lifetime.
    pub fn watch<F>(&self, path: &str, f: F) -> Result<(), KeeperError>
    where
        F: Fn(WatchEvent, &str) + Send + 'static,
    {
        unsafe extern "C" fn trampoline<F: Fn(WatchEvent, &str)>(
            ctx: *mut core::ffi::c_void,
            ev: ffi::nlk_event,
            path: *const core::ffi::c_char,
        ) {
            use ffi::nlk_event as E;
            let ev = match ev {
                E::NLK_EV_CREATED => WatchEvent::Created,
                E::NLK_EV_DELETED => WatchEvent::Deleted,
                E::NLK_EV_CHANGED => WatchEvent::Changed,
                _ => WatchEvent::SessionLost,
            };
            let path = unsafe { CStr::from_ptr(path) }.to_string_lossy();
            unsafe { (*(ctx as *const F))(ev, &path) };
        }
        let c = CString::new(path).unwrap();
        let ctx = Box::into_raw(Box::new(f)) as *mut core::ffi::c_void;
        let w = unsafe { ffi::nlk_watch_subscribe(self.raw, c.as_ptr(), Some(trampoline::<F>), ctx) };
        if w.is_null() {
            return Err(KeeperError::Other(last_error()));
        }
        Ok(())
    }

    /// Simulate ungraceful client death; ephemerals must be reaped by
    /// session expiry, not by any goodbye message.
    pub fn abandon(self) {
        unsafe { ffi::nlk_session_abandon(self.raw) };
        core::mem::forget(self);
    }
}

impl Drop for Session<'_> {
    fn drop(&mut self) {
        unsafe { ffi::nlk_session_close(self.raw) }
    }
}
