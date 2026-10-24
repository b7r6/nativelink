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

//! Real filesystem effects driven by the existing finite retention candidate
//! and independent oracle. The bounded checkpoint driver is test-only; this is
//! neither a production authority catalogue nor a Raft/provider qualification.

#[path = "retention/coarse.rs"]
mod coarse;
#[path = "retention/protocol.rs"]
mod protocol;
#[path = "retention/reference.rs"]
mod reference;

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use coarse::Coarse;
use nativelink_store::physical_filesystem::{
    Algorithm, DeleteReceipt, FailureKind, Incarnation, Limits as PhysicalLimits, ObjectId,
    PhysicalFilesystem, PhysicalKey, WriteReceipt,
};
use protocol::*;
use reference::Reference;
use serde::{Deserialize, Serialize};

const PAYLOAD: &[u8] = b"the same logical object across physical incarnations";
const CHECKPOINT_MAX: u64 = 1024 * 1024;

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "nativelink-physical-retention-{}",
            uuid::Uuid::new_v4()
        ));
        fs::create_dir(&path).unwrap();
        fs::create_dir(path.join("authority")).unwrap();
        fs::create_dir(path.join("physical")).unwrap();
        Self(path)
    }
    fn checkpoint(&self) -> PathBuf {
        self.0.join("authority/checkpoint")
    }
    fn physical(&self) -> PathBuf {
        self.0.join("physical")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    format: u32,
    candidate: Vec<u8>,
    oracle: Vec<u8>,
}

struct Driver {
    candidate: Coarse,
    oracle: Reference,
    checkpoint: PathBuf,
}

impl Driver {
    fn new(checkpoint: PathBuf) -> Self {
        let driver = Self {
            candidate: Coarse::new(Limits::default()).unwrap(),
            oracle: Reference::new(Limits::default()).unwrap(),
            checkpoint,
        };
        driver.persist();
        driver
    }

    fn persist(&self) {
        let bytes = serde_json::to_vec(&Checkpoint {
            format: 1,
            candidate: self.candidate.snapshot().unwrap(),
            oracle: self.oracle.snapshot().unwrap(),
        })
        .unwrap();
        assert!(bytes.len() as u64 <= CHECKPOINT_MAX);
        let staging = self.checkpoint.with_extension("next");
        let mut file = File::create(&staging).unwrap();
        file.write_all(&bytes).unwrap();
        file.sync_all().unwrap();
        drop(file);
        fs::rename(staging, &self.checkpoint).unwrap();
        File::open(self.checkpoint.parent().unwrap())
            .unwrap()
            .sync_all()
            .unwrap();
    }

    fn reopen(checkpoint: &Path) -> Self {
        assert!(fs::metadata(checkpoint).unwrap().len() <= CHECKPOINT_MAX);
        let saved: Checkpoint = serde_json::from_slice(&fs::read(checkpoint).unwrap()).unwrap();
        assert_eq!(saved.format, 1);
        let driver = Self {
            candidate: Coarse::restore(&saved.candidate).unwrap(),
            oracle: Reference::restore(&saved.oracle).unwrap(),
            checkpoint: checkpoint.to_path_buf(),
        };
        driver.oracle.check().unwrap();
        assert_eq!(driver.candidate.view(), driver.oracle.view());
        driver
    }

    fn commit(&mut self, event: Event) -> Reply {
        let before = self.candidate.view();
        let actual = self.candidate.apply(event.clone());
        let expected = self.oracle.apply(event);
        assert_eq!(actual, expected);
        self.oracle.check().unwrap();
        assert_eq!(self.candidate.view(), self.oracle.view());
        if actual == Reply::Applied {
            self.persist();
        } else {
            assert_eq!(self.candidate.view(), before);
        }
        actual
    }

    fn applied(&mut self, event: Event) {
        assert_eq!(self.commit(event), Reply::Applied);
    }
    fn rejected(&mut self, event: Event) {
        assert_eq!(self.commit(event), Reply::Rejected);
    }
    fn view(&self) -> View {
        self.candidate.view()
    }

    fn allocate_and_start(&mut self, key: Key, id: u64) {
        self.applied(Event::CreateCohort {
            cohort: CohortId(id),
        });
        self.applied(Event::Allocate {
            cohort: CohortId(id),
            key,
            logical_object: 1,
        });
        self.applied(Event::Acquire {
            root: RootId(id),
            lease: 1,
            kind: RootKind::Staging,
            closure: vec![key],
            until: 10,
            fences: BTreeMap::from([(CohortId(id), 0)]),
        });
        self.applied(Event::StartWrite {
            write: WriteId(id),
            key,
            staging: RootId(id),
            lease: 1,
        });
    }

    fn write_done(&mut self, receipt: WriteReceipt) {
        let write = WriteId(receipt.write);
        assert_eq!(physical(self.view().writes[&write].key), receipt.key);
        assert_eq!(receipt.object, object());
        self.applied(Event::WriteDone {
            write,
            success: true,
        });
    }

    fn delete_effect(&self, store: &PhysicalFilesystem, delete: DeleteId) -> DeleteReceipt {
        let view = self.view();
        let intent = &view.deletes[&delete];
        assert!(view.generations[&intent.key].retired);
        assert!(!view.protected.contains(&intent.key));
        store.delete(physical(intent.key), delete.0).unwrap()
    }

    fn delete_done(&mut self, receipt: DeleteReceipt) -> Reply {
        let delete = DeleteId(receipt.delete);
        assert_eq!(physical(self.view().deletes[&delete].key), receipt.key);
        self.commit(Event::DeleteDone {
            delete,
            success: true,
        })
    }

    fn close(&mut self, cohort: CohortId) -> u64 {
        self.applied(Event::Seal { cohort });
        self.applied(Event::BeginClose { cohort });
        let fence = self.view().cohorts[&cohort].fence;
        self.applied(Event::FinishClose { cohort, fence });
        fence
    }
}

fn inc(value: u64) -> Incarnation {
    let mut bytes = [0; 16];
    bytes[8..].copy_from_slice(&value.to_be_bytes());
    Incarnation::new(bytes).unwrap()
}
fn physical(key: Key) -> PhysicalKey {
    PhysicalKey::new(inc(key.incarnation), key.generation).unwrap()
}
fn object() -> ObjectId {
    ObjectId::for_bytes(Algorithm::Sha256, PAYLOAD)
}

#[test]
fn durable_retirement_checkpoint_reaps_real_late_write_without_touching_recreation() {
    for acknowledge_delete_first in [false, true] {
        let directory = Directory::new();
        let mut driver = Driver::new(directory.checkpoint());
        let old = Key {
            incarnation: 1,
            generation: 1,
        };
        let replacement = Key {
            incarnation: 2,
            generation: 1,
        };
        let old_store =
            PhysicalFilesystem::bootstrap(&directory.physical(), inc(1), PhysicalLimits::default())
                .unwrap();
        driver.allocate_and_start(old, 1);
        let mut late = old_store.begin_write(physical(old), 1, object()).unwrap();
        late.append(PAYLOAD).unwrap();
        driver.applied(Event::Release {
            root: RootId(1),
            lease: 1,
        });
        let fence = driver.close(CohortId(1));
        driver.applied(Event::Retire {
            cohort: CohortId(1),
            fence,
            keys: vec![old],
        });
        let old_delete = driver.delete_effect(&old_store, DeleteId(1));
        assert!(old_delete.already_absent);
        if acknowledge_delete_first {
            assert_eq!(driver.delete_done(old_delete), Reply::Applied);
        }
        drop(driver);
        let mut driver = Driver::reopen(&directory.checkpoint());
        assert!(driver.view().generations[&old].retired);
        assert!(!driver.view().generations[&old].ready);
        assert_eq!(driver.view().writes[&WriteId(1)].outcome, None);

        let replacement_store =
            PhysicalFilesystem::bootstrap(&directory.physical(), inc(2), PhysicalLimits::default())
                .unwrap();
        driver.allocate_and_start(replacement, 2);
        let mut write = replacement_store
            .begin_write(physical(replacement), 2, object())
            .unwrap();
        write.append(PAYLOAD).unwrap();
        driver.write_done(write.finish().unwrap());
        let replacement_before = driver.view().generations[&replacement].clone();
        let lost_write_receipt = late.finish().unwrap();
        drop(old_store);
        drop(driver);
        let old_store = PhysicalFilesystem::open(&directory.physical(), inc(1)).unwrap();
        let mut driver = Driver::reopen(&directory.checkpoint());
        // Physical existence is not authoritative readiness. The lost result
        // remains unresolved across both namespace and authority recovery.
        assert_eq!(
            old_store
                .read(physical(old), object(), 0, None)
                .unwrap()
                .bytes,
            PAYLOAD
        );
        assert!(!driver.view().generations[&old].ready);
        assert_eq!(driver.view().writes[&WriteId(1)].outcome, None);
        driver.write_done(lost_write_receipt);
        assert_eq!(
            driver.view().generations[&old].current_delete,
            Some(DeleteId(2))
        );
        assert_eq!(
            driver.delete_done(old_delete),
            if acknowledge_delete_first {
                Reply::Ignored
            } else {
                Reply::Applied
            }
        );
        assert_eq!(
            driver.view().generations[&old].current_delete,
            Some(DeleteId(2))
        );
        driver.applied(Event::Reopen {
            cohort: CohortId(1),
            fence,
        });
        driver.rejected(Event::Acquire {
            root: RootId(3),
            lease: 1,
            kind: RootKind::Serving,
            closure: vec![old],
            until: 10,
            fences: BTreeMap::from([(CohortId(1), driver.view().cohorts[&CohortId(1)].fence)]),
        });
        let reaped = driver.delete_effect(&old_store, DeleteId(2));
        assert!(!reaped.already_absent);
        assert_eq!(driver.delete_done(reaped), Reply::Applied);
        drop(driver);
        let driver = Driver::reopen(&directory.checkpoint());
        assert_eq!(driver.view().generations[&old].current_delete, None);
        assert_eq!(driver.view().writes[&WriteId(1)].outcome, Some(true));
        assert!(driver.view().generations[&old].retired);
        assert!(!driver.view().generations[&old].ready);
        assert_eq!(
            old_store
                .read(physical(old), object(), 0, None)
                .unwrap_err()
                .kind,
            FailureKind::Missing
        );
        assert_eq!(driver.view().generations[&replacement], replacement_before);
        assert!(driver.view().protected.contains(&replacement));
        assert_eq!(
            replacement_store
                .read(physical(replacement), object(), 0, None)
                .unwrap()
                .bytes,
            PAYLOAD
        );
    }
}

#[test]
fn committed_protection_prevents_a_physical_delete_intent() {
    let directory = Directory::new();
    let mut driver = Driver::new(directory.checkpoint());
    let key = Key {
        incarnation: 1,
        generation: 1,
    };
    let store =
        PhysicalFilesystem::bootstrap(&directory.physical(), inc(1), PhysicalLimits::default())
            .unwrap();
    driver.allocate_and_start(key, 1);
    let mut write = store.begin_write(physical(key), 1, object()).unwrap();
    write.append(PAYLOAD).unwrap();
    driver.write_done(write.finish().unwrap());
    driver.applied(Event::Acquire {
        root: RootId(2),
        lease: 1,
        kind: RootKind::Serving,
        closure: vec![key],
        until: 10,
        fences: BTreeMap::from([(CohortId(1), 0)]),
    });
    driver.applied(Event::Release {
        root: RootId(1),
        lease: 1,
    });
    let fence = driver.close(CohortId(1));
    let captured = driver.view().cohorts[&CohortId(1)].captured.clone();
    driver.applied(Event::Mark {
        cohort: CohortId(1),
        fence,
        at: 0,
        roots: captured,
    });
    driver.rejected(Event::Retire {
        cohort: CohortId(1),
        fence,
        keys: vec![key],
    });
    assert!(driver.view().deletes.is_empty());
    drop(driver);
    let driver = Driver::reopen(&directory.checkpoint());
    assert!(driver.view().protected.contains(&key));
    assert_eq!(
        store.read(physical(key), object(), 0, None).unwrap().bytes,
        PAYLOAD
    );
}
