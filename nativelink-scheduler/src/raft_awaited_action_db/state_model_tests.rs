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

//! Randomized model test of the replicated state machine.
//!
//! Seeded command sequences, built to hit the edges (stale revisions,
//! timestamps from a leader whose clock is behind, dedup on shared keys,
//! sessions superseded under running work, clients dropped mid-flight), are
//! applied to [`AppliedState`]. After every command the invariants below are
//! checked; at random points the state is round-tripped through the
//! snapshot encoding; and a second replica applying the same log must end
//! byte-for-byte identical.

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use nativelink_util::action_messages::{
    ActionInfo, ActionResult, ActionStage, ActionUniqueKey, ActionUniqueQualifier, OperationId,
    WorkerId,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;

use super::state::AppliedState;
use super::types::Command;
use crate::awaited_action_db::{SortedAwaitedAction, SortedAwaitedActionState};

/// `SplitMix64`: small, seedable, and the same everywhere.
struct Rng(u64);

impl Rng {
    const fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % (n as u64)).unwrap_or(0)
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> Option<&'a T> {
        if items.is_empty() {
            None
        } else {
            items.get(self.below(items.len()))
        }
    }
}

fn action_info(rng: &mut Rng) -> Arc<ActionInfo> {
    // Few digests, so cacheable actions collide and exercise dedup.
    let byte = u8::try_from(rng.below(4)).unwrap_or(0);
    let key = ActionUniqueKey {
        execution_scope: None,
        instance_name: "model".to_string(),
        digest_function: DigestHasherFunc::Sha256,
        digest: DigestInfo::new([byte; 32], u64::from(byte)),
    };
    Arc::new(ActionInfo {
        command_digest: DigestInfo::new([0u8; 32], 0),
        input_root_digest: DigestInfo::new([0u8; 32], 0),
        timeout: Duration::MAX,
        platform_properties: HashMap::new(),
        priority: i32::try_from(rng.below(3)).unwrap_or(0),
        load_timestamp: UNIX_EPOCH,
        insert_timestamp: UNIX_EPOCH,
        unique_qualifier: if rng.chance(70) {
            ActionUniqueQualifier::Cacheable(key)
        } else {
            ActionUniqueQualifier::Uncacheable(key)
        },
    })
}

/// Pools of identities the generator reuses, so commands collide.
struct World {
    clients: Vec<OperationId>,
    workers: Vec<WorkerId>,
    now: u128,
}

fn next_command(rng: &mut Rng, world: &mut World, state: &AppliedState) -> Command {
    // Time mostly moves forward; sometimes a new leader is behind.
    if rng.chance(10) {
        world.now = world
            .now
            .saturating_sub(u128::from(rng.next() % 5_000_000_000));
    } else {
        world.now += u128::from(rng.next() % 2_000_000_000);
    }
    let now_unix_nanos = world.now;
    let ops: Vec<OperationId> = state.operations.keys().cloned().collect();
    let worker = rng
        .pick(&world.workers)
        .cloned()
        .unwrap_or_else(|| WorkerId("w0".into()));
    match rng.below(10) {
        0..=2 => {
            let client_operation_id = if rng.chance(20) {
                // A client id already used: the newest mapping wins.
                rng.pick(&world.clients).cloned().unwrap_or_default()
            } else {
                OperationId::default()
            };
            world.clients.push(client_operation_id.clone());
            Command::AddAction {
                client_operation_id,
                operation_id: OperationId::default(),
                action_info: action_info(rng),
                now_unix_nanos,
                origin_metadata: None,
            }
        }
        3 | 4 => {
            let Some(op) = rng.pick(&ops).cloned() else {
                return Command::Expire {
                    now_unix_nanos,
                    lease_timeout_nanos: 1,
                };
            };
            let current = state.operations[&op].clone();
            let mut expected_revision = current.version_public();
            if rng.chance(15) {
                expected_revision -= 1; // a writer that read an older record
            }
            let mut record = current;
            let mut stage_state = record.state().as_ref().clone();
            stage_state.stage = match rng.below(4) {
                0 => ActionStage::Queued,
                1 => ActionStage::Executing,
                2 => ActionStage::Completed(ActionResult::default()),
                _ => ActionStage::Completed(ActionResult {
                    exit_code: 1,
                    ..ActionResult::default()
                }),
            };
            record.worker_set_state(Arc::new(stage_state), UNIX_EPOCH);
            record.attempts = rng.below(8);
            if rng.chance(20) {
                // An escalation rewrites the action, and its sort key with it.
                let mut info = record.action_info().as_ref().clone();
                info.priority += 1;
                record.set_action_info(Arc::new(info));
            }
            Command::UpdateAwaitedAction {
                operation_id: op,
                expected_revision,
                awaited_action: Box::new(record),
                now_unix_nanos,
            }
        }
        5 => {
            let Some(op) = rng.pick(&ops).cloned() else {
                return Command::Expire {
                    now_unix_nanos,
                    lease_timeout_nanos: 1,
                };
            };
            let expected_revision = state.operation_revision.get(&op).copied().unwrap_or(0);
            let session_epoch = state.sessions.get(&worker).copied().unwrap_or(1);
            Command::AssignToWorker {
                operation_id: op,
                expected_revision,
                worker_id: worker,
                session_epoch,
                now_unix_nanos,
            }
        }
        6 => {
            let epoch = state.sessions.get(&worker).copied().unwrap_or(0) + rng.next() % 2;
            Command::WorkerJoin {
                worker_id: worker,
                session_epoch: epoch,
                now_unix_nanos,
            }
        }
        7 => {
            let epoch = state.sessions.get(&worker).copied().unwrap_or(0);
            Command::WorkerLeave {
                worker_id: worker,
                session_epoch: epoch.saturating_sub(rng.next() % 2),
                now_unix_nanos,
            }
        }
        8 => Command::Expire {
            now_unix_nanos,
            lease_timeout_nanos: u128::from(rng.next() % 4_000_000_000),
        },
        _ => {
            let mut drop_ids = Vec::new();
            for _ in 0..rng.below(4) {
                if let Some(client) = rng.pick(&world.clients) {
                    drop_ids.push(client.clone());
                }
            }
            Command::DropClients {
                client_operation_ids: drop_ids,
                now_unix_nanos,
            }
        }
    }
}

fn sorted_state_of(stage: &ActionStage) -> Option<SortedAwaitedActionState> {
    match stage {
        ActionStage::Unknown => None,
        ActionStage::CacheCheck => Some(SortedAwaitedActionState::CacheCheck),
        ActionStage::Queued => Some(SortedAwaitedActionState::Queued),
        ActionStage::Executing => Some(SortedAwaitedActionState::Executing),
        ActionStage::Completed(_) | ActionStage::CompletedFromCache(_) => {
            Some(SortedAwaitedActionState::Completed)
        }
    }
}

const ALL_SORTED: [SortedAwaitedActionState; 4] = [
    SortedAwaitedActionState::CacheCheck,
    SortedAwaitedActionState::Queued,
    SortedAwaitedActionState::Executing,
    SortedAwaitedActionState::Completed,
];

fn check_invariants(state: &AppliedState, context: &str) {
    // I1: every operation is in exactly the sorted index of its stage, and
    // the indices hold nothing else.
    let indexed: usize = ALL_SORTED
        .iter()
        .map(|s| state.sorted.btree_for_sorted_state(*s).len())
        .sum();
    assert_eq!(indexed, state.operations.len(), "I1 index size: {context}");
    for (op, action) in &state.operations {
        let key = SortedAwaitedAction::from(action);
        let want = sorted_state_of(&action.state().stage);
        for s in ALL_SORTED {
            let present = state.sorted.btree_for_sorted_state(s).contains(&key);
            let expected =
                want.is_some_and(|w| core::mem::discriminant(&w) == core::mem::discriminant(&s));
            assert_eq!(present, expected, "I1 {op} in {s:?}: {context}");
        }
        // I6: the record carries the revision apply holds for it.
        assert_eq!(
            state.operation_revision.get(op).copied(),
            Some(action.version_public()),
            "I6 revision of {op}: {context}"
        );
    }
    // I2/I3: client mappings point at live operations, and every operation
    // has a client (one with none is removed, never left behind).
    for (client, op) in &state.client_to_operation {
        assert!(
            state.operations.contains_key(op),
            "I2 {client} -> {op}: {context}"
        );
    }
    for op in state.operations.keys() {
        assert!(
            state.client_to_operation.values().any(|o| o == op),
            "I3 orphan operation {op}: {context}"
        );
    }
    // I4: the dedup map points only at live, unfinished operations.
    for (key, op) in &state.unique_key_to_operation {
        let action = state
            .operations
            .get(op)
            .unwrap_or_else(|| panic!("I4 dedup {key:?} -> missing {op}: {context}"));
        assert!(
            !action.state().stage.is_finished(),
            "I4 dedup {key:?} -> finished {op}: {context}"
        );
    }
    // I5: only executing operations have an owner, and owners name the
    // session membership currently records.
    for (op, owner) in &state.operation_owner {
        let action = state
            .operations
            .get(op)
            .unwrap_or_else(|| panic!("I5 owner of missing {op}: {context}"));
        assert!(
            matches!(action.state().stage, ActionStage::Executing),
            "I5 owner of non-executing {op}: {context}"
        );
        assert_eq!(
            state.sessions.get(&owner.worker_id).copied(),
            Some(owner.session_epoch),
            "I5 owner of {op} names a superseded session: {context}"
        );
    }
}

fn run_seed(seed: u64, steps: usize) {
    let mut rng = Rng(seed);
    let mut world = World {
        clients: Vec::new(),
        workers: (0..3).map(|i| WorkerId(format!("w{i}"))).collect(),
        now: 1_000_000_000_000,
    };
    let mut leader = AppliedState::default();
    let mut follower = AppliedState::default();
    let mut log_now = 0;
    for step in 0..steps {
        let mut cmd = next_command(&mut rng, &mut world, &leader);
        if rng.chance(15) {
            // Proposals committed together, sometimes nested, as the
            // proposer groups them.
            let mut cmds = vec![cmd];
            for _ in 0..rng.below(4) {
                cmds.push(next_command(&mut rng, &mut world, &leader));
            }
            if rng.chance(20) {
                cmds = vec![Command::Batch(cmds)];
            }
            cmd = Command::Batch(cmds);
        }
        let context = format!("seed {seed} step {step} after {cmd:?}");
        let (leader_resp, _) = leader.apply(cmd.clone());
        let (follower_resp, _) = follower.apply(cmd);
        // I9: replicas applying the same log agree, response and state.
        assert_eq!(
            format!("{leader_resp:?}"),
            format!("{follower_resp:?}"),
            "I9 response: {context}"
        );
        // I7: log time never moves backwards.
        assert!(leader.log_now >= log_now, "I7: {context}");
        log_now = leader.log_now;
        check_invariants(&leader, &context);
        if rng.chance(5) {
            // I8: a follower rebuilt from a snapshot is the same state.
            follower = AppliedState::from_portable(follower.to_portable());
        }
        // The full state comparison is costly; responses are compared every
        // step above, the whole state periodically and at the end.
        if step % 25 == 24 || step + 1 == steps {
            assert_eq!(
                leader.canonical_json(),
                follower.canonical_json(),
                "I9 state: {context}"
            );
        }
    }
}

#[test]
fn random_logs_keep_every_invariant() {
    for seed in 0..200 {
        run_seed(seed, 300);
    }
}
