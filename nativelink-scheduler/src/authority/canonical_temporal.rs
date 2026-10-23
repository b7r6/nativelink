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

//! Read-only admission for the inert canonical owner. Numeric consistency is
//! not evidence of a historical clock sample or a promise of eventual expiry.

use openraft::{EntryPayload, LogId, Vote};
use redb::{ReadableDatabase, ReadableTable};

use super::super::{
    ActiveFormat, AppliedState, AuthorityDomain, CanonicalBootstrap, CanonicalContract,
    ClusterEpoch, ErrorVerb, Identity, Journal, LOGS, META, MeasuredOperation, ReaderCapabilities,
    Result, SavedSnapshot, decode, decode_log, failure, replay_format, validate_metadata,
    validate_singleton_recovery,
};
use super::LIMITS;
use crate::authority::{
    ACK_TIMEOUT, Event, HOLD_COOLDOWN, INTEREST_TIMEOUT, PROGRESS_TIMEOUT, Phase, Request, State,
    TERMINAL_RETENTION,
};

// Protobuf permits 315576000000 seconds plus fractional nanoseconds. The
// seconds profile rounds that fraction up and reserves one strict-expiry tick.
const MAX_FINITE_TIMEOUT: u64 = 315_576_000_001;
pub(super) const MAX_CLOCK: u64 = u64::MAX - (MAX_FINITE_TIMEOUT + 1);

fn request(request: &Request) -> Result<()> {
    // Projection validation makes MAX mean an omitted Action timeout, rather
    // than accepting a synthetic request merely because its integer is MAX.
    request
        .require_payload()
        .map_err(|error| failure(ErrorVerb::Read, error))?;
    if request.execution_timeout != u64::MAX
        && !(1..=MAX_FINITE_TIMEOUT).contains(&request.execution_timeout)
    {
        return Err(failure(ErrorVerb::Read, "invalid canonical finite timeout"));
    }
    Ok(())
}

fn clock(now: u64) -> Result<()> {
    if now > MAX_CLOCK {
        return Err(failure(
            ErrorVerb::Read,
            "canonical clock has no expiry headroom",
        ));
    }
    Ok(())
}

fn anchor(deadline: u64, interval: u64, now: u64) -> Result<()> {
    if deadline
        .checked_sub(interval)
        .is_none_or(|start| start > now)
    {
        return Err(failure(
            ErrorVerb::Read,
            "invalid canonical deadline interval",
        ));
    }
    Ok(())
}

fn state(state: &State) -> Result<()> {
    state
        .check_invariants()
        .map_err(|error| failure(ErrorVerb::Read, error))?;
    if state.limits != LIMITS {
        return Err(failure(
            ErrorVerb::Read,
            "canonical temporal limits mismatch",
        ));
    }
    clock(state.now)?;
    for execution in state.executions.values() {
        request(&execution.request)?;
        if execution.hold_cooldown_until != 0 {
            anchor(execution.hold_cooldown_until, HOLD_COOLDOWN, state.now)?;
        }
        if execution
            .terminal_at
            .is_some_and(|at| at.checked_add(TERMINAL_RETENTION).is_none())
        {
            return Err(failure(ErrorVerb::Read, "canonical retention overflow"));
        }
    }
    for operation in state.operations.values() {
        request(&operation.original_request)?;
        anchor(
            operation.view.interest_deadline.at,
            INTEREST_TIMEOUT,
            state.now,
        )?;
    }
    for attempt in state.attempts.values() {
        let interval = match attempt.phase {
            Phase::Dispatching => ACK_TIMEOUT,
            Phase::Accepted | Phase::Preparing | Phase::Running | Phase::Uploading => {
                PROGRESS_TIMEOUT
            }
            _ => return Err(failure(ErrorVerb::Read, "invalid canonical attempt phase")),
        };
        anchor(attempt.deadline.at, interval, state.now)?;
        if let Some(deadline) = attempt.running_deadline {
            let timeout = state
                .executions
                .get(&attempt.execution)
                .ok_or_else(|| failure(ErrorVerb::Read, "canonical attempt has no execution"))?
                .request
                .execution_timeout;
            if timeout == u64::MAX {
                if deadline != u64::MAX {
                    return Err(failure(
                        ErrorVerb::Read,
                        "omitted canonical timeout is finite",
                    ));
                }
            } else {
                if deadline == u64::MAX {
                    return Err(failure(
                        ErrorVerb::Read,
                        "finite canonical timeout saturated",
                    ));
                }
                anchor(deadline, timeout, state.now)?;
            }
        }
    }
    // Generic invariants preserve enqueue/terminal/heartbeat ordering, exact
    // holds and phase-specific optionality. Uploading may lack a Running anchor;
    // retained revoked attempts and expired deadlines remain valid.
    Ok(())
}

fn event(event: &Event) -> Result<()> {
    match event {
        Event::Submit { request: value, .. } => request(value),
        Event::AdvanceTime { now } => clock(*now),
        _ => Ok(()),
    }
}

fn read<T: serde::de::DeserializeOwned>(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
    epoch: ClusterEpoch,
    key: &str,
) -> Result<T> {
    decode(
        epoch,
        table
            .get(key)
            .map_err(|error| failure(ErrorVerb::Read, error))?
            .ok_or_else(|| failure(ErrorVerb::Read, "missing canonical temporal metadata"))?
            .value(),
    )
}

fn applied(
    value: &AppliedState,
    domain: AuthorityDomain,
    contract: CanonicalContract,
) -> Result<State> {
    if value.format != ActiveFormat::base(domain) {
        return Err(failure(
            ErrorVerb::Read,
            "canonical temporal format is not Base",
        ));
    }
    let result = value.state(domain.epoch, ReaderCapabilities::Base, Some(contract))?;
    state(&result)?;
    Ok(result)
}

/// Persisted applied facts from the audited transaction, before suffix staging.
/// This projection is not a Raft read barrier or permission to seed a clock.
#[derive(Clone, Copy, Debug)]
pub(super) struct AppliedFacts {
    pub(super) last_applied: Option<LogId<u64>>,
    pub(super) now: u64,
}

pub(super) async fn audit(
    journal: &Journal,
    domain: AuthorityDomain,
    contract: CanonicalContract,
    phase: CanonicalBootstrap,
    #[cfg(test)] during_temporal: Option<Box<dyn FnOnce() + Send>>,
) -> Result<AppliedFacts> {
    let node_id = journal.node_id;
    journal
        .io(MeasuredOperation::ReadLogs, 0, move |database| {
            // Journal.io moves the owned queue guard and database Arc into this
            // blocking task. Cancelling its waiter cannot release namespace custody
            // or serialization while this consistent read transaction is active.
            let transaction = database
                .begin_read()
                .map_err(|error| failure(ErrorVerb::Read, error))?;
            #[cfg(test)]
            if let Some(hook) = during_temporal {
                hook();
            }
            let table = transaction
                .open_table(META)
                .map_err(|error| failure(ErrorVerb::Read, error))?;
            validate_metadata(
                &table,
                domain.epoch,
                Some(contract),
                phase == CanonicalBootstrap::Sealed,
            )?;
            if read::<CanonicalBootstrap>(&table, domain.epoch, "canonical_bootstrap")? != phase {
                return Err(failure(
                    ErrorVerb::Read,
                    "canonical temporal phase mismatch",
                ));
            }
            let identity: Identity = read(&table, domain.epoch, "identity")?;
            if identity.node_id != node_id || identity.epoch != domain.epoch {
                return Err(failure(
                    ErrorVerb::Read,
                    "canonical temporal identity mismatch",
                ));
            }
            let mut staged: AppliedState = read(&table, domain.epoch, "applied")?;
            let mut staged_state = applied(&staged, domain, contract)?;
            let applied_position = staged.last_applied;
            let facts = AppliedFacts {
                last_applied: applied_position,
                now: staged_state.now,
            };
            let mut membership = staged.membership.clone();
            let mut snapshot = None;
            let mut committed: Option<LogId<u64>> = None;
            let mut purged: Option<LogId<u64>> = None;
            for row in table
                .iter()
                .map_err(|error| failure(ErrorVerb::Read, error))?
            {
                let (key, value) = row.map_err(|error| failure(ErrorVerb::Read, error))?;
                match key.value() {
                    "identity" | "applied" | "canonical_contract" | "canonical_bootstrap" => {}
                    "vote" => {
                        decode::<Vote<u64>>(domain.epoch, value.value())?;
                    }
                    "committed" => committed = decode(domain.epoch, value.value())?,
                    "purged" => purged = Some(decode(domain.epoch, value.value())?),
                    "snapshot_sequence" => {
                        decode::<u64>(domain.epoch, value.value())?;
                    }
                    "snapshot" => {
                        let saved: SavedSnapshot = decode(domain.epoch, value.value())?;
                        saved.validate(domain.epoch, ReaderCapabilities::Base, Some(contract))?;
                        applied(&saved.applied, domain, contract)?;
                        snapshot = Some(saved.meta);
                    }
                    _ => {
                        return Err(failure(
                            ErrorVerb::Read,
                            "unknown canonical temporal metadata",
                        ));
                    }
                }
            }
            let frontier = committed.max(applied_position);
            let logs = transaction
                .open_table(LOGS)
                .map_err(|error| failure(ErrorVerb::Read, error))?;
            let mut previous = purged;
            let mut first = true;
            // Stream all retained entries, including covered and uncommitted rows.
            // Memory is bounded per record/state; total history and work are not.
            for row in logs
                .iter()
                .map_err(|error| failure(ErrorVerb::Read, error))?
            {
                let (key, value) = row.map_err(|error| failure(ErrorVerb::Read, error))?;
                let entry = decode_log(domain.epoch, key.value(), value.value())?;
                let expected = previous.map_or(Some(0), |id| id.index.checked_add(1));
                let covered_prefix = first
                    && applied_position
                        .is_some_and(|id| entry.log_id.index <= id.index.saturating_add(1))
                    && purged.is_none_or(|id| entry.log_id.index > id.index);
                if expected != Some(entry.log_id.index) && !covered_prefix {
                    return Err(failure(
                        ErrorVerb::Read,
                        "noncontiguous canonical temporal logs",
                    ));
                }
                previous = Some(entry.log_id);
                first = false;
                if let EntryPayload::Membership(value) = &entry.payload {
                    if frontier.is_none_or(|position| entry.log_id.index > position.index) {
                        return Err(failure(
                            ErrorVerb::Read,
                            "pending canonical temporal membership",
                        ));
                    }
                    if applied_position.is_none_or(|position| entry.log_id.index > position.index) {
                        membership =
                            openraft::StoredMembership::new(Some(entry.log_id), value.clone());
                    }
                }
                if let EntryPayload::Normal(batch) = &entry.payload {
                    // The owner has no admitted format profile. Existing opening
                    // already rejects these controls; keep that closed here too.
                    if batch.control.is_some() {
                        return Err(failure(
                            ErrorVerb::Read,
                            "canonical temporal format control",
                        ));
                    }
                    for recorded in &batch.events {
                        event(&recorded.event)?;
                    }
                }
                if applied_position.is_none_or(|position| entry.log_id.index > position.index) {
                    replay_format(
                        &mut staged.format,
                        &mut staged.membership,
                        &entry,
                        ReaderCapabilities::Base,
                    )?;
                    if staged.format != ActiveFormat::base(domain) {
                        return Err(failure(
                            ErrorVerb::Read,
                            "canonical temporal suffix changed format",
                        ));
                    }
                    staged.last_applied = Some(entry.log_id);
                    if let EntryPayload::Normal(batch) = &entry.payload {
                        for recorded in &batch.events {
                            let _reply = staged_state.apply(recorded.clone());
                            // Validate every intermediate poststate, including an
                            // ignored/rejected event, before the next batched event.
                            state(&staged_state)?;
                        }
                    }
                }
            }
            if committed > previous.max(applied_position) {
                return Err(failure(
                    ErrorVerb::Read,
                    "missing canonical temporal committed position",
                ));
            }
            if phase == CanonicalBootstrap::Sealed {
                validate_singleton_recovery(node_id, frontier, &membership, snapshot.as_ref())?;
            }
            Ok(facts)
        })
        .await
}
