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

use super::*;

fn setup(algorithm: u8) -> (PublicationAuthority, a::Request, ActionBinding) {
    let captured = request(
        algorithm,
        true,
        false,
        "worker-invocation",
        &context().principal,
        false,
    );
    let binding = ActionBinding::capture(&context(), &captured).unwrap();
    (real(&captured), captured, binding)
}
fn direct(
    state: &mut PublicationAuthority,
    binding: &ActionBinding,
    authorization: u64,
    generation: u64,
) -> Candidate {
    let result = ActionResult {
        stdout_raw: format!("direct-result-{generation}").into_bytes().into(),
        ..ActionResult::default()
    };
    let object = Object::for_bytes(binding.result_algorithm(), &result.encode_to_vec()).unwrap();
    state
        .install_physical(key(generation), object, true)
        .unwrap();
    state
        .direct_update_candidate(
            authorization,
            &context(),
            binding,
            result,
            BTreeSet::from([Member {
                object,
                key: key(generation),
                role: Role::Result,
            }]),
        )
        .unwrap()
}
fn roundtrip(state: &PublicationAuthority) {
    let bytes = state.snapshot().unwrap();
    let recovered = PublicationAuthority::restore(&bytes).unwrap();
    assert_eq!(recovered.snapshot().unwrap(), bytes);
    assert_eq!(recovered.scheduler_view(), state.scheduler_view());
    assert_eq!(recovered.protected_keys(), state.protected_keys());
}

#[test]
fn trusted_direct_update_has_no_scheduler_identity_and_survives_later_revocation() {
    for algorithm in [1, 9] {
        let (mut state, _, binding) = setup(algorithm);
        let scheduler = state.scheduler_view();
        state
            .authorize_update(1, &context(), "action", &binding)
            .unwrap();
        let value = direct(&mut state, &binding, 1, 1);
        let member = value.closure.first().unwrap().clone();
        stage(&mut state, 1, value);
        state.publish(1, 1, 100).unwrap();
        assert_eq!(state.scheduler_view(), scheduler);
        assert!(state.operations().is_empty());
        assert_eq!(
            state.stages()[&1].candidate.publisher,
            Publisher::AuthorizedClient { authorization: 1 }
        );
        state.serve("action", 1, state.commit(), 1, 90).unwrap();
        assert!(state.can_read(1, &member));
        state.revoke_update(1, &context()).unwrap();
        assert_eq!(state.heads()["action"], 1);
        assert!(state.can_read(1, &member));
        assert_eq!(state.scheduler_view(), scheduler);
        roundtrip(&state);
        let lookup = request(
            algorithm,
            false,
            false,
            "read-invocation",
            &context().principal,
            false,
        );
        state
            .admit_request(1, "action".into(), &context(), lookup, 80)
            .unwrap();
        assert_eq!(
            state.scheduler_view().attempts,
            Vec::<nativelink_scheduler::authority::AttemptView>::new()
        );
        let a::TerminalResult::Completed(cached) = terminal(&state, 1) else {
            panic!("cache miss")
        };
        assert!(
            cached
                .require_response()
                .unwrap()
                .decode()
                .unwrap()
                .cached_result
        );
        assert_eq!(state.operations()[&1].publication, Some(1));
        roundtrip(&state);
    }
}

#[test]
fn worker_and_authorized_client_heads_follow_commit_order_without_rewriting_operations() {
    for algorithm in [1, 9] {
        for worker_first in [false, true] {
            let (mut state, captured, binding) = setup(algorithm);
            state
                .authorize_update(1, &context(), "action", &binding)
                .unwrap();
            register_real(&mut state);
            state
                .admit_request(1, "action".into(), &context(), captured, 100)
                .unwrap();
            let attempt = assign(&mut state);
            let from_worker = real_candidate(&mut state, algorithm, attempt);
            let exact = from_worker.response.clone();
            let from_client = direct(&mut state, &binding, 1, 2);
            stage(&mut state, 1, from_worker);
            stage(&mut state, 2, from_client);
            let (first, last) = if worker_first { (1, 2) } else { (2, 1) };
            state.publish(first, first, 100).unwrap();
            state.serve("action", first, state.commit(), 1, 90).unwrap();
            let protected = state.stages()[&first]
                .candidate
                .closure
                .first()
                .unwrap()
                .clone();
            state.publish(last, last, 100).unwrap();
            assert_eq!(state.heads()["action"], last);
            assert_eq!(
                terminal(&state, 1),
                a::TerminalResult::Completed(exact.clone())
            );
            assert_eq!(state.scheduler_view().attempts.len(), 1);
            assert!(state.can_read(1, &protected));
            state.revoke_update(1, &context()).unwrap();
            assert_eq!(state.heads()["action"], last);
            assert!(state.can_read(1, &protected));
            let lookup = request(
                algorithm,
                false,
                false,
                "read-invocation",
                &context().principal,
                false,
            );
            state
                .admit_request(2, "action".into(), &context(), lookup, 80)
                .unwrap();
            assert_eq!(state.operations()[&2].publication, Some(last));
            assert_eq!(terminal(&state, 1), a::TerminalResult::Completed(exact));
            assert_eq!(state.scheduler_view().attempts.len(), 1);
            state.lose_index();
            roundtrip(&state);
        }
    }
}

#[test]
fn grants_and_direct_candidates_require_the_complete_registered_binding() {
    for algorithm in [1, 9] {
        let (mut state, _, binding) = setup(algorithm);
        let before = state.snapshot().unwrap();
        for foreign in [
            TrustedContext {
                principal: "foreign".into(),
                ..context()
            },
            TrustedContext {
                execution_config: "foreign".into(),
                ..context()
            },
        ] {
            assert_eq!(
                state.authorize_update(1, &foreign, "action", &binding),
                Err(Error::Unauthorized)
            );
            assert_eq!(state.snapshot().unwrap(), before);
        }
        let mut variants = Vec::new();
        let mut changed = binding.clone();
        changed.context.principal = "foreign".into();
        variants.push(changed);
        let mut changed = binding.clone();
        changed.context.execution_config = "foreign".into();
        variants.push(changed);
        let mut changed = binding.clone();
        changed.action.instance.push('x');
        variants.push(changed);
        let mut changed = binding.clone();
        changed.action.digest_function = if algorithm == 1 { "blake3" } else { "sha256" }.into();
        variants.push(changed);
        let mut changed = binding.clone();
        changed.action.digest = "ff".repeat(32);
        variants.push(changed);
        let mut changed = binding.clone();
        changed.action.digest_size_bytes += 1;
        variants.push(changed);
        let mut changed = binding.clone();
        changed.platform.insert("memory_kb".into(), "4".into());
        variants.push(changed);
        let mut changed = binding.clone();
        changed.do_not_cache = true;
        variants.push(changed);
        for changed in &variants {
            assert_eq!(
                state.authorize_update(1, &context(), "action", changed),
                Err(Error::Unauthorized)
            );
            assert_eq!(state.snapshot().unwrap(), before);
        }
        assert_eq!(
            state.authorize_update(1, &context(), "alias", &binding),
            Err(Error::Unauthorized)
        );
        assert_eq!(state.authorize_client(1), Err(Error::Unauthorized));
        assert_eq!(state.snapshot().unwrap(), before);
        state
            .authorize_update(1, &context(), "action", &binding)
            .unwrap();
        let value = direct(&mut state, &binding, 1, 1);
        let result = ActionResult::decode(value.result_bytes.as_slice()).unwrap();
        let before = state.snapshot().unwrap();
        for changed in variants {
            assert_eq!(
                state.direct_update_candidate(
                    1,
                    &context(),
                    &changed,
                    result.clone(),
                    value.closure.clone()
                ),
                Err(Error::Unauthorized)
            );
        }
        let foreign = TrustedContext {
            execution_config: "foreign".into(),
            ..context()
        };
        assert_eq!(
            state.direct_update_candidate(1, &foreign, &binding, result, value.closure.clone()),
            Err(Error::Unauthorized)
        );
        assert_eq!(state.revoke_update(1, &foreign), Err(Error::Unauthorized));
        let mut wrong = value.clone();
        wrong.action = "alias".into();
        assert!(state.stage(1, wrong, 10, BTreeMap::new()).is_err());
        let mut wrong = value;
        wrong.publisher = Publisher::AuthorizedClient { authorization: 2 };
        assert_eq!(
            state.stage(1, wrong, 10, BTreeMap::new()),
            Err(Error::Unauthorized)
        );
        assert_eq!(state.snapshot().unwrap(), before);
    }
}

#[test]
fn revocation_fences_prepared_updates_and_cannot_be_undone_by_reusing_a_grant() {
    for algorithm in [1, 9] {
        let (mut state, _, binding) = setup(algorithm);
        state
            .authorize_update(1, &context(), "action", &binding)
            .unwrap();
        let value = direct(&mut state, &binding, 1, 1);
        stage(&mut state, 1, value.clone());
        state.revoke_update(1, &context()).unwrap();
        roundtrip(&state);
        let before = state.snapshot().unwrap();
        assert_eq!(state.publish(1, 1, 100), Err(Error::Unauthorized));
        assert_eq!(
            state.stage(2, value, 10, BTreeMap::new()),
            Err(Error::Unauthorized)
        );
        assert_eq!(
            state.authorize_update(1, &context(), "action", &binding),
            Err(Error::Conflict)
        );
        assert_eq!(state.snapshot().unwrap(), before);
        state
            .authorize_update(2, &context(), "action", &binding)
            .unwrap();
        assert_eq!(state.publish(1, 1, 100), Err(Error::Unauthorized));
        let value = direct(&mut state, &binding, 2, 2);
        stage(&mut state, 2, value);
        state.publish(2, 2, 100).unwrap();
        assert_eq!(state.heads()["action"], 2);
        roundtrip(&state);
        let (mut expiry, _, binding) = setup(algorithm);
        expiry
            .authorize_update(1, &context(), "action", &binding)
            .unwrap();
        let value = direct(&mut expiry, &binding, 1, 1);
        stage(&mut expiry, 1, value);
        expiry.advance(11).unwrap();
        let before = expiry.snapshot().unwrap();
        assert_eq!(expiry.publish(1, 1, 100), Err(Error::Stale));
        assert_eq!(expiry.snapshot().unwrap(), before);
    }
}

#[test]
fn direct_update_policy_rejects_noncanonical_success_and_mismatched_digest() {
    for algorithm in [1, 9] {
        let (mut state, _, binding) = setup(algorithm);
        state
            .authorize_update(1, &context(), "action", &binding)
            .unwrap();
        let value = direct(&mut state, &binding, 1, 1);
        let result = ActionResult::decode(value.result_bytes.as_slice()).unwrap();
        let base = ExecuteResponse {
            result: Some(result),
            ..ExecuteResponse::default()
        };
        let mut variants = Vec::new();
        let mut changed = base.clone();
        changed.cached_result = true;
        variants.push(changed);
        let mut changed = base.clone();
        changed.message = "not UpdateActionResult".into();
        variants.push(changed);
        let mut changed = base.clone();
        changed.status = Some(Status::default());
        variants.push(changed);
        let mut changed = base;
        changed.status = Some(Status {
            code: 13,
            ..Status::default()
        });
        variants.push(changed);
        let before = state.snapshot().unwrap();
        for response in variants {
            let mut wrong = value.clone();
            wrong.response = a::ResultPayload::from_response(&response, MAX_PAYLOAD).unwrap();
            assert_eq!(
                state.stage(1, wrong, 10, BTreeMap::new()),
                Err(Error::Invalid)
            );
            assert_eq!(state.snapshot().unwrap(), before);
        }
        let mut wrong = value.clone();
        wrong.result.hash[0] ^= 1;
        assert_eq!(
            state.stage(1, wrong, 10, BTreeMap::new()),
            Err(Error::Invalid)
        );
        let mut failed = ActionResult::decode(value.result_bytes.as_slice()).unwrap();
        failed.exit_code = 1;
        let object = Object::for_bytes(algorithm, &failed.encode_to_vec()).unwrap();
        let mut failed_state = state.clone();
        failed_state.install_physical(key(2), object, true).unwrap();
        let failed_before = failed_state.snapshot().unwrap();
        assert_eq!(
            failed_state.direct_update_candidate(
                1,
                &context(),
                &binding,
                failed,
                BTreeSet::from([Member {
                    object,
                    key: key(2),
                    role: Role::Result
                }])
            ),
            Err(Error::Invalid)
        );
        assert_eq!(failed_state.snapshot().unwrap(), failed_before);
        let mut wrong = value;
        wrong.result.algorithm = if algorithm == 1 { 9 } else { 1 };
        assert_eq!(
            state.stage(1, wrong, 10, BTreeMap::new()),
            Err(Error::Unauthorized)
        );
        assert_eq!(state.snapshot().unwrap(), before);
        let captured = request(
            algorithm,
            true,
            true,
            "no-cache",
            &context().principal,
            false,
        );
        let binding = ActionBinding::capture(&context(), &captured).unwrap();
        let mut no_cache = real(&captured);
        assert_eq!(
            no_cache.authorize_update(1, &context(), "action", &binding),
            Err(Error::Invalid)
        );
    }
}

#[test]
fn snapshot_formats_and_grant_stage_acceptance_revocation_positions_are_relational() {
    for algorithm in [1, 9] {
        let (mut state, _, binding) = setup(algorithm);
        let version2 = String::from_utf8(state.snapshot().unwrap()).unwrap();
        assert!(version2.starts_with("{\"version\":2,"));
        assert_eq!(
            PublicationAuthority::restore(
                version2
                    .replacen("\"version\":2", "\"version\":3", 1)
                    .as_bytes()
            ),
            Err(Error::Corrupt)
        );
        state
            .authorize_update(1, &context(), "action", &binding)
            .unwrap();
        let issued = state.commit();
        let value = direct(&mut state, &binding, 1, 1);
        stage(&mut state, 1, value);
        let staged = state.stages()[&1].authorized_at.unwrap();
        state.publish(1, 1, 100).unwrap();
        let accepted = state.accepted()[&1].commit;
        state.revoke_update(1, &context()).unwrap();
        let revoked = state.commit();
        assert!(issued < staged && staged < accepted && accepted < revoked);
        let encoded = String::from_utf8(state.snapshot().unwrap()).unwrap();
        assert!(encoded.starts_with("{\"version\":3,"));
        let changes = vec![
            ("\"version\":3".into(), "\"version\":2".into()),
            (format!("\"issued_at\":{issued}"), "\"issued_at\":0".into()),
            (
                format!("\"issued_at\":{issued}"),
                format!("\"issued_at\":{staged}"),
            ),
            (
                format!("\"authorized_at\":{staged}"),
                format!("\"authorized_at\":{issued}"),
            ),
            (
                format!("\"authorized_at\":{staged}"),
                "\"authorized_at\":0".into(),
            ),
            (
                format!("\"revoked_at\":{revoked}"),
                format!("\"revoked_at\":{}", staged + 1),
            ),
            (
                format!("\"authorized_at\":{staged}"),
                format!("\"authorized_at\":{accepted}"),
            ),
            (
                format!("\"revoked_at\":{revoked}"),
                format!("\"revoked_at\":{accepted}"),
            ),
            (
                "\"AuthorizedClient\":{\"authorization\":1}".into(),
                "\"AuthorizedClient\":{\"authorization\":2}".into(),
            ),
        ];
        for (old, new) in changes {
            assert!(encoded.contains(&old), "missing target {old}");
            assert_eq!(
                PublicationAuthority::restore(encoded.replacen(&old, &new, 1).as_bytes()),
                Err(Error::Corrupt),
                "accepted {old}"
            );
        }
        let marker = "\"update_authorizations\":";
        let at = encoded.find(marker).unwrap();
        let (prefix, grants) = encoded.split_at(at);
        let after_grants = grants.find(",\"namespaces\":").unwrap();
        let without_grants = format!("{prefix}{}", &grants[after_grants + 1..]);
        for version in [2, 3] {
            let corrupt =
                without_grants.replacen("\"version\":3", &format!("\"version\":{version}"), 1);
            assert_eq!(
                PublicationAuthority::restore(corrupt.as_bytes()),
                Err(Error::Corrupt)
            );
        }
        for (old, new) in [
            (
                "\"principal\":\"trusted-client\"",
                "\"principal\":\"foreign\"",
            ),
            (
                "\"execution_config\":\"fixed-config-v1\"",
                "\"execution_config\":\"foreign\"",
            ),
            ("\"label\":\"action\"", "\"label\":\"alias\""),
        ] {
            assert!(grants.contains(old));
            let corrupt = format!("{prefix}{}", grants.replacen(old, new, 1));
            assert_eq!(
                PublicationAuthority::restore(corrupt.as_bytes()),
                Err(Error::Corrupt)
            );
        }
        roundtrip(&state);
    }
    let mut synthetic = authority(&[("a", false)]);
    let before = synthetic.snapshot().unwrap();
    let (_, _, binding) = setup(1);
    assert_eq!(
        synthetic.authorize_update(1, &context(), "action", &binding),
        Err(Error::Unauthorized)
    );
    assert_eq!(synthetic.snapshot().unwrap(), before);
    assert!(
        String::from_utf8(before)
            .unwrap()
            .starts_with("{\"version\":1,")
    );
}

#[test]
fn grant_rows_are_bounded_and_retained_after_revocation() {
    let (mut state, _, binding) = setup(1);
    for id in 1..=MAX_STAGES as u64 {
        state
            .authorize_update(id, &context(), "action", &binding)
            .unwrap();
        state.revoke_update(id, &context()).unwrap();
    }
    let before = state.snapshot().unwrap();
    assert_eq!(
        state.authorize_update(9, &context(), "action", &binding),
        Err(Error::Bounds)
    );
    assert_eq!(state.revoke_update(1, &context()), Err(Error::Stale));
    assert_eq!(state.snapshot().unwrap(), before);
    roundtrip(&state);
}

#[test]
fn retained_direct_event_positions_cannot_be_reused_by_another_transaction() {
    let mut accepted_corruptions = Vec::new();
    for algorithm in [1, 9] {
        let (mut state, _, binding) = setup(algorithm);
        state
            .authorize_update(1, &context(), "action", &binding)
            .unwrap();
        state
            .authorize_update(2, &context(), "action", &binding)
            .unwrap();
        let first = direct(&mut state, &binding, 1, 1);
        let second = direct(&mut state, &binding, 2, 2);
        stage(&mut state, 1, first);
        stage(&mut state, 2, second);
        state.publish(1, 1, 100).unwrap();
        state.publish(2, 2, 100).unwrap();
        state
            .authorize_update(3, &context(), "action", &binding)
            .unwrap();
        let issued = state.commit();
        state.revoke_update(1, &context()).unwrap();
        let revoked = state.commit();
        let encoded = String::from_utf8(state.snapshot().unwrap()).unwrap();
        let changes = [
            (
                format!("\"issued_at\":{issued}"),
                format!("\"issued_at\":{}", state.accepted()[&1].commit),
            ),
            (
                format!("\"revoked_at\":{revoked}"),
                format!("\"revoked_at\":{}", state.accepted()[&2].commit),
            ),
            (
                format!(
                    "\"authorized_at\":{}",
                    state.stages()[&2].authorized_at.unwrap()
                ),
                format!(
                    "\"authorized_at\":{}",
                    state.stages()[&1].authorized_at.unwrap()
                ),
            ),
        ];
        for (old, new) in changes {
            assert!(encoded.contains(&old));
            if PublicationAuthority::restore(encoded.replacen(&old, &new, 1).as_bytes())
                != Err(Error::Corrupt)
            {
                accepted_corruptions.push(format!("algorithm={algorithm}: {old} -> {new}"));
            }
        }
        roundtrip(&state);
    }
    assert!(
        accepted_corruptions.is_empty(),
        "retained event positions reused: {accepted_corruptions:?}"
    );
}
