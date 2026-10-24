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

//! Real captured requests against the unchanged independent head model.
//!
//! The model action labels are opaque. This fixture gives each label distinct
//! immutable command/action bytes under one explicit trusted context. Physical
//! readiness remains the original comparison's declared trusted observation.

use nativelink_proto::build::bazel::remote::execution::v2::{Action, Command, ExecuteRequest};
use nativelink_scheduler::authority::request::RequestPayload;
use prost_types::Timestamp;

use super::*;

fn context() -> c::TrustedContext {
    c::TrustedContext {
        principal: "independent-request-principal".into(),
        execution_config: "independent-request-config-v1".into(),
    }
}

fn captured_request(label: &str, algorithm: r::Algorithm, skip: bool) -> a::Request {
    let command = Command {
        arguments: vec!["/bin/true".into(), label.into()],
        ..Command::default()
    }
    .encode_to_vec();
    let action = Action {
        command_digest: Some(digest(&object(&command, algorithm))),
        input_root_digest: Some(digest(&object(
            &Directory::default().encode_to_vec(),
            algorithm,
        ))),
        do_not_cache: label == UNCACHED,
        ..Action::default()
    }
    .encode_to_vec();
    let execute = ExecuteRequest {
        instance_name: "independent-real-request".into(),
        action_digest: Some(digest(&object(&action, algorithm))),
        digest_function: match algorithm {
            r::Algorithm::Sha256 => 1,
            r::Algorithm::Blake3 => 9,
        },
        skip_cache_lookup: skip,
        ..ExecuteRequest::default()
    };
    a::Request::from_payload(
        RequestPayload::capture(
            &execute,
            None,
            &Timestamp::default(),
            &context().principal,
            &action,
            &command,
            c::MAX_PAYLOAD,
        )
        .unwrap(),
    )
    .unwrap()
}

struct RealPair {
    pair: Pair,
    algorithm: r::Algorithm,
    originals: BTreeMap<u64, a::Request>,
}
impl RealPair {
    fn new(algorithm: r::Algorithm) -> Self {
        let (scenario, facts) = scenario(algorithm);
        let bindings = scenario
            .actions
            .keys()
            .map(|label| {
                let request = captured_request(label, algorithm, false);
                (
                    label.clone(),
                    c::ActionBinding::capture(&context(), &request).unwrap(),
                )
            })
            .collect();
        let actual = PublicationAuthority::new_real(context(), bindings).unwrap();
        Self {
            pair: Pair::with_actual(scenario, facts, actual),
            algorithm,
            originals: BTreeMap::new(),
        }
    }
    fn admit(&mut self, operation: u64, label: &str, skip: bool, proposed: u64) -> r::Reply {
        let original = captured_request(label, self.algorithm, skip);
        let execution = self
            .pair
            .actual
            .admit_request(
                operation,
                label.into(),
                &context(),
                original.clone(),
                OPERATION_UNTIL,
            )
            .unwrap();
        self.originals.insert(operation, original);
        self.pair
            .record_admission(operation, label, skip, proposed, execution)
    }
    fn assign(&mut self, execution: u64, attempt: u64) {
        self.pair.assign(execution, attempt);
        let view = self.pair.actual.scheduler_view();
        let actual = &view
            .executions
            .iter()
            .find(|row| row.id == self.pair.executions[&execution])
            .unwrap()
            .request;
        let op = view
            .operations
            .iter()
            .find(|row| row.execution == self.pair.executions[&execution])
            .unwrap()
            .id
            .0;
        assert_eq!(actual, &self.originals[&op]);
    }
    fn recovered(&mut self) {
        self.pair.recovered();
        let view = self.pair.actual.scheduler_view();
        for (operation, original) in &self.originals {
            let execution = view
                .operations
                .iter()
                .find(|row| row.id.0 == *operation)
                .unwrap()
                .execution;
            let actual = &view
                .executions
                .iter()
                .find(|row| row.id == execution)
                .unwrap()
                .request;
            // Joined requests in this fixture have identical captured bytes.
            assert_eq!(actual, original);
        }
    }
}

#[test]
fn real_sha256_and_blake3_request_matrix_matches_independent_publication_model() {
    for algorithm in [r::Algorithm::Sha256, r::Algorithm::Blake3] {
        let mut real = RealPair::new(algorithm);
        real.pair.register();
        assert_eq!(real.admit(1, ACTION, true, 1), r::Reply::Started(1));
        real.assign(1, 1);
        real.pair.accept(1);
        real.pair.serve(ACTION, 1, 1);
        real.recovered();
        assert_eq!(real.admit(2, ACTION, false, 2), r::Reply::Hit(1));
        assert_eq!(real.admit(3, ACTION, true, 2), r::Reply::Started(2));
        assert_eq!(real.admit(4, ACTION, true, 7), r::Reply::Joined(2));
        real.pair.actual.cancel_joined(4).unwrap();
        real.pair.expect(&r::Event::CancelJoined(4));
        real.assign(2, 2);
        real.pair.stage(3);
        real.recovered();
        real.pair.prepare(3);
        real.recovered();
        real.pair.publish(3);
        assert_eq!(real.admit(5, UNCACHED, false, 3), r::Reply::Started(3));
        assert_eq!(real.admit(6, UNCACHED, false, 4), r::Reply::Started(4));
        real.assign(3, 3);
        real.assign(4, 4);
        real.pair.accept(4);
        real.pair.accept(5);
        assert_eq!(real.admit(7, ACTION, true, 5), r::Reply::Started(5));
        real.assign(5, 5);
        real.pair.accept(7);
        assert_eq!(real.admit(8, ACTION, true, 6), r::Reply::Started(6));
        real.assign(6, 6);
        real.pair.accept(8);
        assert!(!real.pair.actual.heads().contains_key(UNCACHED));
        assert_eq!(real.pair.actual.heads()[ACTION], 3);
        real.pair.late_write(1);
        real.recovered();
        real.pair.actual.lose_index();
        real.pair.expect(&r::Event::LoseIndex);
        real.pair.actual.rebuild_index();
        real.pair.expect(&r::Event::RebuildIndex);
        assert_eq!(real.pair.actual.index(), &real.pair.expected.index);
        for now in [41, 70, 80, 81] {
            real.pair.actual.advance(now).unwrap();
            real.pair.expect(&r::Event::Advance(now));
            real.recovered();
        }
    }
}

#[test]
fn real_prepared_output_cannot_publish_after_actual_worker_revocation() {
    for algorithm in [r::Algorithm::Sha256, r::Algorithm::Blake3] {
        let mut real = RealPair::new(algorithm);
        real.pair.register();
        real.admit(1, ACTION, true, 1);
        real.assign(1, 1);
        real.pair.stage(1);
        real.pair.prepare(1);
        real.pair.revoke();
        real.recovered();
        let actual = real.pair.actual.snapshot().unwrap();
        let expected = real.pair.expected.snapshot().unwrap();
        assert!(real.pair.actual.publish(1, 1, RESULT_UNTIL).is_err());
        real.pair.position += 1;
        assert!(
            real.pair
                .expected
                .apply(
                    real.pair.position,
                    &r::Event::Publish {
                        stage: 1,
                        publication: 1,
                        until: RESULT_UNTIL,
                    },
                    &real.pair.facts
                )
                .is_err()
        );
        assert_eq!(real.pair.actual.snapshot().unwrap(), actual);
        assert_eq!(real.pair.expected.snapshot().unwrap(), expected);
        real.pair.late_write(1);
        real.recovered();
    }
}
