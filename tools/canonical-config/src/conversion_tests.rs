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

use serde_json::{Value, json};

use super::convert;

const SOURCE: &[u8] = include_bytes!("../fixtures/legacy.json");
const MAPPING: &[u8] = include_bytes!("../fixtures/mapping.json");

fn source() -> Value {
    serde_json::from_slice(SOURCE).unwrap()
}
fn mapping() -> Value {
    serde_json::from_slice(MAPPING).unwrap()
}
fn bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}

#[test]
fn complete_proposal_preserves_all_roles_and_never_qualifies_cutover() {
    let report = convert(SOURCE, MAPPING);
    assert!(report.mapping_complete, "{:?}", report.blockers);
    assert!(report.blockers.is_empty());
    assert!(!report.deployable);
    assert!(!report.cutover_qualified);
    assert_eq!(report.diff.len(), 9);
    let encoded = serde_json::to_value(&report).unwrap();
    assert_eq!(encoded["candidate_configuration"], mapping()["candidate"]);
    assert_eq!(
        encoded["diff"]["cas"]["before"]["declared_encoding"],
        mapping()["declarations"]["source_data_encoding"]
    );
    assert_eq!(
        encoded["diff"]["cas"]["proposed"]["declared_namespace_owner"],
        mapping()["declarations"]["proposed_namespace_owner"]
    );
    assert_eq!(
        encoded["diff"]["publication_and_diagnostics"]["before"]["declared_source_ac_owner"],
        mapping()["declarations"]["source_ac_owner"]
    );
    assert_eq!(
        encoded["diff"]["collection"]["before"]["declared_source_owner"],
        mapping()["declarations"]["source_collection_owner"]
    );
    assert_eq!(
        encoded["unverified_declarations"],
        mapping()["declarations"]
    );
    assert_eq!(
        encoded["diff"]["cas"]["before"]["store"],
        source()["stores"][0]
    );
    assert_eq!(
        encoded["diff"]["temporary_action_cache"]["before"]["store"],
        source()["stores"][1]
    );
    assert_eq!(
        encoded["diff"]["authority"]["before"]["scheduler"],
        source()["schedulers"][0]
    );
    assert_eq!(
        encoded["diff"]["client_listener"]["before"]["services"]["ac"][0]["ac_store"],
        "source-ac"
    );
    assert_eq!(
        encoded["diff"]["worker_transfer"]["before"]["listener"],
        "source-worker"
    );
    assert_eq!(encoded["diff"]["collection"]["proposed"]["enabled"], false);
    assert_eq!(
        encoded["diff"]["collection"]["proposed"]["ownership_granted"],
        false
    );
    assert_eq!(
        encoded["diff"]["publication_and_diagnostics"]["proposed"]["worker_templates"],
        "removed"
    );
    assert!(
        report
            .runtime_gates
            .iter()
            .any(|gate| gate.contains("outstanding requests"))
    );
    assert_eq!(
        serde_json::to_vec(&report).unwrap(),
        serde_json::to_vec(&convert(SOURCE, MAPPING)).unwrap()
    );
}

#[test]
fn every_required_declaration_must_be_explicit_and_nonempty() {
    for key in mapping()["declarations"].as_object().unwrap().keys() {
        let mut facts = mapping();
        facts["declarations"].as_object_mut().unwrap().remove(key);
        let report = convert(SOURCE, &bytes(&facts));
        assert!(!report.mapping_complete, "{key}");
        assert!(report.candidate_configuration.is_none(), "{key}");
        assert!(!report.blockers.is_empty());
        if mapping()["declarations"][key].is_string() {
            let mut facts = mapping();
            facts["declarations"][key] = json!("");
            assert!(
                convert(SOURCE, &bytes(&facts))
                    .candidate_configuration
                    .is_none(),
                "{key}"
            );
        }
    }
}

#[test]
fn source_tls_credential_locations_are_required_and_preserved_without_opening() {
    let mut legacy = source();
    let tls = json!({"cert_file":"/old-credentials/worker-server.pem", "key_file":"/old-credentials/worker-server.key", "client_ca_file":"/old-credentials/worker-ca.pem", "client_crl_file":"/old-credentials/worker-crl.pem"});
    legacy["servers"][1]["listener"]["http"]["tls"] = tls.clone();
    assert_eq!(
        convert(&bytes(&legacy), MAPPING).blockers[0].code,
        "source_transfer_credentials"
    );
    let mut facts = mapping();
    facts["declarations"]["source_worker_credential_files"] = json!([
        "/old-credentials/server-ca.pem",
        "/old-credentials/client.pem",
        "/old-credentials/client.key"
    ]);
    let report = convert(&bytes(&legacy), &bytes(&facts));
    assert!(report.mapping_complete, "{:?}", report.blockers);
    let encoded = serde_json::to_value(report).unwrap();
    assert_eq!(
        encoded["diff"]["worker_listener"]["before"]["listener"]["http"]["tls"],
        tls
    );
    assert_eq!(
        encoded["diff"]["worker_transfer"]["before"]["credential_locations"],
        facts["declarations"]["source_worker_credential_files"]
    );
}

#[test]
fn unsupported_placement_and_encoding_remain_blocking_on_every_branch() {
    let mut legacy = source();
    legacy["stores"][0] = json!({"name":"source-cas","shard":{"stores":[
        {"size_partitioning":{"lower_store":{"redis":{"addresses":["redis://secret@example.invalid"]}},"upper_store":{"experimental_cloud_object_store":{"provider":"aws","bucket":"source-bucket","access_key":"DO-NOT-EMIT"}}}},
        {"dedup":{"index_store":{"grpc":{"uri":"http://invalid"}},"content_store":{"compression":{"backend":{"memory":{}}}}}}
    ]}});
    let report = convert(&bytes(&legacy), MAPPING);
    assert!(report.candidate_configuration.is_none());
    for suffix in [
        "/shard",
        "/size_partitioning",
        "/redis",
        "/dedup",
        "/grpc",
        "/compression",
        "/memory",
    ] {
        assert!(
            report
                .blockers
                .iter()
                .any(|error| error.path.ends_with(suffix)),
            "{suffix}"
        );
    }
    let serialized = serde_json::to_string(&report).unwrap();
    assert!(!serialized.contains("DO-NOT-EMIT"));
    assert!(!serialized.contains("secret@example"));
}

#[test]
fn worker_publication_templates_and_redis_authority_need_disposition() {
    let mut legacy = source();
    legacy["workers"] = json!([{"local":{"upload_action_result":{"ac_store":"source-ac","historical_results_store":"source-cas","success_message_template":"DO-NOT-EMIT"}}}]);
    legacy["schedulers"][0]["simple"]["redis"] = json!({"store":"source-cas"});
    let report = convert(&bytes(&legacy), MAPPING);
    for code in [
        "legacy_worker_publication",
        "removed_diagnostic_template",
        "unsupported_backend_or_authority",
    ] {
        assert!(
            report.blockers.iter().any(|error| error.code == code),
            "{code}"
        );
    }
    assert!(report.candidate_configuration.is_none());
    assert!(
        !serde_json::to_string(&report)
            .unwrap()
            .contains("DO-NOT-EMIT")
    );
}

#[test]
fn ambiguous_bindings_aliases_and_unknown_source_semantics_refuse() {
    let mutations = [
        ("/stores/1/name", json!("source-cas")),
        ("/servers/1/services/worker_api/scheduler", json!("missing")),
        (
            "/stores/1/filesystem/content_path",
            json!("/source/cas/content"),
        ),
        (
            "/schedulers/0/simple/worker_allocation_strategy",
            json!("least_loaded"),
        ),
        (
            "/stores/0/filesystem/eviction_policy",
            json!({"max_bytes":100}),
        ),
    ];
    for (path, replacement) in mutations {
        let mut legacy = source();
        let (parent, key) = path.rsplit_once('/').unwrap();
        legacy.pointer_mut(parent).unwrap()[key] = replacement;
        let report = convert(&bytes(&legacy), MAPPING);
        assert!(report.candidate_configuration.is_none(), "{path}");
        assert!(!report.blockers.is_empty(), "{path}");
    }
}

#[test]
fn any_worker_writable_ac_route_blocks_the_mapping() {
    let mut legacy = source();
    legacy["servers"][1]["services"]["ac"] = legacy["servers"][0]["services"]["ac"].clone();
    let report = convert(&bytes(&legacy), MAPPING);
    assert_eq!(report.blockers[0].code, "unsupported_listener_boundary");
    let mut facts = mapping();
    facts["source_bindings"]["worker_transfer_listener"] = json!("source-client");
    assert_eq!(
        convert(SOURCE, &bytes(&facts)).blockers[0].code,
        "unsupported_listener_boundary"
    );
}

#[test]
fn cold_destinations_and_namespace_separation_are_mandatory_declarations() {
    for (path, replacement) in [
        ("/candidate/cas/root", json!("/source/cas/content/new")),
        (
            "/candidate/temporary_action_cache/namespace_id",
            json!("c".repeat(64)),
        ),
        ("/declarations/data_disposition", json!("reuse_old_ac")),
    ] {
        let mut facts = mapping();
        *facts.pointer_mut(path).unwrap() = replacement;
        assert!(
            convert(SOURCE, &bytes(&facts))
                .candidate_configuration
                .is_none(),
            "{path}"
        );
    }
}

#[test]
fn both_inputs_refuse_duplicates_and_interpolation_without_printing_values() {
    for legacy in [
        br"{stores:[],stores:[]}".as_slice(),
        br#"{secret:"$PRIVATE_VALUE"}"#,
    ] {
        let report = convert(legacy, MAPPING);
        assert!(report.candidate_configuration.is_none());
        assert!(
            !serde_json::to_string(&report)
                .unwrap()
                .contains("PRIVATE_VALUE")
        );
    }
    for facts in [
        br"{candidate:null,candidate:{}}".as_slice(),
        br#"{credential:"~/PRIVATE_VALUE"}"#,
    ] {
        let report = convert(SOURCE, facts);
        assert!(report.candidate_configuration.is_none());
        assert!(
            !serde_json::to_string(&report)
                .unwrap()
                .contains("PRIVATE_VALUE")
        );
    }
}

#[test]
fn diagnostic_inventory_is_bounded_and_never_looks_complete_when_truncated() {
    let legacy = json!({"stores":vec![json!({"memory":{}});200]});
    let report = convert(&bytes(&legacy), MAPPING);
    assert!(report.blockers.len() <= super::MAX_DIAGNOSTICS + 2);
    assert!(
        report
            .blockers
            .iter()
            .any(|error| error.code == "diagnostic_bound")
    );
    assert!(report.candidate_configuration.is_none());
    assert!(!report.mapping_complete);
}
