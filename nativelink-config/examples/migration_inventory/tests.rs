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

fn codes(report: &Report) -> BTreeSet<&str> {
    report.blockers.iter().map(|blocker| blocker.code).collect()
}

fn json(report: &Report) -> String {
    serde_json::to_string_pretty(report).unwrap()
}

#[test]
fn nested_placement_and_aliases_preserve_every_branch_without_secret_values() {
    let bytes = br"{
      stores: [
        {name:'root', shard:{stores:[
          {weight:1,store:{size_partitioning:{size:100,lower_store:{ref_store:{name:'redis'}},upper_store:{ref_store:{name:'archive'}}}}},
          {weight:1,store:{dedup:{index_store:{memory:{}},content_store:{compression:{compression_algorithm:{lz4:{}},backend:{ref_store:{name:'archive'}}}}}}}
        ]}},
        {name:'redis',redis_store:{addresses:['redis://user:secret@private.example/3'],key_prefix:'private-prefix'}},
        {name:'archive',experimental_cloud_object_store:{provider:'aws',region:'private-region',bucket:'private-bucket',secret_access_key:'super-secret'}}
      ],
      servers:[{listener:{http:{socket_address:'0.0.0.0:50051'}},services:{cas:[{cas_store:'root'}]}}]
    }";
    let original = bytes.to_vec();
    let report = inspect(bytes);
    assert!(!report.deployable);
    assert_eq!(report.schema_validation, "accepted_deserialization_only");
    assert_eq!(report.constructions.len(), 10);
    assert_eq!(report.references.len(), 4);
    assert!(
        report
            .references
            .iter()
            .all(|reference| reference.resolution == "resolved")
    );
    let archive = report
        .constructions
        .iter()
        .find(|node| node.path == "/stores/2")
        .unwrap();
    assert_eq!(archive.provider, Some("aws"));
    assert!(archive.roles.contains("cas"));
    assert!(
        report.fields.iter().any(
            |field| field.path == "/stores/2/experimental_cloud_object_store/secret_access_key"
        )
    );
    let output = json(&report);
    for secret in [
        "user:secret",
        "private.example",
        "private-prefix",
        "private-region",
        "private-bucket",
        "super-secret",
        "0.0.0.0",
    ] {
        assert!(
            !output.contains(secret),
            "value escaped into report: {secret}"
        );
    }
    assert_eq!(bytes.as_slice(), original);
    assert_eq!(
        output,
        json(&inspect(bytes)),
        "deterministic repeated inventory"
    );
}

#[test]
fn cycles_missing_and_duplicate_references_are_not_silently_resolved() {
    let report = inspect(
        br"{
      stores:[
        {name:'a',fast_slow:{fast:{memory:{}},slow:{ref_store:{name:'b'}}}},
        {name:'b',verify:{backend:{ref_store:{name:'a'}}}},
        {name:'missing',ref_store:{name:'absent'}},
        {name:'duplicate',memory:{}},{name:'duplicate',noop:{}},
        {name:'ambiguous',ref_store:{name:'duplicate'}}
      ],servers:[]
    }",
    );
    assert_eq!(report.schema_validation, "accepted_deserialization_only");
    assert!(codes(&report).is_superset(&BTreeSet::from([
        "reference_cycle",
        "unresolved_reference",
        "duplicate_declaration"
    ])));
    assert!(!report.cycles.is_empty());
    assert_eq!(
        report
            .references
            .iter()
            .find(|r| r.name == "absent")
            .unwrap()
            .resolution,
        "missing"
    );
    let duplicate = report
        .references
        .iter()
        .find(|r| r.name == "duplicate")
        .unwrap();
    assert_eq!(duplicate.resolution, "ambiguous");
    assert_eq!(duplicate.matching_declarations, 2);
    assert!(
        duplicate.targets.is_empty(),
        "ambiguity must not clone every declaration into each reference"
    );
}

#[test]
fn scheduler_state_and_data_aliases_propagate_through_nested_wrappers() {
    let report = inspect(br"{
      stores:[{name:'alias',ref_store:{name:'shared'}},{name:'shared',redis_store:{addresses:['redis://localhost']}}],
      schedulers:[{name:'main',property_modifier:{modifications:[],scheduler:{cache_lookup:{ac_store:'alias',scheduler:{simple:{supported_platform_properties:{cpu_count:'minimum'},experimental_backend:{redis:{redis_store:'shared'}}}}}}}}],
      servers:[{listener:{http:{socket_address:'127.0.0.1:0'}},services:{cas:[{cas_store:'alias'}],execution:[{cas_store:'alias',scheduler:'main'}]}}]
    }");
    assert_eq!(report.schema_validation, "accepted_deserialization_only");
    assert_eq!(
        report
            .constructions
            .iter()
            .filter(|n| n.kind == "scheduler")
            .count(),
        3
    );
    let shared = report
        .constructions
        .iter()
        .find(|node| node.path == "/stores/1")
        .unwrap();
    assert!(shared.roles.is_superset(&BTreeSet::from([
        "cas",
        "action_cache",
        "scheduler_authority"
    ])));
    assert!(codes(&report).is_superset(&BTreeSet::from([
        "scheduler_redis_state",
        "mixed_authority_and_data_alias",
        "legacy_scheduler_policy"
    ])));
}

#[test]
fn writable_ac_colocation_and_direct_worker_publication_block_cutover() {
    let report = inspect(br"{
      stores:[{name:'cas',fast_slow:{fast:{memory:{}},slow:{noop:{}}}},{name:'ac',memory:{}}],
      schedulers:[{name:'main',simple:{}}],
      workers:[{local:{platform_properties:{},worker_api_endpoint:{uri:'grpc://127.0.0.1:50051'},cas_fast_slow_store:'cas',work_directory:'/private/work',upload_action_result:{ac_store:'ac',historical_results_store:'ac',success_message_template:'private-history-token'}}}],
      servers:[{listener:{http:{socket_address:'0.0.0.0:50051',tls:{cert_file:'/private/cert',key_file:'/private/key',client_ca_file:'/shared-ca'}}},services:{cas:[{cas_store:'cas'}],ac:[{ac_store:'ac'}],worker_api:{scheduler:'main'}}}]
    }");
    assert_eq!(report.schema_validation, "accepted_deserialization_only");
    assert!(codes(&report).is_superset(&BTreeSet::from([
        "worker_and_writable_ac_share_listener",
        "writable_ac_boundary_unproven",
        "worker_publication_bypass",
        "removed_diagnostic_template",
        "memory_only_or_discarding_cas"
    ])));
    assert!(!json(&report).contains("private-history-token"));
    assert!(!json(&report).contains("/private/"));
    assert!(!json(&report).contains("/shared-ca"));
}

#[test]
fn readonly_ac_does_not_prove_external_worker_exclusion() {
    let report = inspect(br"{stores:[{name:'ac',memory:{}}],servers:[{listener:{http:{socket_address:'127.0.0.1:0'}},services:{ac:[{ac_store:'ac',read_only:true}]}}]}");
    assert!(!codes(&report).contains("writable_ac_boundary_unproven"));
    assert!(codes(&report).contains("listener_worker_exclusion"));
    assert!(!report.deployable);
}

#[test]
fn interpolation_never_reads_environment_and_unknown_schema_never_passes() {
    // HOME commonly exists, but whether it exists or what it contains cannot
    // affect output. Even shell defaults are kept as unresolved input data.
    let report = inspect(br"{stores:[{name:'disk',filesystem:{content_path:'${HOME}/secret',temp_path:'~/temp'}}],servers:[]}");
    assert_eq!(report.schema_validation, "skipped_unresolved_interpolation");
    assert!(codes(&report).contains("unresolved_interpolation"));
    assert!(!json(&report).contains("${HOME}"));
    assert!(!json(&report).contains("~/temp"));
    let unknown = inspect(br"{stores:[{name:'memory',memory:{invented:7}}],servers:[]}");
    assert_eq!(unknown.schema_validation, "rejected");
    assert!(codes(&unknown).contains("legacy_schema_rejected"));
    let unknown_selector = inspect(br"{stores:[{name:'x',magical:{}}],servers:[]}");
    assert!(codes(&unknown_selector).contains("unknown_selector"));
}

#[test]
fn duplicate_keys_invalid_bytes_and_depth_are_refused_without_error_value_leakage() {
    for input in [
        br"{stores:[],stores:[{name:'hidden-secret',memory:{}}],servers:[]}".as_slice(),
        br"{stores:[{name:'x',memory:{},memory:{}}],servers:[]}",
        br"{stores:[],servers:[],global:{max_open_files:NaN}}",
        b"{invalid secret syntax",
    ] {
        let report = inspect(input);
        assert!(codes(&report).contains("invalid_json5_or_duplicate_key"));
        assert!(report.constructions.is_empty());
        assert!(!json(&report).contains("hidden-secret"));
    }
    assert!(codes(&inspect(&[0xff])).contains("invalid_utf8"));
    assert!(codes(&inspect(&vec![b' '; MAX_INPUT_BYTES + 1])).contains("input_bound"));
    // Closing delimiters in strings cannot hide actual object depth.
    let nested = format!(
        "{}0{}",
        "{x:'}',v:".repeat(MAX_DEPTH + 1),
        "}".repeat(MAX_DEPTH + 1)
    );
    assert!(codes(&inspect(nested.as_bytes())).contains("input_bound"));
    for separator in ['\u{2028}', '\u{2029}'] {
        let commented = format!("// comment{separator}{nested}");
        assert!(codes(&inspect(commented.as_bytes())).contains("input_bound"));
    }
    assert!(bounded_depth(
        br#"{/*[[[[*/stores:[],servers:[],comment:'[[[[[\\"'}"#
    ));
}

#[test]
fn old_service_maps_and_bytestream_references_are_inventoried() {
    let report = inspect(br"{stores:[{name:'memory',memory:{}}],servers:[{listener:{http:{socket_address:'127.0.0.1:0'}},services:{cas:{'a/b':{cas_store:'memory'}},bytestream:{cas_stores:{'a/b':'memory'}}}}]}");
    assert_eq!(report.schema_validation, "accepted_deserialization_only");
    assert_eq!(report.references.len(), 2);
    assert!(report.references.iter().all(|r| r.path.contains("a~1b")));
}

#[test]
fn all_current_selectors_have_an_explicit_disposition() {
    for selector in [
        "cache_metrics",
        "memory",
        "experimental_cloud_object_store",
        "ontap_s3_existence_cache",
        "verify",
        "completeness_checking",
        "compression",
        "dedup",
        "existence_cache",
        "fast_slow",
        "shard",
        "filesystem",
        "ref_store",
        "size_partitioning",
        "grpc",
        "redis_store",
        "noop",
        "experimental_mongo",
    ] {
        assert!(disposition("store", selector).is_some());
    }
    for selector in [
        "simple",
        "grpc",
        "cache_lookup",
        "property_modifier",
        "historical_resource",
    ] {
        assert!(disposition("scheduler", selector).is_some());
    }
    for key in ["stores", "schedulers"] {
        for value in ["null", "[]"] {
            let input = format!("{{stores:[],servers:[],{key}:{value}}}");
            // Duplicate stores must fail the lexical parse, not disappear.
            let report = inspect(input.as_bytes());
            assert!(codes(&report).contains(if key == "stores" {
                "invalid_json5_or_duplicate_key"
            } else {
                "legacy_top_level_selector"
            }));
        }
    }
}

#[test]
fn omitted_history_store_is_not_evidence_of_no_worker_publication() {
    for upload in ["", ",upload_action_result:{historical_results_store:null}"] {
        let input = format!(
            "{{stores:[{{name:'cas',fast_slow:{{fast:{{memory:{{}}}},slow:{{noop:{{}}}}}}}}],servers:[],workers:[{{local:{{platform_properties:{{}},worker_api_endpoint:{{uri:'grpc://localhost:1'}},cas_fast_slow_store:'cas',work_directory:'/unused'{upload}}}}}]}}"
        );
        let report = inspect(input.as_bytes());
        assert_eq!(report.schema_validation, "accepted_deserialization_only");
        assert!(codes(&report).contains("implicit_worker_history_route"));
        assert!(
            !report
                .references
                .iter()
                .any(|reference| reference.role == "worker_mutable_history"),
            "a question is not a claim that a live write occurred"
        );
    }
}

#[test]
fn repeated_ambiguous_references_never_clone_the_target_cross_product() {
    let mut stores = vec![serde_json::json!({"name":"duplicate","memory":{}}); 1024];
    stores.extend((0..1024).map(
        |index| serde_json::json!({"name":format!("ref-{index}"),"ref_store":{"name":"duplicate"}}),
    ));
    let input = serde_json::to_vec(&serde_json::json!({"stores":stores,"servers":[]})).unwrap();
    let report = inspect(&input);
    assert!(report.inventory_complete);
    assert_eq!(report.references.len(), 1024);
    assert!(
        report
            .references
            .iter()
            .all(|reference| reference.matching_declarations == 1024
                && reference.targets.is_empty()
                && reference.resolution == "ambiguous")
    );
    assert!(codes(&report).contains("duplicate_declaration"));
    assert!(codes(&report).contains("unresolved_reference"));
    assert!(
        json(&report).len() < 2 * MAX_INPUT_BYTES,
        "refuse quadratic target amplification"
    );

    let stores = vec![serde_json::json!({"name":"duplicate","memory":{}}); MAX_CONSTRUCTIONS + 1];
    let input = serde_json::to_vec(&serde_json::json!({"stores":stores,"servers":[]})).unwrap();
    let report = inspect(&input);
    assert!(!report.inventory_complete);
    assert!(codes(&report).contains("construction_bound"));
    assert_eq!(report.constructions.len(), MAX_CONSTRUCTIONS);
}

#[test]
fn traversal_and_long_chain_analysis_exhaust_explicit_work_budgets() {
    let input = serde_json::to_vec(
        &serde_json::json!({"stores":[],"servers":[],"unrecognized":vec![0;MAX_INVENTORY_ITEMS]}),
    )
    .unwrap();
    assert!(input.len() < MAX_INPUT_BYTES);
    let report = inspect(&input);
    assert!(!report.inventory_complete);
    assert!(codes(&report).contains("inventory_work_bound"));
    assert_eq!(report.schema_validation, "not_attempted");
    assert!(report.constructions.is_empty());

    let mut stores: Vec<_> = (0..512).map(|index| serde_json::json!({"name":format!("node-{index}"),"ref_store":{"name":format!("node-{}",index+1)}})).collect();
    stores.push(serde_json::json!({"name":"node-512","memory":{}}));
    let input = serde_json::to_vec(&serde_json::json!({"stores":stores,"servers":[]})).unwrap();
    let report = inspect(&input);
    assert_eq!(report.schema_validation, "accepted_deserialization_only");
    assert!(!report.inventory_complete);
    assert!(codes(&report).contains("graph_work_bound"));
    assert!(!report.deployable);
    assert_eq!(
        json(&report),
        json(&inspect(&input)),
        "budget exhaustion must itself be deterministic"
    );
}
