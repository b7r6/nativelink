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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use nativelink_config::cas_server::CasConfig;
use nativelink_config::schedulers::SchedulerSpec;
use nativelink_config::stores::StoreSpec;
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

pub(crate) const MAX_INPUT_BYTES: usize = 1024 * 1024;
const MAX_DEPTH: usize = 64;
const MAX_CONSTRUCTIONS: usize = 4096;
const MAX_INVENTORY_ITEMS: usize = 65_536;
const MAX_GRAPH_STEPS: usize = 65_536;

#[derive(Debug, Serialize)]
pub(crate) struct Report {
    format: &'static str,
    deployable: bool,
    candidate_configuration: Option<()>,
    input_bytes: usize,
    schema_validation: &'static str,
    runtime_semantics_validated: bool,
    inventory_complete: bool,
    constructions: Vec<Construction>,
    references: Vec<Reference>,
    fields: Vec<Field>,
    cycles: Vec<Vec<String>>,
    blockers: Vec<Blocker>,
    #[serde(skip)]
    budget: Budget,
}

#[derive(Debug)]
struct Budget {
    items: usize,
    graph_steps: usize,
    exhausted: bool,
}

#[derive(Debug, Serialize)]
struct Construction {
    path: String,
    kind: &'static str,
    selector: String,
    disposition: &'static str,
    provider: Option<&'static str>,
    roles: BTreeSet<&'static str>,
}

#[derive(Debug, Serialize)]
struct Reference {
    path: String,
    kind: &'static str,
    name: String,
    role: &'static str,
    matching_declarations: usize,
    // Only the unique resolved target is retained. Ambiguity never multiplies
    // a declaration list into every referencing edge.
    targets: Vec<String>,
    resolution: &'static str,
    #[serde(skip)]
    owner: Option<String>,
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
struct Field {
    path: String,
    purpose: &'static str,
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
struct Blocker {
    code: &'static str,
    path: String,
    question: &'static str,
}

#[derive(Default)]
struct Graph {
    names: BTreeMap<(&'static str, String), Vec<String>>,
    edges: BTreeMap<String, BTreeSet<String>>,
}

impl Report {
    fn new(input_bytes: usize) -> Self {
        let mut report = Self {
            format: "nativelink-experimental-migration-inventory-v1",
            deployable: false,
            candidate_configuration: None,
            input_bytes,
            schema_validation: "not_attempted",
            runtime_semantics_validated: false,
            inventory_complete: true,
            constructions: Vec::new(),
            references: Vec::new(),
            fields: Vec::new(),
            cycles: Vec::new(),
            blockers: Vec::new(),
            budget: Budget {
                items: MAX_INVENTORY_ITEMS,
                graph_steps: MAX_GRAPH_STEPS,
                exhausted: false,
            },
        };
        for (code, question) in [
            (
                "experimental_inventory_only",
                "A qualified canonical schema and converter are still required; this report is not a configuration or cutover authorization.",
            ),
            (
                "authority_fencing",
                "Which externally fenced authority epoch, membership, worker cohort, and execution domain will own new work after draining the old authority?",
            ),
            (
                "placement_and_namespace",
                "Which explicit physical destinations and namespace identities preserve or deliberately relocate each old placement? No destination is inferred.",
            ),
            (
                "publication_and_collection",
                "Who owns accepted publication and collection, and how are prior writes, retained results, and storage lifetime obligations settled or isolated?",
            ),
            (
                "listener_worker_exclusion",
                "What credential or network evidence excludes workers from every writable AC listener and mutable sink, including external proxies?",
            ),
            (
                "parser_dependency_closure",
                "This experiment uses the current config crate and its dependency closure; it does not establish the final isolated legacy parser or Redis removal.",
            ),
        ] {
            report.block(code, "", question);
        }
        report
    }

    fn block(&mut self, code: &'static str, path: &str, question: &'static str) {
        self.blockers.push(Blocker {
            code,
            path: path.to_owned(),
            question,
        });
    }

    fn field(&mut self, path: &str, purpose: &'static str) {
        if !self.spend_item(path) {
            return;
        }
        self.fields.push(Field {
            path: path.to_owned(),
            purpose,
        });
    }

    fn stop_inventory(&mut self, code: &'static str, path: &str, reason: &'static str) {
        if !self.budget.exhausted {
            self.block(code, path, reason);
        }
        self.inventory_complete = false;
        self.budget.exhausted = true;
    }

    fn spend_item(&mut self, path: &str) -> bool {
        if self.budget.exhausted {
            return false;
        }
        if let Some(remaining) = self.budget.items.checked_sub(1) {
            self.budget.items = remaining;
            true
        } else {
            self.stop_inventory("inventory_work_bound", path, "Inventory traversal exhausted its fixed item budget. The report is incomplete; unresolved references and absent findings are not evidence of safety.");
            false
        }
    }

    fn graph_exhausted(&mut self) {
        self.stop_inventory("graph_work_bound", "", "Reference analysis exhausted its fixed graph-step budget. The report is incomplete; split the topology for review. No missing cycle or ownership finding can be treated as resolved.");
    }

    fn finish(mut self) -> Self {
        self.constructions.sort_by(|a, b| a.path.cmp(&b.path));
        self.references.sort_by(|a, b| a.path.cmp(&b.path));
        self.fields.sort();
        self.fields.dedup();
        self.cycles.sort();
        self.cycles.dedup();
        self.blockers.sort();
        self.blockers.dedup();
        self
    }
}

fn child(path: &str, key: &str) -> String {
    format!("{path}/{}", key.replace('~', "~0").replace('/', "~1"))
}

// Unlike a plain Value, this parser rejects duplicate keys instead of silently
// choosing a destination, reference, or listener policy. Errors are never
// printed: serde errors may include credential values from the input.
struct UniqueValue(Value);

impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct UniqueVisitor;
        impl<'de> Visitor<'de> for UniqueVisitor {
            type Value = UniqueValue;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("JSON5 data without duplicate keys")
            }
            fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Bool(v)))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(UniqueValue(v.into()))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(UniqueValue(v.into()))
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Self::Value, E> {
                serde_json::Number::from_f64(v)
                    .map(|v| UniqueValue(Value::Number(v)))
                    .ok_or_else(|| E::custom("nonfinite number"))
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::String(v.to_owned())))
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }
            fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(UniqueValue(value)) = seq.next_element()? {
                    values.push(value);
                }
                Ok(UniqueValue(Value::Array(values)))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut values = Map::new();
                while let Some((key, UniqueValue(value))) =
                    map.next_entry::<String, UniqueValue>()?
                {
                    if values.insert(key, value).is_some() {
                        return Err(serde::de::Error::custom("duplicate object key"));
                    }
                }
                Ok(UniqueValue(Value::Object(values)))
            }
        }
        deserializer.deserialize_any(UniqueVisitor)
    }
}

// Bound nesting before the recursive JSON5 parser, ignoring delimiters inside
// quoted strings and comments. This is a bound check, not a second JSON5 parser.
fn bounded_depth(bytes: &[u8]) -> bool {
    let mut depth = 0_usize;
    let mut quote = None;
    let mut escaped = false;
    let mut line_comment = false;
    let mut block_comment = false;
    let mut index = 0;
    while let Some(&byte) = bytes.get(index) {
        index += 1;
        if line_comment {
            if matches!(byte, b'\n' | b'\r') {
                line_comment = false;
            } else if byte == 0xe2
                && bytes.get(index) == Some(&0x80)
                && matches!(bytes.get(index + 1), Some(0xa8 | 0xa9))
            {
                // JSON5 also terminates line comments at U+2028/U+2029.
                line_comment = false;
                index += 2;
            }
            continue;
        }
        if block_comment {
            if byte == b'*' && bytes.get(index) == Some(&b'/') {
                block_comment = false;
                index += 1;
            }
            continue;
        }
        if let Some(delimiter) = quote {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == delimiter {
                quote = None;
            }
            continue;
        }
        if matches!(byte, b'\'' | b'"') {
            quote = Some(byte);
            continue;
        }
        if byte == b'/' && bytes.get(index) == Some(&b'/') {
            line_comment = true;
            index += 1;
            continue;
        }
        if byte == b'/' && bytes.get(index) == Some(&b'*') {
            block_comment = true;
            index += 1;
            continue;
        }
        match byte {
            b'{' | b'[' => {
                depth += 1;
                if depth > MAX_DEPTH {
                    return false;
                }
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    true
}

pub(crate) fn inspect(bytes: &[u8]) -> Report {
    let mut report = Report::new(bytes.len());
    if bytes.len() > MAX_INPUT_BYTES || !bounded_depth(bytes) {
        report.inventory_complete = false;
        report.block(
            "input_bound",
            "",
            "Provide at most 1 MiB with bounded nesting; this input was not parsed.",
        );
        return report.finish();
    }
    let Ok(text) = std::str::from_utf8(bytes) else {
        report.inventory_complete = false;
        report.block("invalid_utf8", "", "Provide a UTF-8 JSON5 configuration.");
        return report.finish();
    };
    let Ok(UniqueValue(value)) = serde_json5::from_str(text) else {
        report.inventory_complete = false;
        report.block("invalid_json5_or_duplicate_key", "", "Resolve invalid JSON5, duplicate keys, or nonfinite numbers; no last-value-wins interpretation is permitted.");
        return report.finish();
    };
    if !value.is_object() {
        report.inventory_complete = false;
        report.block("invalid_root", "", "The legacy root must be an object.");
        return report.finish();
    }
    let interpolation = inventory_fields(&value, "", &mut report);
    if report.budget.exhausted {
        return report.finish();
    }
    if interpolation {
        report.schema_validation = "skipped_unresolved_interpolation";
        report.block("unresolved_interpolation", "", "Supply explicit reviewed values offline; environment and home references are never expanded, read, or guessed.");
    } else {
        // Deserialize the original bytes with the current Rust schema. Do not
        // call file/runtime loaders: those normalize defaults and apply selected
        // runtime validations. No tracing subscriber is installed by this tool.
        if let Ok(config) = serde_json5::from_str::<CasConfig>(text) {
            // Exhaustive matches make newly added variants require review.
            for store in &config.stores {
                let _ = current_store_selector(&store.spec);
            }
            for scheduler in config.schedulers.as_deref().unwrap_or_default() {
                let _ = current_scheduler_selector(&scheduler.spec);
            }
            report.schema_validation = "accepted_deserialization_only";
        } else {
            report.schema_validation = "rejected";
            report.block("legacy_schema_rejected", "", "Resolve missing, unknown, or invalid fields against the current Rust schema; this remaining inventory is syntactic only.");
        }
    }
    let mut graph = Graph::default();
    declarations(&value, "stores", "store", &mut report, &mut graph);
    declarations(&value, "schedulers", "scheduler", &mut report, &mut graph);
    if report.budget.exhausted {
        return report.finish();
    }
    routes(&value, &mut report);
    if report.budget.exhausted {
        return report.finish();
    }
    resolve(&mut report, &mut graph);
    report.finish()
}

const fn current_store_selector(spec: &StoreSpec) -> &'static str {
    match spec {
        StoreSpec::CacheMetrics(_) => "cache_metrics",
        StoreSpec::Memory(_) => "memory",
        StoreSpec::ExperimentalCloudObjectStore(_) => "experimental_cloud_object_store",
        StoreSpec::OntapS3ExistenceCache(_) => "ontap_s3_existence_cache",
        StoreSpec::Verify(_) => "verify",
        StoreSpec::CompletenessChecking(_) => "completeness_checking",
        StoreSpec::Compression(_) => "compression",
        StoreSpec::Dedup(_) => "dedup",
        StoreSpec::ExistenceCache(_) => "existence_cache",
        StoreSpec::FastSlow(_) => "fast_slow",
        StoreSpec::Shard(_) => "shard",
        StoreSpec::Filesystem(_) => "filesystem",
        StoreSpec::RefStore(_) => "ref_store",
        StoreSpec::SizePartitioning(_) => "size_partitioning",
        StoreSpec::Grpc(_) => "grpc",
        StoreSpec::RedisStore(_) => "redis_store",
        StoreSpec::Noop(_) => "noop",
        StoreSpec::ExperimentalMongo(_) => "experimental_mongo",
    }
}

const fn current_scheduler_selector(spec: &SchedulerSpec) -> &'static str {
    match spec {
        SchedulerSpec::Simple(_) => "simple",
        SchedulerSpec::Grpc(_) => "grpc",
        SchedulerSpec::CacheLookup(_) => "cache_lookup",
        SchedulerSpec::PropertyModifier(_) => "property_modifier",
        SchedulerSpec::HistoricalResource(_) => "historical_resource",
    }
}

fn disposition(kind: &str, selector: &str) -> Option<&'static str> {
    match (kind, selector) {
        ("scheduler", "simple") => Some("replace_with_committed_authority"),
        ("scheduler", "grpc") => Some("retire_independent_scheduler_federation"),
        ("scheduler", "cache_lookup") => Some("replace_wrapper_with_fixed_request_stage"),
        ("scheduler", "property_modifier") => Some("retire_custom_property_rewriting"),
        ("scheduler", "historical_resource") => Some("retire_wrapper_and_class_policy"),
        ("store", "cache_metrics" | "verify" | "completeness_checking" | "existence_cache") => {
            Some("replace_recursive_wrapper_with_fixed_role")
        }
        ("store", "memory") => {
            Some("cache_or_test_only_requires_explicit_authoritative_destination")
        }
        ("store", "experimental_cloud_object_store") => {
            Some("provider_and_namespace_require_explicit_qualification_or_relocation")
        }
        ("store", "ontap_s3_existence_cache") => {
            Some("retire_wrapper_and_unqualified_ontap_placement")
        }
        ("store", "compression") => Some("legacy_physical_encoding_requires_verified_import"),
        ("store", "dedup") => Some("retire_chunk_index_layout_requires_verified_relocation"),
        ("store", "fast_slow") => {
            Some("replace_composition_with_explicit_cache_gateway_worker_roles")
        }
        ("store", "shard" | "size_partitioning") => {
            Some("retire_arbitrary_placement_preserve_every_destination")
        }
        ("store", "filesystem") => Some("qualify_physical_destination_and_shared_or_local_role"),
        ("store", "ref_store") => Some("replace_alias_with_explicit_role_endpoint_namespace"),
        ("store", "grpc") => Some("replace_recursive_transport_with_authenticated_fixed_route"),
        ("store", "redis_store") => {
            Some("retire_redis_requires_explicit_state_and_data_disposition")
        }
        ("store", "noop") => Some("test_only_never_authoritative_storage"),
        ("store", "experimental_mongo") => Some("unsupported_target_requires_explicit_relocation"),
        _ => None,
    }
}

fn declarations(
    value: &Value,
    field: &str,
    kind: &'static str,
    report: &mut Report,
    graph: &mut Graph,
) {
    let path = child("", field);
    if value.get(field).is_some() {
        report.block("legacy_top_level_selector", &path, "The future canonical entry point refuses this key even when empty or null; this tool does not alter today's runtime.");
    }
    let Some(entries) = value.get(field).and_then(Value::as_array) else {
        return;
    };
    for (index, entry) in entries.iter().enumerate() {
        let path = child(&path, &index.to_string());
        if !report.spend_item(&path) {
            return;
        }
        let Some(name) = entry.get("name").and_then(Value::as_str) else {
            report.block(
                "missing_declaration_name",
                &path,
                "Provide an unambiguous declaration name before resolving references.",
            );
            continue;
        };
        graph
            .names
            .entry((kind, name.to_owned()))
            .or_default()
            .push(path.clone());
        construction(entry, &path, kind, report, graph);
    }
}

fn reference(
    report: &mut Report,
    value: &Value,
    path: &str,
    kind: &'static str,
    role: &'static str,
    owner: Option<&str>,
) {
    if !report.spend_item(path) {
        return;
    }
    if value.is_null() {
        return;
    }
    let Some(name) = value.as_str() else {
        report.block(
            "invalid_reference",
            path,
            "A reference must name exactly one declared component.",
        );
        return;
    };
    report.references.push(Reference {
        path: path.to_owned(),
        kind,
        name: name.to_owned(),
        role,
        matching_declarations: 0,
        targets: Vec::new(),
        resolution: "unresolved",
        owner: owner.map(str::to_owned),
    });
}

fn construction(
    value: &Value,
    path: &str,
    kind: &'static str,
    report: &mut Report,
    graph: &mut Graph,
) {
    if !report.spend_item(path) {
        return;
    }
    if report.constructions.len() >= MAX_CONSTRUCTIONS {
        report.stop_inventory("construction_bound", path, "Inventory stopped at 4096 constructions; split the input for review. The report is incomplete.");
        return;
    }
    let Some(object) = value.as_object() else {
        report.block(
            "invalid_construction",
            path,
            "A construction must contain exactly one known selector.",
        );
        return;
    };
    let selectors: Vec<_> = object
        .iter()
        .filter(|(key, _)| key.as_str() != "name")
        .collect();
    if selectors.len() != 1 {
        report.block(
            "ambiguous_construction",
            path,
            "Resolve absent, multiple, or unknown selector fields; no construction is chosen.",
        );
        return;
    }
    let (selector, body) = selectors[0];
    let Some(disposition) = disposition(kind, selector) else {
        report.block(
            "unknown_selector",
            path,
            "The selector has no reviewed migration disposition; the inventory cannot classify it.",
        );
        return;
    };
    let provider = if selector == "ontap_s3_existence_cache" {
        Some("ontap")
    } else if selector == "experimental_cloud_object_store" {
        match body.get("provider").and_then(Value::as_str) {
            Some("aws") => Some("aws"),
            Some("gcs") => Some("gcs"),
            Some("azure") => Some("azure"),
            Some("ontap") => Some("ontap"),
            Some("r2") => Some("r2"),
            Some("oci") => Some("oci"),
            _ => None,
        }
    } else {
        None
    };
    report.constructions.push(Construction {
        path: path.to_owned(),
        kind,
        selector: selector.clone(),
        disposition,
        provider,
        roles: BTreeSet::new(),
    });
    graph.edges.entry(path.to_owned()).or_default();
    let body_path = child(path, selector);
    if kind == "scheduler" {
        if let Some(nested) = body.get("scheduler") {
            let nested_path = child(&body_path, "scheduler");
            graph
                .edges
                .entry(path.to_owned())
                .or_default()
                .insert(nested_path.clone());
            construction(nested, &nested_path, "scheduler", report, graph);
        }
        if let Some(store) = body.get("ac_store") {
            reference(
                report,
                store,
                &child(&body_path, "ac_store"),
                "store",
                "action_cache",
                Some(path),
            );
        }
        if let Some(store) = body.pointer("/experimental_backend/redis/redis_store") {
            reference(
                report,
                store,
                &format!("{body_path}/experimental_backend/redis/redis_store"),
                "store",
                "scheduler_authority",
                Some(path),
            );
            report.block("scheduler_redis_state", &body_path, "How will old scheduler state be drained and fenced before the committed authority replaces Redis state?");
        }
        return;
    }
    if selector == "ref_store"
        && let Some(name) = body.get("name")
    {
        reference(
            report,
            name,
            &child(&body_path, "name"),
            "store",
            "alias",
            Some(path),
        );
    }
    for field in [
        "backend",
        "fast",
        "slow",
        "lower_store",
        "upper_store",
        "index_store",
        "content_store",
        "cas_store",
    ] {
        if let Some(nested) = body.get(field) {
            if selector == "ontap_s3_existence_cache" && field == "backend" {
                report.field(&child(&body_path, field), "ontap_physical_placement");
                continue;
            }
            let nested_path = child(&body_path, field);
            graph
                .edges
                .entry(path.to_owned())
                .or_default()
                .insert(nested_path.clone());
            construction(nested, &nested_path, "store", report, graph);
        }
    }
    if let Some(shards) = body.get("stores").and_then(Value::as_array) {
        for (index, shard) in shards.iter().enumerate() {
            if let Some(nested) = shard.get("store") {
                let nested_path = format!("{body_path}/stores/{index}/store");
                graph
                    .edges
                    .entry(path.to_owned())
                    .or_default()
                    .insert(nested_path.clone());
                construction(nested, &nested_path, "store", report, graph);
            }
        }
    }
    if selector == "experimental_cloud_object_store" {
        let provider = body.get("provider").and_then(Value::as_str);
        report.block(if provider == Some("azure") { "azure_not_qualified_by_config" } else { "provider_requires_relocation_or_qualification" }, &body_path,
            "Confirm the provider, generation/acknowledgment contract, destination, namespace, and migration evidence; recognizing a provider does not qualify it.");
    }
}

// Report only field locations and their purposes, never endpoint, path,
// credential, arbitrary property, script, or message-template values.
fn inventory_fields(value: &Value, path: &str, report: &mut Report) -> bool {
    if !report.spend_item(path) {
        return false;
    }
    let mut interpolation = false;
    match value {
        Value::String(text) => {
            if text.contains('$') || text.contains('~') {
                report.field(path, "unresolved_interpolation");
                interpolation = true;
            }
        }
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                interpolation |= inventory_fields(value, &child(path, &index.to_string()), report);
                if report.budget.exhausted {
                    break;
                }
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                if report.budget.exhausted {
                    break;
                }
                let field = child(path, key);
                let purpose = match key.as_str() {
                    "endpoint" | "endpoints" | "addresses" | "address" | "uri"
                    | "socket_address" | "connection_string" => Some("endpoint_value_withheld"),
                    "provider"
                    | "region"
                    | "bucket"
                    | "container"
                    | "account_name"
                    | "account_id"
                    | "namespace"
                    | "database"
                    | "cas_collection"
                    | "scheduler_collection" => Some("physical_destination_value_withheld"),
                    "key_prefix" | "content_path" | "temp_path" | "index_path"
                    | "work_directory" | "cache_root" => Some("placement_or_prefix_value_withheld"),
                    "access_key_id"
                    | "secret_access_key"
                    | "sas_url"
                    | "key_file"
                    | "token_file"
                    | "headers"
                    | "additional_environment" => {
                        Some("credential_or_secret_location_values_withheld")
                    }
                    "tls"
                    | "tls_config"
                    | "client_ca_file"
                    | "client_crl_file"
                    | "ca_file"
                    | "cert_file"
                    | "root_certificates"
                    | "experimental_identity_header" => {
                        Some("listener_or_transport_trust_value_withheld")
                    }
                    "compression_algorithm"
                    | "size"
                    | "weight"
                    | "fast_direction"
                    | "slow_direction"
                    | "store_type"
                    | "min_size"
                    | "normal_size"
                    | "max_size"
                    | "experimental_chunking" => {
                        Some("physical_encoding_or_routing_value_withheld")
                    }
                    "supported_platform_properties"
                    | "modifications"
                    | "classes"
                    | "class_by_mnemonic"
                    | "class_by_timeout"
                    | "memory_escalation"
                    | "reservation_hold"
                    | "allocation_strategy" => {
                        report.block("legacy_scheduler_policy", &field, "Which fixed resource requirements and validated normalization preserve the intended behavior when configurable matching, wrapper, or class policy is removed?");
                        Some("retired_scheduler_policy_value_withheld")
                    }
                    "success_message_template" | "failure_message_template" => {
                        report.block("removed_diagnostic_template", &field, "Retire this mutable diagnostic/history association; identify any external integration that depends on it.");
                        Some("removed_diagnostic_template_value_withheld")
                    }
                    "experimental_buck2_invocation_isolation"
                    | "single_use"
                    | "persistent_workers"
                    | "experimental_buck2_file_capture"
                    | "capacity"
                    | "resource_enforcement" => {
                        Some("worker_identity_or_resource_semantics_value_withheld")
                    }
                    _ => None,
                };
                if let Some(purpose) = purpose {
                    report.field(&field, purpose);
                }
                interpolation |= inventory_fields(value, &field, report);
            }
        }
        _ => {}
    }
    interpolation
}

fn each_service(value: &Value, path: &str, mut visit: impl FnMut(&Value, &str)) {
    match value {
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                visit(value, &child(path, &index.to_string()));
            }
        }
        Value::Object(values) => {
            for (instance, value) in values {
                visit(value, &child(path, instance));
            }
        }
        _ => {}
    }
}

fn routes(value: &Value, report: &mut Report) {
    if let Some(servers) = value.get("servers").and_then(Value::as_array) {
        for (index, server) in servers.iter().enumerate() {
            let path = format!("/servers/{index}");
            let Some(services) = server.get("services") else {
                continue;
            };
            let mut writable_ac = false;
            for (service, field, role) in [
                ("cas", "cas_store", "cas"),
                ("ac", "ac_store", "action_cache"),
                ("execution", "cas_store", "cas"),
                ("bytestream", "cas_store", "cas"),
                ("fetch", "fetch_store", "cas"),
                ("push", "push_store", "cas"),
            ] {
                if let Some(entries) = services.get(service) {
                    let service_path = format!("{path}/services/{service}");
                    // The old ByteStream object has a separate instance map.
                    if service == "bytestream"
                        && let Some(names) = entries.get("cas_stores").and_then(Value::as_object)
                    {
                        for (instance, name) in names {
                            reference(
                                report,
                                name,
                                &child(&format!("{service_path}/cas_stores"), instance),
                                "store",
                                role,
                                None,
                            );
                        }
                        continue;
                    }
                    each_service(entries, &service_path, |entry, entry_path| {
                        if let Some(name) = entry.get(field) {
                            reference(report, name, &child(entry_path, field), "store", role, None);
                        }
                        if service == "execution"
                            && let Some(name) = entry.get("scheduler")
                        {
                            reference(
                                report,
                                name,
                                &child(entry_path, "scheduler"),
                                "scheduler",
                                "execution_authority",
                                None,
                            );
                        }
                        if let Some(name) = entry.pointer("/experimental_chunking/index_store") {
                            reference(
                                report,
                                name,
                                &format!("{entry_path}/experimental_chunking/index_store"),
                                "store",
                                "chunk_index",
                                None,
                            );
                        }
                        if service == "ac" && entry.get("read_only") != Some(&Value::Bool(true)) {
                            writable_ac = true;
                        }
                    });
                }
            }
            if let Some(entries) = services.get("capabilities") {
                each_service(
                    entries,
                    &format!("{path}/services/capabilities"),
                    |entry, entry_path| {
                        if let Some(name) = entry.pointer("/remote_execution/scheduler") {
                            reference(
                                report,
                                name,
                                &format!("{entry_path}/remote_execution/scheduler"),
                                "scheduler",
                                "execution_authority",
                                None,
                            );
                        }
                    },
                );
            }
            if let Some(name) = services.pointer("/worker_api/scheduler") {
                reference(
                    report,
                    name,
                    &format!("{path}/services/worker_api/scheduler"),
                    "scheduler",
                    "worker_authority",
                    None,
                );
            }
            if let Some(name) = services.pointer("/experimental_bep/store") {
                reference(
                    report,
                    name,
                    &format!("{path}/services/experimental_bep/store"),
                    "store",
                    "build_event_history",
                    None,
                );
            }
            if writable_ac {
                report.block("writable_ac_boundary_unproven", &path, "Prove worker exclusion on this writable AC route; different ports or a shared permissive mTLS trust do not establish exclusion.");
                if services
                    .get("worker_api")
                    .is_some_and(|value| !value.is_null())
                {
                    report.block("worker_and_writable_ac_share_listener", &path, "Separate worker authority traffic from writable client AC and establish enforced credential or network exclusion.");
                }
            }
        }
    }
    if let Some(workers) = value.get("workers").and_then(Value::as_array) {
        for (index, worker) in workers.iter().enumerate() {
            let Some(local) = worker.get("local") else {
                continue;
            };
            let path = format!("/workers/{index}/local");
            if !report.spend_item(&path) {
                return;
            }
            if let Some(name) = local.get("cas_fast_slow_store") {
                reference(
                    report,
                    name,
                    &child(&path, "cas_fast_slow_store"),
                    "store",
                    "worker_cas",
                    None,
                );
            }
            for (field, role) in [
                ("ac_store", "worker_mutable_ac"),
                ("historical_results_store", "worker_mutable_history"),
            ] {
                if let Some(name) = local.pointer(&format!("/upload_action_result/{field}")) {
                    reference(
                        report,
                        name,
                        &format!("{path}/upload_action_result/{field}"),
                        "store",
                        role,
                        None,
                    );
                    if !name.is_null() {
                        report.block("worker_publication_bypass", &path, "Remove or isolate direct worker AC/history writes; only current-attempt authority acceptance may authorize publication.");
                    }
                }
            }
            if local
                .pointer("/upload_action_result/historical_results_store")
                .is_none_or(Value::is_null)
            {
                report.block("implicit_worker_history_route", &path, "The legacy schema defaults historical storage to the worker CAS. Confirm the effective history-upload strategy and its authority-controlled replacement; omission is not evidence that the worker performs no publication.");
            }
        }
    }
    if let Some(name) = value.pointer("/experimental_origin_events/publisher/store") {
        reference(
            report,
            name,
            "/experimental_origin_events/publisher/store",
            "store",
            "origin_events",
            None,
        );
    }
}

fn graph_step(budget: &mut usize, count: usize) -> Option<()> {
    *budget = budget.checked_sub(count)?;
    Some(())
}

fn reachable(graph: &Graph, start: &str, budget: &mut usize) -> Option<BTreeSet<String>> {
    let mut seen = BTreeSet::new();
    let mut pending = vec![start.to_owned()];
    while let Some(path) = pending.pop() {
        graph_step(budget, 1)?;
        if seen.insert(path.clone())
            && let Some(children) = graph.edges.get(&path)
        {
            graph_step(budget, children.len())?;
            pending.extend(children.iter().cloned());
        }
    }
    Some(seen)
}

fn resolve(report: &mut Report, graph: &mut Graph) {
    if resolve_bounded(report, graph).is_none() {
        report.graph_exhausted();
    }
}

fn resolve_bounded(report: &mut Report, graph: &mut Graph) -> Option<()> {
    for ((_, _), paths) in &graph.names {
        graph_step(&mut report.budget.graph_steps, paths.len())?;
        if paths.len() > 1 {
            for path in paths {
                report.block(
                    "duplicate_declaration",
                    path,
                    "Declaration names must be unique; no last declaration is selected.",
                );
            }
        }
    }
    let mut problems = Vec::new();
    for reference in &mut report.references {
        graph_step(&mut report.budget.graph_steps, 1)?;
        let targets = graph.names.get(&(reference.kind, reference.name.clone()));
        reference.matching_declarations = targets.map_or(0, Vec::len);
        reference.resolution = match reference.matching_declarations {
            0 => "missing",
            1 => "resolved",
            _ => "ambiguous",
        };
        if reference.matching_declarations != 1 {
            problems.push(reference.path.clone());
            continue;
        }
        // Never clone an ambiguous target list into every reference. The full
        // declarations already appear once in the construction inventory.
        reference.targets.push(targets?.first()?.clone());
        if let Some(owner) = &reference.owner {
            graph_step(&mut report.budget.graph_steps, 1)?;
            graph
                .edges
                .entry(owner.clone())
                .or_default()
                .insert(reference.targets[0].clone());
        }
    }
    for path in problems {
        report.block("unresolved_reference", &path, "Resolve this missing or ambiguous component reference before choosing any migration topology.");
    }
    for (from, children) in &graph.edges {
        for to in children {
            if reachable(graph, to, &mut report.budget.graph_steps)?.contains(from) {
                report.cycles.push(vec![from.clone(), to.clone()]);
            }
        }
    }
    if !report.cycles.is_empty() {
        report.block("reference_cycle", "/stores", "Break the reported cyclic graph edges; no expansion or physical placement can be inferred through them.");
    }
    let mut roles: BTreeMap<String, BTreeSet<&'static str>> = BTreeMap::new();
    for reference in &report.references {
        graph_step(&mut report.budget.graph_steps, 1)?;
        if reference.role == "alias" || reference.targets.len() != 1 {
            continue;
        }
        let reachable = reachable(graph, &reference.targets[0], &mut report.budget.graph_steps)?;
        graph_step(&mut report.budget.graph_steps, reachable.len())?;
        for path in reachable {
            roles.entry(path).or_default().insert(reference.role);
        }
    }
    graph_step(
        &mut report.budget.graph_steps,
        report.constructions.len() * 2,
    )?;
    for construction in &mut report.constructions {
        construction.roles = roles.remove(&construction.path).unwrap_or_default();
    }
    let mut mixed = Vec::new();
    for construction in &report.constructions {
        if construction.roles.contains("scheduler_authority")
            && construction.roles.iter().any(|role| {
                matches!(
                    *role,
                    "cas" | "worker_cas" | "action_cache" | "worker_mutable_ac"
                )
            })
        {
            mixed.push(construction.path.clone());
        }
    }
    for path in mixed {
        report.block("mixed_authority_and_data_alias", &path, "Separate scheduler authority, CAS/cache, and mutable publication ownership; shared aliases do not establish safe namespace isolation.");
    }
    let mut memory_roots = Vec::new();
    graph_step(&mut report.budget.graph_steps, report.constructions.len())?;
    let selectors: BTreeMap<_, _> = report
        .constructions
        .iter()
        .map(|node| (node.path.as_str(), node.selector.as_str()))
        .collect();
    for reference in &report.references {
        graph_step(&mut report.budget.graph_steps, 1)?;
        if !matches!(reference.role, "cas" | "worker_cas") || reference.targets.len() != 1 {
            continue;
        }
        let reachable = reachable(graph, &reference.targets[0], &mut report.budget.graph_steps)?;
        graph_step(&mut report.budget.graph_steps, reachable.len())?;
        let leaves: Vec<_> = reachable
            .iter()
            .filter(|path| graph.edges.get(*path).is_some_and(BTreeSet::is_empty))
            .map(|path| selectors.get(path.as_str()).copied())
            .collect();
        if !leaves.is_empty()
            && leaves
                .iter()
                .all(|selector| matches!(selector, Some("memory" | "noop")))
        {
            memory_roots.push(reference.path.clone());
        }
    }
    for path in memory_roots {
        report.block("memory_only_or_discarding_cas", &path, "Provide an explicit qualified authoritative replacement destination and data disposition; memory/noop cannot be silently mapped to a filesystem or cloud path.");
    }
    Some(())
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
