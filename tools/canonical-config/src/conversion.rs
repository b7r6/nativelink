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

use std::collections::BTreeMap;

use nativelink_canonical_config::{
    Error, Manifest, overlaps, parse_data, require_absolute_path, require_text,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::legacy::{Legacy, Server, Store};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MappingFacts {
    format: MappingFormat,
    source_bindings: Bindings,
    candidate: Manifest,
    declarations: Declarations,
}

#[derive(Debug, Deserialize)]
enum MappingFormat {
    #[serde(rename = "experimental_filesystem_mapping_v1")]
    ExperimentalFilesystemMappingV1,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Bindings {
    cas_store: String,
    ac_store: String,
    scheduler: String,
    instance_name: String,
    client_listener: String,
    worker_listener: String,
    /// A named source listener offering CAS/ByteStream, explicitly supplied.
    worker_transfer_listener: String,
}

/// Required operator declarations, never facts inferred by probing the host.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Declarations {
    source_cas_namespace: String,
    source_ac_namespace: String,
    source_ac_owner: String,
    source_collection_owner: String,
    proposed_namespace_owner: String,
    source_data_encoding: String,
    source_encoding_evidence_ref: String,
    source_worker_credential_files: Vec<String>,
    namespace_ownership_evidence_ref: String,
    listener_worker_exclusion_evidence_ref: String,
    credential_placement_evidence_ref: String,
    old_execution_domain: String,
    old_domain_fencing_evidence_ref: String,
    new_worker_cohort_evidence_ref: String,
    external_worker_publication_and_template_retirement_evidence_ref: String,
    data_disposition: ColdDisposition,
}

#[derive(Debug, Deserialize, Serialize)]
enum ColdDisposition {
    #[serde(rename = "fresh_namespaces_leave_old_data_untouched")]
    FreshNamespacesLeaveOldDataUntouched,
}

#[derive(Debug, Serialize)]
pub struct Report {
    #[serde(rename = "report_format")]
    pub format: &'static str,
    pub mapping_complete: bool,
    pub deployable: bool,
    pub cutover_qualified: bool,
    pub candidate_configuration: Option<Manifest>,
    pub blockers: Vec<Error>,
    pub diff: BTreeMap<String, Difference>,
    pub unverified_declarations: Option<Value>,
    pub runtime_gates: &'static [&'static str],
}

#[derive(Debug, Serialize)]
pub struct Difference {
    pub before: Value,
    pub proposed: Value,
}

const GATES: &[&str] = &[
    "No runtime consumes this experimental format.",
    "Verify physical namespace identity, exclusive ownership, path aliasing and same-filesystem requirements.",
    "Prove cold destinations and isolation from old workers and outstanding requests; no import is performed.",
    "Verify credential contents and worker exclusion from every writable AC route, including external ingress.",
    "Verify old-domain fencing and new-worker cohort/incarnation lifecycle before admission.",
    "Qualify the durable clock, admission/recovery profile, fixed constructors and combined cutover.",
    "Collection remains disabled; this report grants no collector ownership.",
];

const MAX_DIAGNOSTICS: usize = 128;
const MAX_DIAGNOSTIC_PATH: usize = 2048;

impl Report {
    const fn empty() -> Self {
        Self {
            format: "experimental_canonical_conversion_v1",
            mapping_complete: false,
            deployable: false,
            cutover_qualified: false,
            candidate_configuration: None,
            blockers: Vec::new(),
            diff: BTreeMap::new(),
            unverified_declarations: None,
            runtime_gates: GATES,
        }
    }
}

pub fn convert(legacy_bytes: &[u8], mapping_bytes: &[u8]) -> Report {
    let mut report = Report::empty();
    let source = match parse_data(legacy_bytes) {
        Ok(value) => value,
        Err(mut error) => {
            error.path.insert_str(0, "/legacy");
            report.blockers.push(error);
            return report;
        }
    };
    unsupported(&source, "/legacy", &mut report.blockers);
    if report.blockers.len() == MAX_DIAGNOSTICS {
        report.blockers.push(Error::new(
            "diagnostic_bound",
            "/legacy",
            "Diagnostic inventory is incomplete at its bound; conversion is blocked.",
        ));
    }
    let legacy = serde_json::from_value::<Legacy>(source);
    let mapping = parse_data(mapping_bytes).and_then(|value| {
        serde_json::from_value::<MappingFacts>(value).map_err(|_| Error::new(
            "mapping_schema", "/mapping", "Supply every explicit mapping fact; missing, unknown or invalid fields block conversion.",
        ))
    });
    let Ok(legacy) = legacy else {
        report.blockers.push(Error::new("unsupported_legacy_shape", "/legacy", "Only the documented direct-filesystem/simple-scheduler source shape is supported; no fields are ignored."));
        return report;
    };
    let mapping = match mapping {
        Ok(mapping) => mapping,
        Err(error) => {
            report.blockers.push(error);
            return report;
        }
    };
    if let Err(error) = validate(&legacy, &mapping) {
        report.blockers.push(error);
    }
    if !report.blockers.is_empty() {
        return report;
    }
    let bindings = &mapping.source_bindings;
    // validate() establishes unique complete source bindings.
    let cas = legacy
        .stores
        .iter()
        .find(|store| store.name == bindings.cas_store)
        .unwrap();
    let sink = legacy
        .stores
        .iter()
        .find(|store| store.name == bindings.ac_store)
        .unwrap();
    let client = legacy
        .servers
        .iter()
        .find(|server| server.name == bindings.client_listener)
        .unwrap();
    let worker = legacy
        .servers
        .iter()
        .find(|server| server.name == bindings.worker_listener)
        .unwrap();
    let transfer = legacy
        .servers
        .iter()
        .find(|server| server.name == bindings.worker_transfer_listener)
        .unwrap();
    let candidate = &mapping.candidate;
    let mut add = |role: &str, before: Value, proposed: Value| {
        report
            .diff
            .insert(role.to_owned(), Difference { before, proposed });
    };
    add(
        "authority",
        json!({"scheduler": legacy.schedulers[0], "execution_domain": mapping.declarations.old_execution_domain}),
        json!({"identity": candidate.authority, "declared_namespace_owner": mapping.declarations.proposed_namespace_owner, "algorithm": "fixed durable singleton", "reader_profile": "fixed by executable; unqualified"}),
    );
    for (role, store, namespace, proposed) in [
        (
            "cas",
            cas,
            &mapping.declarations.source_cas_namespace,
            &candidate.cas,
        ),
        (
            "temporary_action_cache",
            sink,
            &mapping.declarations.source_ac_namespace,
            &candidate.temporary_action_cache,
        ),
    ] {
        add(
            role,
            json!({"provider": "filesystem", "store": store, "declared_namespace": namespace, "declared_encoding": mapping.declarations.source_data_encoding, "provider_endpoint": null, "bucket_or_container": null, "shard_or_dedup": null}),
            json!({"provider": "filesystem", "namespace": proposed, "declared_namespace_owner": mapping.declarations.proposed_namespace_owner, "content_child": "content", "temporary_child": "temp", "encoding": "fixed retained FilesystemStore; physical preflight required", "data_disposition": mapping.declarations.data_disposition, "eviction": "disabled for authoritative roles", "provider_endpoint": null, "bucket_or_container": null, "shard_or_dedup": null}),
        );
    }
    add(
        "client_listener",
        json!(client),
        json!({"listener": candidate.listeners.client, "routes": ["Execution", "Operations", "ActionCache", "CAS", "ByteStream", "Capabilities"], "runtime_wiring": "unqualified"}),
    );
    add(
        "worker_listener",
        json!(worker),
        json!({"listener": candidate.listeners.worker, "routes": ["AuthorityWorkerApi", "CAS", "ByteStream"], "writable_ac": false}),
    );
    add(
        "worker_transfer",
        json!({"listener": transfer.name, "address": transfer.listener.http.socket_address, "tls": transfer.listener.http.tls, "credential_locations": mapping.declarations.source_worker_credential_files, "source_bindings": bindings}),
        json!(candidate.worker.transfer),
    );
    add(
        "worker_local_execution",
        json!({"embedded_workers": "absent", "external_cohort": "declared, not inspected"}),
        json!(candidate.worker),
    );
    add(
        "publication_and_diagnostics",
        json!({"declared_source_ac_owner": mapping.declarations.source_ac_owner, "embedded_diagnostic_templates": "absent", "external_retirement_declaration": mapping.declarations.external_worker_publication_and_template_retirement_evidence_ref}),
        json!({"worker_ac_publication": "canonical authority only", "trusted_client_ac_updates": "fixed client role", "accepted_history": "canonical authority in CAS", "worker_templates": "removed"}),
    );
    add(
        "collection",
        json!({"declared_source_owner": mapping.declarations.source_collection_owner, "ownership_verified": false}),
        json!({"enabled": false, "ownership_granted": false}),
    );
    report.unverified_declarations = Some(json!(mapping.declarations));
    report.candidate_configuration = Some(mapping.candidate);
    report.mapping_complete = true;
    report
}

fn validate(legacy: &Legacy, mapping: &MappingFacts) -> Result<(), Error> {
    let MappingFormat::ExperimentalFilesystemMappingV1 = mapping.format;
    mapping.candidate.validate()?;
    let bindings = &mapping.source_bindings;
    if legacy.stores.len() != 2
        || legacy.schedulers.len() != 1
        || legacy.servers.len() != 2
        || legacy
            .workers
            .as_ref()
            .is_some_and(|workers| !workers.is_empty())
        || bindings.cas_store == bindings.ac_store
        || bindings.client_listener == bindings.worker_listener
        || bindings.instance_name != mapping.candidate.instance_name
    {
        return Err(Error::new(
            "unsupported_topology",
            "/legacy",
            "Require exactly two distinct filesystem stores, one simple scheduler, two named listeners, one matching instance and no embedded legacy workers.",
        ));
    }
    for (path, value) in [
        ("cas_store", &bindings.cas_store),
        ("ac_store", &bindings.ac_store),
        ("scheduler", &bindings.scheduler),
        ("client_listener", &bindings.client_listener),
        ("worker_listener", &bindings.worker_listener),
        (
            "worker_transfer_listener",
            &bindings.worker_transfer_listener,
        ),
    ] {
        require_text(value, &format!("/mapping/source_bindings/{path}"))?;
    }
    let cas = unique(&legacy.stores, &bindings.cas_store, |store| &store.name)?;
    let sink = unique(&legacy.stores, &bindings.ac_store, |store| &store.name)?;
    let _scheduler = unique(&legacy.schedulers, &bindings.scheduler, |scheduler| {
        &scheduler.name
    })?;
    let client = unique(&legacy.servers, &bindings.client_listener, |server| {
        &server.name
    })?;
    let worker = unique(&legacy.servers, &bindings.worker_listener, |server| {
        &server.name
    })?;
    let transfer = unique(
        &legacy.servers,
        &bindings.worker_transfer_listener,
        |server| &server.name,
    )?;
    validate_routes(client, worker, transfer, bindings)?;
    validate_placement(cas, sink, &mapping.candidate)?;
    for server in &legacy.servers {
        if let Some(tls) = &server.listener.http.tls {
            for value in [&tls.certificate, &tls.key]
                .into_iter()
                .chain(tls.client_ca.iter())
                .chain(tls.client_crl.iter())
            {
                require_absolute_path(value, "/legacy/servers/listener/http/tls")?;
            }
        }
    }
    let declarations = json!(mapping.declarations);
    for (key, value) in declarations.as_object().unwrap() {
        if let Some(value) = value.as_str() {
            require_text(value, &format!("/mapping/declarations/{key}"))?;
        }
    }
    for value in &mapping.declarations.source_worker_credential_files {
        require_absolute_path(
            value,
            "/mapping/declarations/source_worker_credential_files",
        )?;
    }
    if let Some(tls) = &transfer.listener.http.tls {
        let minimum_files = if tls.client_ca.is_some() { 3 } else { 1 };
        let unique_files: std::collections::BTreeSet<_> = mapping
            .declarations
            .source_worker_credential_files
            .iter()
            .collect();
        if unique_files.len() < minimum_files {
            return Err(Error::new(
                "source_transfer_credentials",
                "/mapping/declarations/source_worker_credential_files",
                "Explicit source TLS trust locations are required; mutual TLS also needs distinct client certificate and key locations.",
            ));
        }
    }
    if mapping.declarations.source_cas_namespace == mapping.declarations.source_ac_namespace
        || [
            &mapping.declarations.source_cas_namespace,
            &mapping.declarations.source_ac_namespace,
        ]
        .into_iter()
        .any(|source| {
            source == &mapping.candidate.cas.namespace_id
                || source == &mapping.candidate.temporary_action_cache.namespace_id
        })
    {
        return Err(Error::new(
            "namespace_reuse",
            "/mapping/declarations",
            "Source and proposed CAS/AC namespaces must be distinct for this cold-cutover proposal.",
        ));
    }
    Ok(())
}

fn unique<'a, T>(values: &'a [T], name: &str, key: impl Fn(&T) -> &String) -> Result<&'a T, Error> {
    let mut found = values.iter().filter(|value| key(value) == name);
    let value = found.next().ok_or_else(|| {
        Error::new(
            "missing_reference",
            "/mapping/source_bindings",
            "A named source binding does not resolve.",
        )
    })?;
    if found.next().is_some() {
        return Err(Error::new(
            "ambiguous_reference",
            "/mapping/source_bindings",
            "A named source binding resolves more than once.",
        ));
    }
    Ok(value)
}

fn validate_routes(
    client: &Server,
    worker: &Server,
    transfer: &Server,
    bindings: &Bindings,
) -> Result<(), Error> {
    let client_services = &client.services;
    let worker_services = &worker.services;
    let cas_routes = |routes: &Option<Vec<crate::legacy::CasRoute>>| {
        routes.as_deref().is_some_and(|routes| matches!(routes, [route] if route.instance_name == bindings.instance_name && route.cas_store == bindings.cas_store))
    };
    if client.listener.http.socket_address == worker.listener.http.socket_address
        || client_services.worker_api.is_some()
        || !cas_routes(&client_services.cas) || !cas_routes(&client_services.bytestream)
        || !client_services.ac.as_deref().is_some_and(|routes| matches!(routes, [route] if route.instance_name == bindings.instance_name && route.ac_store == bindings.ac_store && !route.read_only))
        || !client_services.execution.as_deref().is_some_and(|routes| matches!(routes, [route] if route.instance_name == bindings.instance_name && route.cas_store == bindings.cas_store && route.scheduler == bindings.scheduler))
        || !client_services.capabilities.as_deref().is_some_and(|routes| matches!(routes, [route] if route.instance_name == bindings.instance_name && route.remote_execution.scheduler == bindings.scheduler))
        || worker_services.worker_api.as_ref().is_none_or(|route| route.scheduler != bindings.scheduler)
        || worker_services.ac.is_some() || worker_services.execution.is_some() || worker_services.capabilities.is_some()
        || worker_services.cas.as_ref().is_some_and(|_| !cas_routes(&worker_services.cas))
        || worker_services.bytestream.as_ref().is_some_and(|_| !cas_routes(&worker_services.bytestream))
        || !cas_routes(&transfer.services.cas) || !cas_routes(&transfer.services.bytestream)
        || transfer.services.ac.is_some()
    {
        return Err(Error::new("unsupported_listener_boundary", "/legacy/servers", "Source routes must resolve exactly; worker listeners cannot expose AC and the declared transfer listener must provide CAS/ByteStream."));
    }
    Ok(())
}

fn validate_placement(cas: &Store, sink: &Store, candidate: &Manifest) -> Result<(), Error> {
    let source_paths = [
        &cas.filesystem.content_path,
        &cas.filesystem.temp_path,
        &sink.filesystem.content_path,
        &sink.filesystem.temp_path,
    ];
    let destination_roots = [
        &candidate.authority.journal_root,
        &candidate.cas.root,
        &candidate.temporary_action_cache.root,
        &candidate.worker.root,
    ];
    for (index, source) in source_paths.iter().enumerate() {
        require_absolute_path(source, "/legacy/stores/filesystem")?;
        if source_paths[..index]
            .iter()
            .any(|other| overlaps(source, other))
            || destination_roots
                .iter()
                .any(|destination| overlaps(source, destination))
        {
            return Err(Error::new(
                "declared_placement_overlap",
                "/legacy/stores",
                "Source paths must be distinct and disjoint from fresh destination roots; physical alias checks remain required.",
            ));
        }
    }
    Ok(())
}

fn unsupported(value: &Value, path: &str, blockers: &mut Vec<Error>) {
    if blockers.len() >= MAX_DIAGNOSTICS {
        return;
    }
    if path.len() > MAX_DIAGNOSTIC_PATH {
        blockers.push(Error::new(
            "diagnostic_path_bound",
            "/legacy",
            "Diagnostic paths exceed the bound; the incomplete inventory blocks conversion.",
        ));
        return;
    }
    match value {
        Value::Object(fields) => {
            for (key, value) in fields {
                if blockers.len() >= MAX_DIAGNOSTICS {
                    return;
                }
                let child = format!("{path}/{}", key.replace('~', "~0").replace('/', "~1"));
                let code = match key.as_str() {
                    "shard"
                    | "size_partitioning"
                    | "dedup"
                    | "compression"
                    | "fast_slow"
                    | "ref_store"
                    | "verify"
                    | "existence_cache"
                    | "completeness_checking"
                    | "cache_metrics" => Some("unsupported_store_composition"),
                    "redis"
                    | "redis_store"
                    | "memory"
                    | "noop"
                    | "grpc"
                    | "s3"
                    | "gcs"
                    | "azure"
                    | "cloud"
                    | "mongo"
                    | "ontap"
                    | "experimental_cloud_object_store"
                    | "experimental_ontap_s3"
                    | "experimental_mongo" => Some("unsupported_backend_or_authority"),
                    "cache_lookup" | "property_modifier" | "historical_resource" => {
                        Some("unsupported_scheduler_composition")
                    }
                    "upload_action_result"
                    | "upload_ac_results_strategy"
                    | "upload_historical_results_strategy"
                    | "historical_results_store" => Some("legacy_worker_publication"),
                    _ if key.ends_with("message_template") => Some("removed_diagnostic_template"),
                    _ => None,
                };
                if let Some(code) = code {
                    blockers.push(Error::new(code, &child, "This source feature needs an explicit reviewed disposition; the first converter does not map it."));
                }
                unsupported(value, &child, blockers);
            }
        }
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                unsupported(value, &format!("{path}/{index}"), blockers);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
#[path = "conversion_tests.rs"]
mod tests;
