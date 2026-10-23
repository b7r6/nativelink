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

use std::net::SocketAddr;
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};

use crate::Error;

/// A format identity, not a choice between runtime implementations.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Format {
    #[serde(rename = "experimental_singleton_filesystem_v1")]
    ExperimentalSingletonFilesystemV1,
}

/// Proposed facts for one local singleton and one identity-capable worker.
///
/// No runtime consumes this experimental format. Physical preflight, clock,
/// admission/recovery and cutover qualification remain separate requirements.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub format: Format,
    pub instance_name: String,
    pub authority: Authority,
    pub cas: FilesystemNamespace,
    pub temporary_action_cache: FilesystemNamespace,
    pub listeners: Listeners,
    pub worker: Worker,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Authority {
    pub node_id: u64,
    pub cluster_epoch: u64,
    pub namespace: u64,
    /// The trusted client principal for this single-principal profile.
    pub principal: String,
    pub journal_root: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FilesystemNamespace {
    /// Explicit physical namespace identity; not the logical history binding.
    pub namespace_id: String,
    /// Owns the fixed content/temp child layout, subject to runtime preflight.
    pub root: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Listeners {
    pub client: Listener,
    pub worker: Listener,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Listener {
    pub bind_address: SocketAddr,
    pub certificate_file: String,
    pub key_file: String,
    pub client_ca_file: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Worker {
    pub worker_id: u64,
    /// Declared new process incarnation; reuse is not authorized by parsing.
    pub incarnation_id: u64,
    pub execution_configuration_id: String,
    pub root: String,
    pub capacity: Resources,
    /// Both authority commands and CAS transfers use the fixed worker role.
    pub transfer: Transfer,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub slots: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Transfer {
    pub address: SocketAddr,
    pub server_name: String,
    pub server_ca_file: String,
    pub certificate_file: String,
    pub key_file: String,
}

impl Manifest {
    /// Pure declaration checks. This cannot detect symlinks, bind mounts,
    /// shared CA contents, stale requests, or a falsely claimed namespace.
    pub fn validate(&self) -> Result<(), Error> {
        if self.authority.node_id == 0
            || self.authority.cluster_epoch == 0
            || self.authority.namespace == 0
            || self.worker.worker_id == 0
            || self.worker.incarnation_id == 0
            || self.worker.capacity.cpu_millis == 0
            || self.worker.capacity.memory_bytes == 0
            || self.worker.capacity.slots == 0
        {
            return Err(Error::new(
                "zero_identity_or_capacity",
                "",
                "Identities and resource facts must be nonzero.",
            ));
        }
        for (path, value) in [
            ("/authority/principal", self.authority.principal.as_str()),
            (
                "/worker/execution_configuration_id",
                self.worker.execution_configuration_id.as_str(),
            ),
            (
                "/worker/transfer/server_name",
                self.worker.transfer.server_name.as_str(),
            ),
        ] {
            require_text(value, path)?;
        }
        let roots = [
            (
                "/authority/journal_root",
                self.authority.journal_root.as_str(),
            ),
            ("/cas/root", self.cas.root.as_str()),
            (
                "/temporary_action_cache/root",
                self.temporary_action_cache.root.as_str(),
            ),
            ("/worker/root", self.worker.root.as_str()),
        ];
        for (index, (path, root)) in roots.iter().enumerate() {
            require_absolute_path(root, path)?;
            if roots[..index]
                .iter()
                .any(|(_, other)| overlaps(root, other))
            {
                return Err(Error::new(
                    "overlapping_roots",
                    *path,
                    "Declared role roots must be disjoint; physical alias checks remain required.",
                ));
            }
        }
        for (path, namespace) in [
            ("/cas/namespace_id", &self.cas.namespace_id),
            (
                "/temporary_action_cache/namespace_id",
                &self.temporary_action_cache.namespace_id,
            ),
        ] {
            if namespace.len() != 64
                || !namespace
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(Error::new(
                    "namespace_identity",
                    path,
                    "Namespace identity must be 64 lowercase hexadecimal characters.",
                ));
            }
        }
        if self.cas.namespace_id == self.temporary_action_cache.namespace_id {
            return Err(Error::new(
                "shared_namespace",
                "/temporary_action_cache/namespace_id",
                "CAS and temporary AC sink need distinct namespace identities.",
            ));
        }
        let listeners = &self.listeners;
        for (name, listener) in [("client", &listeners.client), ("worker", &listeners.worker)] {
            if !listener.bind_address.ip().is_loopback() || listener.bind_address.port() == 0 {
                return Err(Error::new(
                    "local_listener_required",
                    format!("/listeners/{name}/bind_address"),
                    "This first profile requires explicit nonzero loopback listener addresses.",
                ));
            }
            for (field, value) in [
                ("certificate_file", &listener.certificate_file),
                ("key_file", &listener.key_file),
                ("client_ca_file", &listener.client_ca_file),
            ] {
                require_absolute_path(value, &format!("/listeners/{name}/{field}"))?;
            }
        }
        if listeners.client.bind_address == listeners.worker.bind_address
            || listeners.client.client_ca_file == listeners.worker.client_ca_file
        {
            return Err(Error::new(
                "shared_listener_boundary",
                "/listeners",
                "Client and worker roles require distinct declared sockets and client trust references; contents remain unverified.",
            ));
        }
        if self.worker.transfer.address != listeners.worker.bind_address {
            return Err(Error::new(
                "worker_transfer_route",
                "/worker/transfer/address",
                "Worker transfers must use the fixed worker listener.",
            ));
        }
        for (field, value) in [
            ("server_ca_file", &self.worker.transfer.server_ca_file),
            ("certificate_file", &self.worker.transfer.certificate_file),
            ("key_file", &self.worker.transfer.key_file),
        ] {
            require_absolute_path(value, &format!("/worker/transfer/{field}"))?;
        }
        Ok(())
    }
}

pub fn require_text(value: &str, path: &str) -> Result<(), Error> {
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        return Err(Error::new(
            "missing_literal_fact",
            path,
            "Supply a nonempty literal declaration without control characters.",
        ));
    }
    Ok(())
}

pub fn require_absolute_path(value: &str, path: &str) -> Result<(), Error> {
    require_text(value, path)?;
    let parsed = Path::new(value);
    if !parsed.is_absolute()
        || value == "/"
        || value.ends_with('/')
        || value.contains("//")
        || value.split('/').any(|part| matches!(part, "." | ".."))
        || parsed
            .components()
            .any(|part| matches!(part, Component::ParentDir | Component::CurDir))
    {
        return Err(Error::new(
            "literal_absolute_path",
            path,
            "Supply a normalized absolute path; this does not establish physical identity.",
        ));
    }
    Ok(())
}

pub fn overlaps(left: &str, right: &str) -> bool {
    Path::new(left).starts_with(right) || Path::new(right).starts_with(left)
}
