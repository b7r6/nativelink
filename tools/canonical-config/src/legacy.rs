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

//! Deliberately narrow legacy data vocabulary, compiled only into the offline
//! binary. Every accepted source field appears in the comparison report.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Legacy {
    pub stores: Vec<Store>,
    pub schedulers: Vec<Scheduler>,
    pub servers: Vec<Server>,
    pub workers: Option<Vec<Value>>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Store {
    pub name: String,
    pub filesystem: Filesystem,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Filesystem {
    pub content_path: String,
    pub temp_path: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Scheduler {
    pub name: String,
    pub simple: Empty,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Empty {}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Server {
    pub name: String,
    pub listener: Listener,
    pub services: Services,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Listener {
    pub http: Http,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Http {
    pub socket_address: SocketAddr,
    pub tls: Option<Tls>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Tls {
    #[serde(rename = "cert_file")]
    pub certificate: String,
    #[serde(rename = "key_file")]
    pub key: String,
    #[serde(rename = "client_ca_file")]
    pub client_ca: Option<String>,
    #[serde(rename = "client_crl_file")]
    pub client_crl: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Services {
    pub cas: Option<Vec<CasRoute>>,
    pub ac: Option<Vec<AcRoute>>,
    pub bytestream: Option<Vec<CasRoute>>,
    pub execution: Option<Vec<ExecutionRoute>>,
    pub capabilities: Option<Vec<CapabilitiesRoute>>,
    pub worker_api: Option<SchedulerRoute>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CasRoute {
    #[serde(default)]
    pub instance_name: String,
    pub cas_store: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcRoute {
    #[serde(default)]
    pub instance_name: String,
    pub ac_store: String,
    #[serde(default)]
    pub read_only: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecutionRoute {
    #[serde(default)]
    pub instance_name: String,
    pub cas_store: String,
    pub scheduler: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CapabilitiesRoute {
    #[serde(default)]
    pub instance_name: String,
    pub remote_execution: SchedulerRoute,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SchedulerRoute {
    pub scheduler: String,
}
