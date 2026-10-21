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

//! Export SDK-owned download identity to the local account's HTTP ledger.

use std::sync::Arc;

use async_trait::async_trait;
use azure_core::http::policies::{Policy, PolicyResult};
use azure_core::http::{ClientOptions, Context, Request};
use nativelink_store::azure_blob_store::AzureDownloadAttempt;

/// Only the replay client installs this policy. Ordinary Azure clients retain
/// the typed context locally without adding these fixture-specific headers.
pub(crate) fn attach(options: &mut ClientOptions) {
    options.per_call_policies.push(Arc::new(DownloadIdentity));
}

#[derive(Debug)]
struct DownloadIdentity;

#[async_trait]
impl Policy for DownloadIdentity {
    async fn send(
        &self,
        context: &Context,
        request: &mut Request,
        next: &[Arc<dyn Policy>],
    ) -> PolicyResult {
        // The SDK owns this context and clones it into every partition task.
        // A detached old task therefore keeps its original attempt identity.
        if let Some(attempt) = context.value::<AzureDownloadAttempt>() {
            request.insert_header("x-nl-read-id", attempt.read_id.to_string());
            request.insert_header("x-nl-attempt", attempt.attempt.to_string());
            request.insert_header("x-nl-sent", attempt.sent.to_string());
            request.insert_header("x-nl-writer-broken", attempt.writer_broken.to_string());
        }

        next[0].send(context, request, &next[1..]).await
    }
}
