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

//! Pure, experimental canonical configuration vocabulary. No runtime consumes
//! this schema yet. Acceptance establishes syntax and declared facts only.
//! Legacy parsing and conversion live exclusively in the offline binary.

mod parse;
mod schema;

pub use parse::{Error, MAX_DEPTH, MAX_INPUT_BYTES, MAX_ITEMS, MAX_STRING_BYTES, parse_data};
pub use schema::*;

/// Reject legacy top-level key presence before decoding any canonical fields.
/// This function performs no I/O, environment expansion, or construction.
pub fn parse_canonical(bytes: &[u8]) -> Result<Manifest, Error> {
    let value = parse_data(bytes)?;
    for key in ["stores", "schedulers"] {
        if value.get(key).is_some() {
            return Err(Error::new(
                "legacy_key",
                format!("/{key}"),
                "Legacy top-level keys are forbidden, including empty and null values.",
            ));
        }
    }
    let manifest: Manifest = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "canonical_schema",
            "",
            "Missing, unknown, or invalid canonical fields.",
        )
    })?;
    manifest.validate()?;
    Ok(manifest)
}
