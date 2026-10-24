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

//! Offline inventory only. This example never emits a deployable configuration.

#[path = "migration_inventory/mod.rs"]
mod inventory;

use std::io::{Read, Write};
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = std::env::args_os().skip(1);
    let Some(path) = args.next() else {
        eprintln!("usage: experimental_migration_inventory LEGACY_CONFIG.json5");
        return ExitCode::from(1);
    };
    if args.next().is_some() {
        eprintln!("expected exactly one input file; output is a JSON report on stdout");
        return ExitCode::from(1);
    }
    let mut bytes = Vec::new();
    let result = std::fs::File::open(path).and_then(|file| {
        file.take((inventory::MAX_INPUT_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
    });
    if result.is_err() {
        eprintln!("could not read input; no configuration or external service was changed");
        return ExitCode::from(1);
    }
    let report = inventory::inspect(&bytes);
    let mut stdout = std::io::stdout().lock();
    if serde_json::to_writer_pretty(&mut stdout, &report).is_err()
        || stdout.write_all(b"\n").is_err()
    {
        return ExitCode::from(1);
    }
    // A successful inventory is still a refusal to authorize deployment.
    ExitCode::from(2)
}
