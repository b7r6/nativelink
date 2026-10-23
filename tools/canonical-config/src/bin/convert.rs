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

#[path = "../conversion.rs"]
mod conversion;
#[path = "../legacy.rs"]
mod legacy;

use std::io::{Read, Write};
use std::process::ExitCode;

use nativelink_canonical_config::MAX_INPUT_BYTES;

fn read_input(path: &std::ffi::OsStr) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take((MAX_INPUT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn main() -> ExitCode {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 2 {
        eprintln!("usage: experimental-canonical-convert LEGACY.json5 MAPPING_FACTS.json5");
        return ExitCode::from(1);
    }
    let (Ok(legacy), Ok(mapping)) = (read_input(&args[0]), read_input(&args[1])) else {
        eprintln!("could not read the two input files; no external state was changed");
        return ExitCode::from(1);
    };
    let report = conversion::convert(&legacy, &mapping);
    let mut stdout = std::io::stdout().lock();
    if serde_json::to_writer_pretty(&mut stdout, &report).is_err()
        || stdout.write_all(b"\n").is_err()
    {
        return ExitCode::from(1);
    }
    // 0 means only that a complete experimental proposal was emitted. It does
    // not mean deployable: the report retains every unqualified runtime gate.
    ExitCode::from(if report.mapping_complete { 0 } else { 2 })
}
