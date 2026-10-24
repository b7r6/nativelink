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

use core::fmt;

use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

pub const MAX_INPUT_BYTES: usize = 1024 * 1024;
pub const MAX_DEPTH: usize = 32;
pub const MAX_ITEMS: usize = 16_384;
pub const MAX_STRING_BYTES: usize = 4096;

/// Diagnostics deliberately omit input values and raw serde error messages.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Error {
    pub code: &'static str,
    pub path: String,
    pub message: &'static str,
}

impl Error {
    pub fn new(code: &'static str, path: impl Into<String>, message: &'static str) -> Self {
        Self {
            code,
            path: path.into(),
            message,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at {}: {}", self.code, self.path, self.message)
    }
}
impl std::error::Error for Error {}

/// Read bounded JSON5 data without resolving references or consulting the host.
/// This neutral parser does not recognize any legacy construction vocabulary.
pub fn parse_data(bytes: &[u8]) -> Result<Value, Error> {
    if bytes.len() > MAX_INPUT_BYTES || !bounded_depth(bytes) {
        return Err(Error::new(
            "input_bound",
            "",
            "Input exceeds the byte or nesting bound.",
        ));
    }
    let text = core::str::from_utf8(bytes)
        .map_err(|_| Error::new("invalid_utf8", "", "Input must be UTF-8."))?;
    let UniqueValue(value) = serde_json5::from_str(text).map_err(|_| {
        Error::new(
            "invalid_json5",
            "",
            "Invalid JSON5, duplicate keys, or nonfinite numbers.",
        )
    })?;
    if !value.is_object() {
        return Err(Error::new(
            "invalid_root",
            "",
            "Input root must be an object.",
        ));
    }
    let mut pending = vec![&value];
    let mut items = 0;
    while let Some(item) = pending.pop() {
        items += 1;
        if items > MAX_ITEMS {
            return Err(Error::new(
                "item_bound",
                "",
                "Input exceeds the item bound.",
            ));
        }
        match item {
            Value::String(text) => validate_string(text)?,
            Value::Array(values) => pending.extend(values),
            Value::Object(values) => {
                for (key, value) in values {
                    validate_string(key)?;
                    pending.push(value);
                }
            }
            _ => {}
        }
    }
    Ok(value)
}

fn validate_string(value: &str) -> Result<(), Error> {
    if value.len() > MAX_STRING_BYTES {
        return Err(Error::new(
            "string_bound",
            "",
            "Input exceeds the string bound.",
        ));
    }
    if value.contains(['$', '~']) {
        return Err(Error::new(
            "unresolved_interpolation",
            "",
            "Supply literal values; environment and home references are never expanded.",
        ));
    }
    Ok(())
}

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
