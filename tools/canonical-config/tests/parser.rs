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

use nativelink_canonical_config::{
    MAX_DEPTH, MAX_INPUT_BYTES, MAX_ITEMS, MAX_STRING_BYTES, parse_canonical, parse_data,
};
use serde_json::{Value, json};

const CANONICAL: &[u8] = include_bytes!("../fixtures/canonical.json");

fn value() -> Value {
    serde_json::from_slice(CANONICAL).unwrap()
}
fn bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}

#[test]
fn canonical_roundtrip_and_declared_roles() {
    let manifest = parse_canonical(CANONICAL).unwrap();
    assert_eq!(
        manifest.worker.transfer.address,
        manifest.listeners.worker.bind_address
    );
    assert_ne!(
        manifest.cas.namespace_id,
        manifest.temporary_action_cache.namespace_id
    );
    assert_eq!(
        parse_canonical(&serde_json::to_vec(&manifest).unwrap()).unwrap(),
        manifest
    );
}

#[test]
fn legacy_key_presence_always_refuses_even_with_bad_credential_locations() {
    for key in ["stores", "schedulers"] {
        for contents in [
            Value::Null,
            json!([]),
            json!({}),
            json!(false),
            json!(0),
            json!("ignored"),
        ] {
            let mut candidate = value();
            candidate[key] = contents;
            candidate["listeners"]["client"]["key_file"] = json!("/nonexistent/no-credential-read");
            let error = parse_canonical(&bytes(&candidate)).unwrap_err();
            assert_eq!(error.code, "legacy_key");
            assert_eq!(error.path, format!("/{key}"));
        }
    }
    assert_eq!(
        parse_canonical(br#"{ "\u0073tores": null }"#)
            .unwrap_err()
            .code,
        "legacy_key"
    );
    assert_eq!(
        parse_canonical(br"{ stores: null, stores: [] }")
            .unwrap_err()
            .code,
        "invalid_json5"
    );
}

#[test]
fn json5_key_equivalence_cannot_bypass_legacy_or_duplicate_refusal() {
    let canonical = core::str::from_utf8(CANONICAL).unwrap();
    for (key, unicode, hexadecimal) in [
        (
            "stores",
            r"\u0073\u0074\u006f\u0072\u0065\u0073",
            r"'\x73tores'",
        ),
        (
            "schedulers",
            r"\u0073\u0063\u0068\u0065\u0064\u0075\u006c\u0065\u0072\u0073",
            r"'\x73chedulers'",
        ),
    ] {
        let mut spellings = vec![
            format!("\"{unicode}\""),
            unicode.to_owned(),
            hexadecimal.to_owned(),
        ];
        for terminator in ["\n", "\r", "\r\n", "\u{2028}", "\u{2029}"] {
            spellings.push(format!("'{}\\{terminator}{}'", &key[..2], &key[2..]));
        }
        for spelling in spellings {
            let input = format!("{{{spelling}:null,{}", &canonical[1..]);
            assert_eq!(
                parse_canonical(input.as_bytes()).unwrap_err().code,
                "legacy_key"
            );
            let duplicate = format!("{{{key}:null,{spelling}:[]}}");
            assert_eq!(
                parse_data(duplicate.as_bytes()).unwrap_err().code,
                "invalid_json5"
            );
        }
    }
}

#[test]
fn identity_numbers_preserve_integer_precision_and_refuse_float_coercion() {
    for node_id in [(1_u64 << 53) - 1, (1_u64 << 53) + 1, (1_u64 << 63) - 1] {
        let mut candidate = value();
        candidate["authority"]["node_id"] = json!(node_id);
        assert_eq!(
            parse_canonical(&bytes(&candidate))
                .unwrap()
                .authority
                .node_id,
            node_id
        );
    }
    let canonical = serde_json::to_string(&value()).unwrap();
    for literal in [
        "1.0",
        "1e0",
        "1.0000000000000001",
        "9007199254740993.0",
        "-1",
        "9223372036854775808",
        "18446744073709551615",
    ] {
        let candidate = canonical.replace("\"node_id\":1", &format!("\"node_id\":{literal}"));
        assert!(parse_canonical(candidate.as_bytes()).is_err(), "{literal}");
    }
}

#[test]
fn comments_and_string_values_do_not_become_legacy_keys() {
    let mut candidate = value();
    candidate["instance_name"] = json!("stores schedulers");
    let text = format!(
        "// stores: []\n/* schedulers: null */{}",
        String::from_utf8(bytes(&candidate)).unwrap()
    );
    assert!(parse_canonical(text.as_bytes()).is_ok());
}

#[test]
fn duplicate_keys_unknown_fields_and_invalid_numbers_refuse() {
    for input in [
        r"{a:{key:1,key:2}}",
        r#"{"a":NaN}"#,
        r#"{"a":Infinity}"#,
        r#"{"a":-Infinity}"#,
        "[]",
    ] {
        assert!(parse_data(input.as_bytes()).is_err(), "{input}");
    }
    for pointer in [
        "/algorithm",
        "/authority/scheduler",
        "/cas/provider",
        "/listeners/worker/services",
        "/worker/ac_store",
    ] {
        let mut candidate = value();
        let (parent, key) = pointer.rsplit_once('/').unwrap();
        candidate.pointer_mut(parent).unwrap()[key] = json!("unsupported");
        assert_eq!(
            parse_canonical(&bytes(&candidate)).unwrap_err().code,
            "canonical_schema"
        );
    }
}

#[test]
fn input_bounds_cover_bytes_depth_strings_items_and_utf8() {
    assert_eq!(
        parse_data(&vec![b' '; MAX_INPUT_BYTES + 1])
            .unwrap_err()
            .code,
        "input_bound"
    );
    for prefix in [
        "",
        "// comment\n",
        "// comment\u{2028}",
        "// comment\u{2029}",
    ] {
        let text = format!(
            "{{a:{prefix}{}0{}}}",
            "[".repeat(MAX_DEPTH),
            "]".repeat(MAX_DEPTH)
        );
        assert_eq!(parse_data(text.as_bytes()).unwrap_err().code, "input_bound");
    }
    assert!(parse_data(br#"{a: '[{', b: "}]", c: 'quote\'[' /* [[[ */}"#).is_ok());
    assert_eq!(
        parse_data(&bytes(&json!({"a":"x".repeat(MAX_STRING_BYTES+1)})))
            .unwrap_err()
            .code,
        "string_bound"
    );
    assert_eq!(
        parse_data(&bytes(&json!({"a":vec![0;MAX_ITEMS]})))
            .unwrap_err()
            .code,
        "item_bound"
    );
    assert_eq!(parse_data(&[0xff]).unwrap_err().code, "invalid_utf8");
}

#[test]
fn declarations_cannot_select_algorithms_or_expand_host_values() {
    for text in [r#"{a:"$TOKEN"}"#, r#"{a:"~/secret"}"#] {
        let error = parse_data(text.as_bytes()).unwrap_err();
        assert_eq!(error.code, "unresolved_interpolation");
        assert!(!error.to_string().contains("TOKEN"));
    }
    let mut candidate = value();
    candidate["format"] = json!("legacy_or_canonical");
    assert_eq!(
        parse_canonical(&bytes(&candidate)).unwrap_err().code,
        "canonical_schema"
    );
}

#[test]
fn declared_aliases_and_wrong_worker_routes_refuse_without_filesystem_access() {
    let cases = [
        (
            "/cas/root",
            json!("/proposed/ac/child"),
            "overlapping_roots",
        ),
        (
            "/cas/root",
            json!("/proposed/../cas"),
            "literal_absolute_path",
        ),
        (
            "/temporary_action_cache/namespace_id",
            json!("a".repeat(64)),
            "shared_namespace",
        ),
        (
            "/authority/cluster_epoch",
            json!(0),
            "zero_identity_or_capacity",
        ),
        (
            "/listeners/worker/client_ca_file",
            json!("/credentials/client-clients-ca.pem"),
            "shared_listener_boundary",
        ),
        (
            "/listeners/worker/bind_address",
            json!("0.0.0.0:5101"),
            "local_listener_required",
        ),
        (
            "/worker/transfer/address",
            json!("127.0.0.1:5100"),
            "worker_transfer_route",
        ),
    ];
    for (path, replacement, code) in cases {
        let mut candidate = value();
        *candidate.pointer_mut(path).unwrap() = replacement;
        assert_eq!(
            parse_canonical(&bytes(&candidate)).unwrap_err().code,
            code,
            "{path}"
        );
    }
}
