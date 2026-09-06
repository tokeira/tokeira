//! The Binding Inventory: every service, method, message, enum, and field the
//! checked-in descriptor sets declare, compared against a fixture captured
//! before the code generator changed. A regeneration may change Rust output
//! freely; it may not change what the wire surface declares. Running with
//! `WIRE_PARITY_CAPTURE=1` rewrites the fixture.
//!
//! The comparison sets the `google.protobuf` package aside on both sides. The
//! vendored tree carries no copy of `descriptor.proto`, so each compiler
//! contributes its own bundled definition of the descriptor language (the
//! previous generator's copy declared `FieldOptions.FeatureSupport`, the
//! current one declares `FileOptions.php_generic_services`). Neither is a
//! surface Tokeira declares; the Temporal and Tokeira packages reference that
//! file only for options. The fixture itself is left as captured.
//!
//! Both sides are ordered by [`canonical`] before the comparison rather than
//! by `Value::to_string()`. The text of an entry depends on which map
//! `serde_json` was built with: a workspace-wide build unifies the
//! `preserve_order` feature into this binary through the AWS HTTP client and
//! keys render in insertion order, while a package-scoped build renders them
//! alphabetically, and the two texts sort the same entries differently. The
//! canonical rendering also makes the fixture's own file order irrelevant.

use std::{fs, path::PathBuf};

use prost::Message;
use prost_types::{DescriptorProto, FileDescriptorProto, FileDescriptorSet};
use serde_json::{Value, json};

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/binding-inventory.json")
}

fn messages(
    prefix: &str,
    message: &DescriptorProto,
    out: &mut Vec<Value>,
    names: &mut Vec<String>,
) {
    let full = format!("{prefix}.{}", message.name());
    names.push(full.clone());
    for field in &message.field {
        out.push(json!({
            "message": full,
            "field": field.name(),
            "number": field.number(),
            "type": field.r#type().as_str_name(),
            "type_name": field.type_name(),
            "label": field.label().as_str_name(),
            "oneof": field.oneof_index,
            "proto3_optional": field.proto3_optional.unwrap_or(false),
        }));
    }
    for nested in &message.nested_type {
        messages(&full, nested, out, names);
    }
}

fn inventory(set: &FileDescriptorSet) -> Value {
    let mut services = Vec::new();
    let mut fields = Vec::new();
    let mut message_names = Vec::new();
    let mut enums = Vec::new();
    for file in &set.file {
        let package = file.package();
        for service in &file.service {
            for method in &service.method {
                services.push(json!({
                    "service": format!("{package}.{}", service.name()),
                    "method": method.name(),
                    "input": method.input_type(),
                    "output": method.output_type(),
                    "client_streaming": method.client_streaming(),
                    "server_streaming": method.server_streaming(),
                }));
            }
        }
        for message in &file.message_type {
            messages(package, message, &mut fields, &mut message_names);
        }
        for enumeration in &file.enum_type {
            let values: Vec<Value> = enumeration
                .value
                .iter()
                .map(|value| json!({ "name": value.name(), "number": value.number() }))
                .collect();
            enums.push(
                json!({ "enum": format!("{package}.{}", enumeration.name()), "values": values }),
            );
        }
    }
    services.sort_by_key(canonical);
    fields.sort_by_key(canonical);
    enums.sort_by_key(canonical);
    message_names.sort();
    message_names.dedup();
    let mut files: Vec<&str> = set.file.iter().map(FileDescriptorProto::name).collect();
    files.sort_unstable();
    json!({
        "files": files,
        "services": services,
        "messages": message_names,
        "enums": enums,
        "fields": fields,
    })
}

/// One entry rendered with its keys sorted, whatever map `serde_json` was
/// built with (see the module documentation).
fn canonical(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            let entries: Vec<String> = keys
                .into_iter()
                .map(|key| format!("{key:?}:{}", canonical(&map[key.as_str()])))
                .collect();
            format!("{{{}}}", entries.join(","))
        }
        Value::Array(items) => {
            let items: Vec<String> = items.iter().map(canonical).collect();
            format!("[{}]", items.join(","))
        }
        scalar => scalar.to_string(),
    }
}

/// Whether an inventory entry belongs to the compiler-bundled descriptor
/// language rather than to a declared surface.
fn is_compiler_package(entry: &Value) -> bool {
    let text = match entry {
        Value::String(name) => name.as_str(),
        Value::Object(map) => map
            .get("message")
            .or_else(|| map.get("service"))
            .or_else(|| map.get("enum"))
            .and_then(Value::as_str)
            .unwrap_or(""),
        _ => "",
    };
    text.starts_with("google.protobuf.") || text.starts_with("google/protobuf/")
}

/// The comparable form of an inventory: compiler-bundled entries removed and
/// every list in canonical order.
fn normalize(mut inventory: Value) -> Value {
    if let Value::Object(surfaces) = &mut inventory {
        for surface in surfaces.values_mut() {
            if let Value::Object(lists) = surface {
                for list in lists.values_mut() {
                    if let Value::Array(entries) = list {
                        entries.retain(|entry| !is_compiler_package(entry));
                        entries.sort_by_key(canonical);
                    }
                }
            }
        }
    }
    inventory
}

// Feature: tonic-0-14-grpc-stack, Property 8: Binding Inventory parity
#[test]
fn binding_inventory_matches_the_fixture() {
    let public = FileDescriptorSet::decode(tokeira_proto::public::FILE_DESCRIPTOR_SET)
        .expect("public descriptor set decodes");
    let internal = FileDescriptorSet::decode(tokeira_proto::internal::FILE_DESCRIPTOR_SET)
        .expect("internal descriptor set decodes");
    let observed = json!({
        "public": inventory(&public),
        "internal": inventory(&internal),
    });
    let rendered = format!(
        "{}\n",
        serde_json::to_string_pretty(&observed).expect("inventory renders")
    );
    let path = fixture_path();
    if std::env::var_os("WIRE_PARITY_CAPTURE").is_some() {
        fs::create_dir_all(path.parent().expect("fixture has a parent")).expect("fixture dir");
        fs::write(&path, rendered).expect("write the inventory fixture");
        return;
    }
    let expected = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {} (capture it first): {error}", path.display()));
    let expected: Value = serde_json::from_str(&expected).expect("fixture parses");
    let expected = normalize(expected);
    let observed = normalize(observed);
    assert!(
        expected == observed,
        "binding inventory changed; expected\n{}\nobserved\n{}",
        serde_json::to_string_pretty(&expected).expect("fixture renders"),
        serde_json::to_string_pretty(&observed).expect("inventory renders")
    );
}
