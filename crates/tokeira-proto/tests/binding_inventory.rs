//! The Binding Inventory: every service, method, message, enum, and field the
//! checked-in descriptor sets declare, compared against a fixture captured
//! before the code generator changed. A regeneration may change Rust output
//! freely; it may not change what the wire surface declares. Running with
//! `WIRE_PARITY_CAPTURE=1` rewrites the fixture.

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
    let by_text = |value: &Value| value.to_string();
    services.sort_by_key(by_text);
    fields.sort_by_key(by_text);
    enums.sort_by_key(by_text);
    message_names.sort();
    message_names.dedup();
    json!({
        "files": set.file.iter().map(FileDescriptorProto::name).collect::<Vec<_>>(),
        "services": services,
        "messages": message_names,
        "enums": enums,
        "fields": fields,
    })
}

// Feature: tonic-0-14-grpc-stack, Property 8: Binding Inventory parity
#[test]
fn binding_inventory_matches_the_fixture() {
    let public = FileDescriptorSet::decode(tokeira_proto::public::FILE_DESCRIPTOR_SET)
        .expect("public descriptor set decodes");
    let internal = FileDescriptorSet::decode(tokeira_proto::internal::FILE_DESCRIPTOR_SET)
        .expect("internal descriptor set decodes");
    let mut observed = json!({
        "public": inventory(&public),
        "internal": inventory(&internal),
    });
    if let Value::Object(map) = &mut observed {
        for value in map.values_mut() {
            if let Value::Object(inner) = value
                && let Some(Value::Array(files)) = inner.get_mut("files")
            {
                files.sort_by_key(|file| file.to_string());
            }
        }
    }
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
    assert!(
        expected == observed,
        "binding inventory changed; expected\n{}\nobserved\n{rendered}",
        serde_json::to_string_pretty(&expected).expect("fixture renders")
    );
}
