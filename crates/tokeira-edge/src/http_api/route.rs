//! Descriptor-derived `google.api.http` routes and bounded path matching.

use std::{collections::HashSet, sync::Arc};

use percent_encoding::percent_decode_str;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, ReflectMessage};

use super::HttpApiError;

const WORKFLOW_SERVICE: &str = "temporal.api.workflowservice.v1.WorkflowService";
const OPERATOR_SERVICE: &str = "temporal.api.operatorservice.v1.OperatorService";
const HTTP_EXTENSION: &str = "google.api.http";

/// HTTP verbs supported by `google.api.HttpRule`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum HttpVerb {
    /// GET.
    Get,
    /// POST.
    Post,
    /// PUT.
    Put,
    /// DELETE.
    Delete,
    /// PATCH.
    Patch,
    /// A custom verb declared by the annotation.
    Custom(String),
}

impl HttpVerb {
    /// Parse a request method for route matching.
    pub fn from_method(method: &str) -> Self {
        match method {
            "GET" => Self::Get,
            "POST" => Self::Post,
            "PUT" => Self::Put,
            "DELETE" => Self::Delete,
            "PATCH" => Self::Patch,
            other => Self::Custom(other.to_owned()),
        }
    }

    fn matches(&self, request: &Self) -> bool {
        self == request || matches!(self, Self::Custom(method) if method == "*")
    }
}

/// One path variable captured for a protobuf field selector.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathCapture {
    /// Dotted protobuf field selector from the annotation.
    pub field_path: String,
    /// Once-decoded value captured by the variable template.
    pub value: String,
}

/// One immutable HTTP binding compiled from the public descriptor set.
#[derive(Clone, Debug)]
pub struct HttpApiBinding {
    /// Request verb.
    pub verb: HttpVerb,
    /// Original annotation path.
    pub path_template: String,
    /// Empty means no body, `*` means the complete request, otherwise a field selector.
    pub body: String,
    /// Full gRPC method path used for the in-process dispatch.
    pub grpc_path: String,
    /// Dynamic request descriptor.
    pub input: MessageDescriptor,
    /// Dynamic response descriptor.
    pub output: MessageDescriptor,
    template: CompiledTemplate,
}

/// A successful annotated-route match.
#[derive(Clone, Debug)]
pub struct HttpApiMatch {
    /// Selected immutable binding.
    pub binding: Arc<HttpApiBinding>,
    /// Values to apply after body decoding.
    pub captures: Vec<PathCapture>,
}

/// Immutable catalog of WorkflowService and OperatorService HTTP bindings.
#[derive(Clone, Debug)]
pub struct HttpApiCatalog {
    pool: DescriptorPool,
    bindings: Arc<[Arc<HttpApiBinding>]>,
}

impl HttpApiCatalog {
    /// Build the catalog from the vendored public descriptor set.
    pub fn from_file_descriptor_set(bytes: &[u8]) -> Result<Self, HttpApiError> {
        let pool = DescriptorPool::decode(bytes)
            .map_err(|error| HttpApiError::Descriptor(error.to_string()))?;
        let extension = pool.get_extension_by_name(HTTP_EXTENSION).ok_or_else(|| {
            HttpApiError::Descriptor(format!("missing `{HTTP_EXTENSION}` extension"))
        })?;
        let mut bindings = Vec::new();
        let mut exact = HashSet::new();

        for service_name in [WORKFLOW_SERVICE, OPERATOR_SERVICE] {
            let service = pool.get_service_by_name(service_name).ok_or_else(|| {
                HttpApiError::Descriptor(format!("missing service `{service_name}`"))
            })?;
            for method in service.methods() {
                let options = method.options();
                if !options.has_extension(&extension) {
                    continue;
                }
                if method.is_client_streaming() || method.is_server_streaming() {
                    return Err(HttpApiError::Descriptor(format!(
                        "streaming method `{}` declares an unsupported HTTP binding",
                        method.full_name()
                    )));
                }
                let value = options.get_extension(&extension);
                let rule = value.as_message().ok_or_else(|| {
                    HttpApiError::Descriptor(format!(
                        "HTTP option on `{}` is not a message",
                        method.full_name()
                    ))
                })?;
                let grpc_path = format!("/{service_name}/{}", method.name());
                append_rule(
                    rule,
                    &method.input(),
                    &method.output(),
                    &grpc_path,
                    false,
                    &mut bindings,
                    &mut exact,
                )?;
            }
        }

        // Literal-heavy routes must win over variables independent of descriptor
        // iteration. The pinned API has no equally-specific ambiguity; exact
        // duplicates above are rejected instead of relying on unstable order.
        bindings.sort_by(|left, right| {
            right
                .template
                .specificity()
                .cmp(&left.template.specificity())
                .then_with(|| left.grpc_path.cmp(&right.grpc_path))
        });

        Ok(Self {
            pool,
            bindings: bindings.into(),
        })
    }

    /// Build from Tokeira's pinned public descriptors.
    pub fn pinned() -> Result<Self, HttpApiError> {
        Self::from_file_descriptor_set(tokeira_proto::public::FILE_DESCRIPTOR_SET)
    }

    /// Shared descriptor pool used for dynamic request/response and `Any` values.
    pub fn pool(&self) -> &DescriptorPool {
        &self.pool
    }

    /// All registered primary and additional bindings.
    pub fn bindings(&self) -> &[Arc<HttpApiBinding>] {
        &self.bindings
    }

    /// Match one request method/path after legacy once-only path decoding.
    pub fn recognize(
        &self,
        method: &str,
        path: &str,
    ) -> Result<Option<HttpApiMatch>, HttpApiError> {
        let verb = HttpVerb::from_method(method);
        let decoded = percent_decode_str(path)
            .decode_utf8()
            .map_err(|error| HttpApiError::InvalidRequest(error.to_string()))?;
        let components = decoded
            .strip_prefix('/')
            .ok_or_else(|| HttpApiError::InvalidRequest("path must begin with `/`".to_owned()))?
            .split('/')
            .collect::<Vec<_>>();
        for binding in self
            .bindings
            .iter()
            .filter(|binding| binding.verb.matches(&verb))
        {
            if let Some(captures) = binding.template.matches(&components) {
                return Ok(Some(HttpApiMatch {
                    binding: binding.clone(),
                    captures,
                }));
            }
        }
        Ok(None)
    }

    /// Whether this path matches an annotated binding under another verb.
    pub fn path_exists_for_other_verb(
        &self,
        method: &str,
        path: &str,
    ) -> Result<bool, HttpApiError> {
        let request_verb = HttpVerb::from_method(method);
        let decoded = percent_decode_str(path)
            .decode_utf8()
            .map_err(|error| HttpApiError::InvalidRequest(error.to_string()))?;
        let Some(decoded) = decoded.strip_prefix('/') else {
            return Ok(false);
        };
        let components = decoded.split('/').collect::<Vec<_>>();
        Ok(self.bindings.iter().any(|binding| {
            !binding.verb.matches(&request_verb) && binding.template.matches(&components).is_some()
        }))
    }
}

fn append_rule(
    rule: &DynamicMessage,
    input: &MessageDescriptor,
    output: &MessageDescriptor,
    grpc_path: &str,
    nested: bool,
    bindings: &mut Vec<Arc<HttpApiBinding>>,
    exact: &mut HashSet<(HttpVerb, String)>,
) -> Result<(), HttpApiError> {
    let (verb, path) = rule_pattern(rule)?;
    let body = string_field(rule, "body")?;
    let template = CompiledTemplate::parse(&path)?;
    validate_field_selector(input, &body, true)?;
    for capture in &template.variables {
        validate_field_selector(input, capture, false)?;
    }
    if !exact.insert((verb.clone(), path.clone())) {
        return Err(HttpApiError::Descriptor(format!(
            "duplicate HTTP binding `{verb:?} {path}` for `{grpc_path}`"
        )));
    }
    bindings.push(Arc::new(HttpApiBinding {
        verb,
        path_template: path,
        body,
        grpc_path: grpc_path.to_owned(),
        input: input.clone(),
        output: output.clone(),
        template,
    }));

    let additional = rule
        .get_field_by_name("additional_bindings")
        .and_then(|value| value.as_list().map(ToOwned::to_owned))
        .unwrap_or_default();
    if nested && !additional.is_empty() {
        return Err(HttpApiError::Descriptor(format!(
            "nested additional binding on `{grpc_path}`"
        )));
    }
    for value in additional {
        let child = value.as_message().ok_or_else(|| {
            HttpApiError::Descriptor(format!(
                "additional binding on `{grpc_path}` is not a message"
            ))
        })?;
        append_rule(child, input, output, grpc_path, true, bindings, exact)?;
    }
    Ok(())
}

fn rule_pattern(rule: &DynamicMessage) -> Result<(HttpVerb, String), HttpApiError> {
    for (name, verb) in [
        ("get", HttpVerb::Get),
        ("post", HttpVerb::Post),
        ("put", HttpVerb::Put),
        ("delete", HttpVerb::Delete),
        ("patch", HttpVerb::Patch),
    ] {
        let field = rule.descriptor().get_field_by_name(name).ok_or_else(|| {
            HttpApiError::Descriptor(format!("HttpRule missing `{name}` descriptor"))
        })?;
        if rule.has_field(&field) {
            let value = rule.get_field(&field);
            let path = value.as_str().ok_or_else(|| {
                HttpApiError::Descriptor(format!("HttpRule `{name}` is not a string"))
            })?;
            return Ok((verb, path.to_owned()));
        }
    }
    let custom = rule
        .descriptor()
        .get_field_by_name("custom")
        .ok_or_else(|| {
            HttpApiError::Descriptor("HttpRule missing `custom` descriptor".to_owned())
        })?;
    if rule.has_field(&custom) {
        let value = rule.get_field(&custom);
        let pattern = value.as_message().ok_or_else(|| {
            HttpApiError::Descriptor("HttpRule `custom` is not a message".to_owned())
        })?;
        let kind = string_field(pattern, "kind")?;
        let path = string_field(pattern, "path")?;
        if kind.is_empty() {
            return Err(HttpApiError::Descriptor(
                "HttpRule custom method is empty".to_owned(),
            ));
        }
        return Ok((HttpVerb::Custom(kind), path));
    }
    Err(HttpApiError::Descriptor(
        "HttpRule has no request pattern".to_owned(),
    ))
}

fn string_field(message: &DynamicMessage, name: &str) -> Result<String, HttpApiError> {
    message
        .get_field_by_name(name)
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .ok_or_else(|| HttpApiError::Descriptor(format!("missing string field `{name}`")))
}

fn validate_field_selector(
    root: &MessageDescriptor,
    selector: &str,
    body: bool,
) -> Result<(), HttpApiError> {
    if selector.is_empty() || (body && selector == "*") {
        return Ok(());
    }
    let mut message = root.clone();
    let parts = selector.split('.').collect::<Vec<_>>();
    for (index, part) in parts.iter().enumerate() {
        let field = message.get_field_by_name(part).ok_or_else(|| {
            HttpApiError::Descriptor(format!(
                "field selector `{selector}` is absent from `{}`",
                message.full_name()
            ))
        })?;
        if index + 1 < parts.len() {
            message = field.kind().as_message().cloned().ok_or_else(|| {
                HttpApiError::Descriptor(format!(
                    "non-message component `{part}` in field selector `{selector}`"
                ))
            })?;
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct CompiledTemplate {
    segments: Vec<TemplateSegment>,
    variables: Vec<String>,
    custom_verb: Option<String>,
}

impl CompiledTemplate {
    fn parse(path: &str) -> Result<Self, HttpApiError> {
        let raw = path.strip_prefix('/').ok_or_else(|| {
            HttpApiError::Descriptor(format!("HTTP path `{path}` must begin with `/`"))
        })?;
        let (raw, custom_verb) = split_custom_verb(raw, path)?;
        let mut segments = Vec::new();
        let mut variables = Vec::new();
        for component in split_template_components(raw, path)? {
            if component.starts_with('{') && component.ends_with('}') {
                let inner = &component[1..component.len() - 1];
                let (field, pattern) = inner.split_once('=').unwrap_or((inner, "*"));
                if field.is_empty() {
                    return Err(HttpApiError::Descriptor(format!(
                        "empty path variable in `{path}`"
                    )));
                }
                variables.push(field.to_owned());
                let pattern = parse_variable_pattern(pattern, path)?;
                segments.push(TemplateSegment::Variable {
                    field: field.to_owned(),
                    pattern,
                });
            } else if component.contains('{') || component.contains('}') {
                return Err(HttpApiError::Descriptor(format!(
                    "malformed path component `{component}` in `{path}`"
                )));
            } else {
                segments.push(TemplateSegment::Literal(component.to_owned()));
            }
        }
        Ok(Self {
            segments,
            variables,
            custom_verb,
        })
    }

    fn specificity(&self) -> (usize, usize) {
        let literals = self.segments.iter().map(TemplateSegment::literals).sum();
        let width = self.segments.iter().map(TemplateSegment::width).sum();
        (literals, width)
    }

    fn matches(&self, components: &[&str]) -> Option<Vec<PathCapture>> {
        if let Some(verb) = &self.custom_verb {
            let (last, prefix) = components.split_last()?;
            let suffix = format!(":{verb}");
            let last = last.strip_suffix(&suffix)?;
            if last.is_empty() {
                return None;
            }
            let mut stripped = prefix.to_vec();
            stripped.push(last);
            match_segments(&self.segments, &stripped, Vec::new())
        } else {
            match_segments(&self.segments, components, Vec::new())
        }
    }

    #[cfg(test)]
    fn example_components(&self) -> Vec<String> {
        let mut components = Vec::new();
        for segment in &self.segments {
            match segment {
                TemplateSegment::Literal(value) => components.push(value.clone()),
                TemplateSegment::Variable { pattern, .. } => {
                    for part in pattern {
                        components.push(match part {
                            VariablePattern::Single | VariablePattern::Multi => "sample".to_owned(),
                            VariablePattern::Literal(value) => value.clone(),
                        });
                    }
                }
            }
        }
        if let (Some(verb), Some(last)) = (&self.custom_verb, components.last_mut()) {
            last.push(':');
            last.push_str(verb);
        }
        components
    }
}

fn split_custom_verb<'a>(
    raw: &'a str,
    path: &str,
) -> Result<(&'a str, Option<String>), HttpApiError> {
    let mut depth = 0_u32;
    let mut separator = None;
    for (index, character) in raw.char_indices() {
        match character {
            '{' => depth = depth.saturating_add(1),
            '}' => {
                depth = depth.checked_sub(1).ok_or_else(|| {
                    HttpApiError::Descriptor(format!("unbalanced braces in `{path}`"))
                })?;
            }
            ':' if depth == 0 => separator = Some(index),
            _ => {}
        }
    }
    if depth != 0 {
        return Err(HttpApiError::Descriptor(format!(
            "unbalanced braces in `{path}`"
        )));
    }
    let Some(separator) = separator else {
        return Ok((raw, None));
    };
    let verb = &raw[separator + 1..];
    if verb.is_empty() || verb.contains('/') || verb.contains(['{', '}']) {
        return Err(HttpApiError::Descriptor(format!(
            "invalid custom verb in `{path}`"
        )));
    }
    Ok((&raw[..separator], Some(verb.to_owned())))
}

fn split_template_components<'a>(raw: &'a str, path: &str) -> Result<Vec<&'a str>, HttpApiError> {
    let mut depth = 0_u32;
    let mut start = 0;
    let mut components = Vec::new();
    for (index, character) in raw.char_indices() {
        match character {
            '{' => depth = depth.saturating_add(1),
            '}' => {
                depth = depth.checked_sub(1).ok_or_else(|| {
                    HttpApiError::Descriptor(format!("unbalanced braces in `{path}`"))
                })?;
            }
            '/' if depth == 0 => {
                components.push(&raw[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(HttpApiError::Descriptor(format!(
            "unbalanced braces in `{path}`"
        )));
    }
    components.push(&raw[start..]);
    Ok(components)
}

fn parse_variable_pattern(pattern: &str, path: &str) -> Result<Vec<VariablePattern>, HttpApiError> {
    if pattern.is_empty() {
        return Err(HttpApiError::Descriptor(format!(
            "empty variable pattern in `{path}`"
        )));
    }
    pattern
        .split('/')
        .map(|component| match component {
            "*" => Ok(VariablePattern::Single),
            "**" => Ok(VariablePattern::Multi),
            literal if !literal.is_empty() && !literal.contains('*') => {
                Ok(VariablePattern::Literal(literal.to_owned()))
            }
            other => Err(HttpApiError::Descriptor(format!(
                "unsupported variable pattern `{other}` in `{path}`"
            ))),
        })
        .collect()
}

#[derive(Clone, Debug)]
enum TemplateSegment {
    Literal(String),
    Variable {
        field: String,
        pattern: Vec<VariablePattern>,
    },
}

impl TemplateSegment {
    fn literals(&self) -> usize {
        match self {
            Self::Literal(_) => 1,
            Self::Variable { pattern, .. } => pattern
                .iter()
                .filter(|part| matches!(part, VariablePattern::Literal(_)))
                .count(),
        }
    }

    fn width(&self) -> usize {
        match self {
            Self::Literal(_) => 1,
            Self::Variable { pattern, .. } => pattern.len(),
        }
    }
}

#[derive(Clone, Debug)]
enum VariablePattern {
    Single,
    Multi,
    Literal(String),
}

fn match_segments(
    patterns: &[TemplateSegment],
    components: &[&str],
    captures: Vec<PathCapture>,
) -> Option<Vec<PathCapture>> {
    let Some((pattern, remaining_patterns)) = patterns.split_first() else {
        return components.is_empty().then_some(captures);
    };
    match pattern {
        TemplateSegment::Literal(expected) => {
            let (actual, remaining) = components.split_first()?;
            (actual == expected)
                .then(|| match_segments(remaining_patterns, remaining, captures))
                .flatten()
        }
        TemplateSegment::Variable { field, pattern } => variable_matches(pattern, components)
            .into_iter()
            .rev()
            .find_map(|length| {
                let mut next = captures.clone();
                next.push(PathCapture {
                    field_path: field.clone(),
                    value: components[..length].join("/"),
                });
                match_segments(remaining_patterns, &components[length..], next)
            }),
    }
}

fn variable_matches(pattern: &[VariablePattern], components: &[&str]) -> Vec<usize> {
    fn walk(
        pattern: &[VariablePattern],
        components: &[&str],
        consumed: usize,
        out: &mut Vec<usize>,
    ) {
        let Some((head, tail)) = pattern.split_first() else {
            out.push(consumed);
            return;
        };
        match head {
            VariablePattern::Single => {
                if !components.is_empty() {
                    walk(tail, &components[1..], consumed + 1, out);
                }
            }
            VariablePattern::Literal(expected) => {
                if components.first().is_some_and(|actual| *actual == expected) {
                    walk(tail, &components[1..], consumed + 1, out);
                }
            }
            VariablePattern::Multi => {
                for length in 0..=components.len() {
                    walk(tail, &components[length..], consumed + length, out);
                }
            }
        }
    }

    let mut matches = Vec::new();
    walk(pattern, components, 0, &mut matches);
    matches
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use proptest::prelude::*;

    use super::*;

    #[test]
    fn pinned_catalog_discovers_primary_and_alias_routes() {
        let catalog = HttpApiCatalog::pinned().expect("pinned descriptors");
        assert!(catalog.bindings().len() > 100);
        let system = catalog
            .recognize("GET", "/system-info")
            .expect("valid path")
            .expect("route");
        assert_eq!(
            system.binding.grpc_path,
            "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo"
        );
        let start = catalog
            .recognize("POST", "/namespaces/ns/workflows/wf")
            .expect("valid path")
            .expect("route");
        assert_eq!(
            start.captures,
            vec![
                PathCapture {
                    field_path: "namespace".to_owned(),
                    value: "ns".to_owned(),
                },
                PathCapture {
                    field_path: "workflow_id".to_owned(),
                    value: "wf".to_owned(),
                }
            ]
        );
        assert!(
            catalog
                .path_exists_for_other_verb("GET", "/namespaces/ns/workflows/wf")
                .expect("valid path")
        );
    }

    #[test]
    fn matching_decodes_before_segment_selection_once() {
        let template = CompiledTemplate::parse("/items/{name}").expect("template");
        assert_eq!(
            template.matches(&["items", "a b"]),
            Some(vec![PathCapture {
                field_path: "name".to_owned(),
                value: "a b".to_owned(),
            }])
        );
    }

    #[test]
    fn nested_variable_patterns_and_custom_verbs_capture_the_resource_name() {
        let template =
            CompiledTemplate::parse("/v1/{name=publishers/*/books/**}:archive").expect("template");
        assert_eq!(
            template.matches(&["v1", "publishers", "acme", "books", "one", "two:archive"]),
            Some(vec![PathCapture {
                field_path: "name".to_owned(),
                value: "publishers/acme/books/one/two".to_owned(),
            }])
        );
        assert!(
            template
                .matches(&["v1", "publishers", "acme", "books", "one:delete"])
                .is_none()
        );
    }

    #[test]
    fn pinned_openapi_documents_cover_every_descriptor_binding_path() {
        let catalog = HttpApiCatalog::pinned().expect("catalog");
        let v2: serde_json::Value =
            serde_json::from_slice(tokeira_proto::public::OPENAPI_V2_JSON).expect("OpenAPI v2");
        let v2_paths = v2["paths"]
            .as_object()
            .expect("v2 paths")
            .keys()
            .map(|path| route_shape(path))
            .collect::<HashSet<_>>();
        let v3_text = std::str::from_utf8(tokeira_proto::public::OPENAPI_V3_YAML).expect("UTF-8");
        let v3_paths = v3_text
            .lines()
            .filter_map(|line| {
                line.strip_prefix("  /")
                    .and_then(|path| path.strip_suffix(':'))
                    .map(|path| route_shape(&format!("/{path}")))
                    .or_else(|| {
                        line.strip_prefix("  ? /")
                            .map(|path| route_shape(&format!("/{path}")))
                    })
            })
            .collect::<HashSet<_>>();

        for binding in catalog.bindings() {
            let shape = route_shape(&binding.path_template);
            assert!(
                v2_paths.contains(&shape),
                "OpenAPI v2 omits {}",
                binding.path_template
            );
            assert!(
                v3_paths.contains(&shape),
                "OpenAPI v3 omits {}",
                binding.path_template
            );
        }
    }

    fn route_shape(path: &str) -> String {
        let mut output = String::new();
        let mut remaining = path;
        while let Some(open) = remaining.find('{') {
            output.push_str(&remaining[..open]);
            let tail = &remaining[open + 1..];
            let Some(close) = tail.find('}') else {
                output.push_str(tail);
                return output;
            };
            let inner = &tail[..close];
            output.push('{');
            if let Some((_, pattern)) = inner.split_once('=') {
                output.push('=');
                output.push_str(pattern);
            }
            output.push('}');
            remaining = &tail[close + 1..];
        }
        output.push_str(remaining);
        output
    }

    proptest! {
        // Feature: edge-http-api-gateway, Property 1
        #[test]
        fn every_descriptor_binding_accepts_a_witness(seed in any::<usize>()) {
            let catalog = HttpApiCatalog::pinned().expect("catalog");
            let binding = &catalog.bindings()[seed % catalog.bindings().len()];
            let components = binding.template.example_components();
            let components = components.iter().map(String::as_str).collect::<Vec<_>>();
            prop_assert!(binding.template.matches(&components).is_some());
        }

        // Feature: edge-http-api-gateway, Property 2
        #[test]
        fn custom_verb_capture_never_accepts_another_verb(name in "[A-Za-z0-9_-]{1,32}") {
            let template = CompiledTemplate::parse("/items/{name}:archive").expect("template");
            let archive = format!("{name}:archive");
            let remove = format!("{name}:remove");
            prop_assert_eq!(
                template.matches(&["items", &archive]),
                Some(vec![PathCapture {
                    field_path: "name".to_owned(),
                    value: name,
                }])
            );
            prop_assert!(template.matches(&["items", &remove]).is_none());
        }

        // Feature: edge-http-api-gateway, Property 10
        #[test]
        fn annotated_workflow_paths_are_never_claimed_as_nexus_or_grpc(
            namespace in "[a-z0-9_-]{1,24}",
            workflow_id in "[a-z0-9_-]{1,24}",
        ) {
            let catalog = HttpApiCatalog::pinned().expect("catalog");
            let path = format!("/namespaces/{namespace}/workflows/{workflow_id}");
            prop_assert!(catalog.recognize("POST", &path).expect("route").is_some());
            prop_assert!(!crate::nexus_http::NexusHttpHandler::recognizes_path(&path));
            prop_assert!(catalog
                .recognize(
                    "POST",
                    "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo",
                )
                .expect("route")
                .is_none());
        }
    }
}
