//! Synthesis of the in-sandbox authoring facade.
//!
//! The facade is derived, never authored: builder classes, one kwargs-shell
//! constructor per kind name from the platform namespaces, and a context class
//! rendered from the platform's serialized typed context. It accumulates
//! plain data inside the sandbox and performs no host calls — the whole
//! structural result crosses to the host once, as the driver's final
//! expression.
//!
//! The rendered source registers with Monty as a genuine module named
//! `tokeira`, so `from tokeira import …` executes as a real import in the
//! root and in every definition part — one module, one set of class
//! identities. The module's public face is the full inventory; each
//! importing file takes exactly the names it imports, by Python's own
//! semantics.
//!
//! Every internal name carries the reserved `__tokeira_internal_` prefix,
//! which preflight forbids in operator code. Monty performs no CPython name
//! mangling (verified by capability probe), so the prefix is usable uniformly
//! for module functions, classes, attributes, and methods.

/// The facade's registered-module name: the `tokeira` the dialect imports
/// from.
pub(crate) const FACADE_MODULE_NAME: &str = "tokeira";

/// File name facade frames carry in Monty tracebacks. The translator renders
/// frames from this file as internal rather than mapping them to operator
/// source.
pub(crate) const FACADE_FILE_NAME: &str = "<tokeira-facade>";

/// Names the facade publishes besides the kind inventory.
pub(crate) const BUILDER_NAMES: &[&str] = &["Context", "Deployment", "create"];

/// The complete importable surface: builders plus the engine kind inventory.
pub(crate) fn facade_names<'a>(kind_names: &'a [&'a str]) -> Vec<&'a str> {
    BUILDER_NAMES
        .iter()
        .copied()
        .chain(kind_names.iter().copied())
        .collect()
}

/// Fixed facade body: match helper and builder classes.
const FACADE_CORE: &str = r#"# --- tokeira facade (synthesized; not operator code) ---
from dataclasses import is_dataclass as __tokeira_internal_is_dataclass


def __tokeira_internal_export(value):
    if __tokeira_internal_is_dataclass(value):
        exported = []
        for name in list(type(value).__dataclass_fields__):
            exported.append([name, __tokeira_internal_export(getattr(value, name))])
        return {
            "__tokeira_internal_struct": type(value).__name__,
            "fields": exported,
        }
    if isinstance(value, list) or isinstance(value, tuple):
        exported = []
        for item in value:
            exported.append(__tokeira_internal_export(item))
        return exported
    if isinstance(value, dict):
        exported = {}
        for key in value:
            exported[key] = __tokeira_internal_export(value[key])
        return exported
    return value


def __tokeira_internal_match(subject, cls, fields):
    if type(subject) is not cls:
        return None
    values = []
    for name in fields:
        if not hasattr(subject, name):
            raise AttributeError(
                "pattern field " + repr(name) + " does not exist on " + cls.__name__
            )
        values.append(getattr(subject, name))
    return values


def __tokeira_internal_create(*fields):
    def annotate(cls):
        return cls
    return annotate


class __tokeira_internal_Output:
    def __init__(self, module, resource, output):
        self.__tokeira_internal_module = module
        self.__tokeira_internal_resource = resource
        self.__tokeira_internal_output = output


class __tokeira_internal_Resource:
    def __init__(self, owner, module, logical_id):
        self.__tokeira_internal_owner = owner
        self.__tokeira_internal_module = module
        self.__tokeira_internal_id = logical_id

    def output(self, name):
        return __tokeira_internal_Output(
            self.__tokeira_internal_module, self.__tokeira_internal_id, name
        )


class __tokeira_internal_Module:
    def __init__(self, owner, name):
        self.__tokeira_internal_owner = owner
        self.__tokeira_internal_name = name

    def resource(self, logical_id, kind, deps=None):
        owner = self.__tokeira_internal_owner
        if not hasattr(kind, "__tokeira_internal_kwargs"):
            raise TypeError(
                "resource " + repr(logical_id) + " expects a kind constructed "
                "from the tokeira facade"
            )
        dep_refs = []
        for dep in [] if deps is None else deps:
            if not hasattr(dep, "__tokeira_internal_id"):
                raise TypeError("resource dependencies must be resource handles")
            if dep.__tokeira_internal_owner is not owner:
                raise ValueError("resource handle belongs to another deployment")
            dep_refs.append(
                {
                    "module": dep.__tokeira_internal_module,
                    "id": dep.__tokeira_internal_id,
                }
            )
        handle = __tokeira_internal_Resource(
            owner, self.__tokeira_internal_name, logical_id
        )
        owner.__tokeira_internal_resources.append(
            {
                "module": self.__tokeira_internal_name,
                "id": logical_id,
                "kind": kind.__tokeira_internal_kind_name,
                "kwargs": __tokeira_internal_export(kind.__tokeira_internal_kwargs),
                "deps": dep_refs,
            }
        )
        return handle


class __tokeira_internal_Deployment:
    def __init__(self, namespaces):
        self.__tokeira_internal_namespaces = list(namespaces)
        self.__tokeira_internal_modules = []
        self.__tokeira_internal_resources = []
        self.__tokeira_internal_writebacks = []

    def module(self, name, deps=None):
        dep_names = []
        for dep in [] if deps is None else deps:
            if not hasattr(dep, "__tokeira_internal_name"):
                raise TypeError("module dependencies must be module handles")
            if dep.__tokeira_internal_owner is not self:
                raise ValueError("module handle belongs to another deployment")
            dep_names.append(dep.__tokeira_internal_name)
        self.__tokeira_internal_modules.append({"name": name, "deps": dep_names})
        return __tokeira_internal_Module(self, name)

    def writeback(self, key, value):
        if hasattr(value, "__tokeira_internal_output"):
            entry = {
                "output": {
                    "module": value.__tokeira_internal_module,
                    "resource": value.__tokeira_internal_resource,
                    "output": value.__tokeira_internal_output,
                }
            }
        else:
            entry = {"literal": value}
        self.__tokeira_internal_writebacks.append({"key": key, "value": entry})

    def __tokeira_internal_envelope(self):
        return {
            "namespaces": self.__tokeira_internal_namespaces,
            "modules": self.__tokeira_internal_modules,
            "resources": self.__tokeira_internal_resources,
            "writebacks": self.__tokeira_internal_writebacks,
        }


"#;

/// Renders the complete facade module source: core, context class, kind
/// shells for every inventory name, and public bindings for the whole
/// importable surface.
pub(crate) fn render(kind_names: &[&str], context: &serde_json::Value) -> String {
    let mut out = String::with_capacity(FACADE_CORE.len() + 1024);
    out.push_str(FACADE_CORE);

    out.push_str("class __tokeira_internal_Context:\n    def __init__(self):\n");
    match context {
        serde_json::Value::Object(fields) if !fields.is_empty() => {
            for (name, value) in fields {
                out.push_str("        self.");
                out.push_str(name);
                out.push_str(" = ");
                out.push_str(&python_literal(value));
                out.push('\n');
            }
        }
        _ => out.push_str("        pass\n"),
    }
    out.push_str("\n\n");

    // Shells for the complete inventory: a definition can only *reference*
    // what it imported, but the set it may import is the whole engine
    // namespace inventory.
    for name in kind_names {
        out.push_str(&format!(
            "class __tokeira_internal_kind_{name}:\n    \
             __tokeira_internal_kind_name = \"{name}\"\n\n    \
             def __init__(self, **kwargs):\n        \
             self.__tokeira_internal_kwargs = kwargs\n\n\n"
        ));
    }

    // The module's public face: every builder and every inventory kind.
    // Aliased imports (`from tokeira import server as s`) are Python's own
    // business at the import site — nothing to render for them here.
    out.push_str("Deployment = __tokeira_internal_Deployment\n");
    out.push_str("Context = __tokeira_internal_Context\n");
    out.push_str("create = __tokeira_internal_create\n");
    for name in kind_names {
        out.push_str(&format!("{name} = __tokeira_internal_kind_{name}\n"));
    }
    out.push_str("# --- end tokeira facade ---\n");
    out
}

/// Renders a serialized context value as a Python literal. JSON and Python
/// literal syntax agree except for the three keyword spellings.
fn python_literal(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "None".to_string(),
        serde_json::Value::Bool(true) => "True".to_string(),
        serde_json::Value::Bool(false) => "False".to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => format!("{s:?}"),
        serde_json::Value::Array(items) => {
            let rendered: Vec<String> = items.iter().map(python_literal).collect();
            format!("[{}]", rendered.join(", "))
        }
        serde_json::Value::Object(fields) => {
            let rendered: Vec<String> = fields
                .iter()
                .map(|(k, v)| format!("{k:?}: {}", python_literal(v)))
                .collect();
            format!("{{{}}}", rendered.join(", "))
        }
    }
}
