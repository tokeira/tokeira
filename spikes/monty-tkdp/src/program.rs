//! Assembly of the transient Monty program: prelude + lowered user source +
//! driver.
//!
//! The prelude is the spike's stand-in for the Tokeira deployment surface —
//! in the real product these types arrive from `tkp`, here they are plain
//! in-sandbox Python (Monty's native `@dataclass` support from
//! pydantic/monty#626 makes that possible). It also carries the one runtime
//! helper the lowering emits calls to, `__tokeira_internal_match`.
//!
//! Variant matching is by exact class identity (`type(subject) is cls`), not
//! `isinstance`: the config surface is a closed algebraic sum, and subclass
//! admission would be an inheritance surprise, not a feature. This is the
//! spike's stand-in for Ian's `subject.variant_id == ManagedDsql` host
//! primitive — expressible entirely in-sandbox because Monty gives classes
//! stable identity.
//!
//! The assembled program is transient: it is never written back over the
//! operator's `.tkdp`, and every byte of it is covered by the source map.

use ruff_text_size::TextSize;

use crate::{
    lower::Lowered,
    preflight::Entrypoints,
    source_map::{Origin, Segment, SourceMap, SourceMapBuilder},
};

/// The fixed prelude prepended to every program.
///
/// Kept dependency-light on Monty features: dataclasses, plain classes with
/// methods, `type`/`getattr`/`hasattr`/`repr`, lists, dicts, string concat.
pub const PRELUDE: &str = r#"# --- tokeira spike prelude (generated; not operator code) ---
from dataclasses import dataclass


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


@dataclass
class InMemory:
    pass


@dataclass
class ManagedDsql:
    region: str


@dataclass
class PreexistingDsql:
    region: str
    endpoint: str
    arn: str


# Monty (at the pinned rev) has no runtime `types.UnionType`, so the union
# alias is a string; it only ever appears in annotation position.
Storage = "InMemory | ManagedDsql | PreexistingDsql"


@dataclass
class DsqlMode:
    name: str


Managed = DsqlMode("managed")


@dataclass
class LocalStateDir:
    pass


@dataclass
class DsqlCluster:
    region: str
    mode: DsqlMode


@dataclass
class AdoptedDsqlCluster:
    region: str
    endpoint: str


class Context:
    def __init__(self):
        self.environment = "spike"


class Module:
    def __init__(self, name):
        self.name = name
        self.resources = []

    def resource(self, name, value):
        self.resources.append((name, value))
        return value


class Deployment:
    def __init__(self, namespaces):
        self.namespaces = namespaces
        self.modules = []

    def module(self, name):
        m = Module(name)
        self.modules.append(m)
        return m


def __tokeira_internal_render(d):
    modules = []
    for m in d.modules:
        resources = []
        for entry in m.resources:
            resources.append({"name": entry[0], "value": repr(entry[1])})
        modules.append({"name": m.name, "resources": resources})
    return {"namespaces": d.namespaces, "modules": modules}


# --- end prelude ---
"#;

/// A fully assembled program ready for Monty, with its map.
#[derive(Debug)]
pub struct Program {
    pub text: String,
    pub map: SourceMap,
    pub entrypoints: Entrypoints,
}

/// Composes prelude + lowered user region + driver into one program.
///
/// The driver invokes the definition's entrypoints and ends with an
/// expression statement, whose value becomes the module result Monty hands
/// back to the host:
/// - `config` + `deployment` → the rendered deployment plan (plain dicts);
/// - `config` alone → the config value itself (crosses the boundary as
///   `MontyObject::Dataclass`).
pub fn assemble(lowered: Lowered, entrypoints: Entrypoints) -> Program {
    let mut text = String::with_capacity(PRELUDE.len() + lowered.text.len() + 128);
    let mut map = SourceMapBuilder::new();

    text.push_str(PRELUDE);
    map.push(TextSize::of(PRELUDE), Origin::Prelude);

    let user_base = map.cursor();
    text.push_str(&lowered.text);
    for Segment { generated, origin } in lowered.segments {
        // Rebase the user-region segments onto the assembled program.
        debug_assert_eq!(user_base + generated.start(), map.cursor());
        map.push(generated.len(), origin);
    }

    // No entrypoints → no driver: the module's own final expression (if any)
    // becomes the result, which keeps bare semantic-probe scripts runnable.
    let driver = match (entrypoints.has_config, entrypoints.has_deployment) {
        (true, true) => "\n__tokeira_internal_render(deployment(config(), Context()))\n",
        (true, false) => "\nconfig()\n",
        (false, _) => "",
    };
    text.push_str(driver);
    map.push(TextSize::of(driver), Origin::Driver);

    Program {
        text,
        map: map.finish(),
        entrypoints,
    }
}
