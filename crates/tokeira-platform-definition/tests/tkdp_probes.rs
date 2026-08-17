//! Capability probes holding the pinned Monty to every behaviour this crate
//! assumes. A pin bump that breaks a probe fails here, before any frontend
//! test confuses the matter.
//
// Feature: tkdp-frontend, Property 14: capability probes.

use monty::MontyRun;
use monty_types::{CompileOptions, MontyObject, PrintWriter, ResourceTracker};

fn run(code: &str) -> Result<MontyObject, String> {
    let runner = MontyRun::new(
        code.to_string(),
        "probe.py",
        Vec::new(),
        CompileOptions::default(),
    )
    .map_err(|error| error.summary())?;
    runner
        .run(
            Vec::new(),
            ResourceTracker::default(),
            PrintWriter::Disabled,
        )
        .map_err(|error| error.summary())
}

#[test]
fn dataclasses_construct_with_defaults_and_keywords() {
    let value = run("
from dataclasses import dataclass


@dataclass
class Probe:
    a: int
    b: str = 'default'


p = Probe(a=1)
q = Probe(2, b='set')
[p.a, p.b, q.a, q.b]
")
    .expect("dataclass construction");
    assert_eq!(format!("{value}"), "[1, 'default', 2, 'set']");
}

#[test]
fn dataclass_field_annotations_are_stored_unevaluated() {
    // `InMemory | Dsql` in annotation position must not evaluate: runtime
    // class unions are unsupported, and the authoring surface leans on this.
    run("
from dataclasses import dataclass


@dataclass
class InMemory:
    pass


@dataclass
class Dsql:
    region: str


@dataclass
class Cfg:
    storage: InMemory | Dsql


Cfg(storage=Dsql(region='eu')).storage.region
")
    .expect("unevaluated union annotation");
}

#[test]
fn type_identity_and_attribute_probes_hold() {
    let value = run("
from dataclasses import dataclass


@dataclass
class A:
    x: int


@dataclass
class B:
    x: int


a = A(x=1)
[type(a) is A, type(a) is B, hasattr(a, 'x'), getattr(a, 'x')]
")
    .expect("identity probes");
    assert_eq!(format!("{value}"), "[True, False, True, 1]");
}

#[test]
fn native_match_is_rejected() {
    let error = run("match 1:\n    case 1:\n        pass\n").expect_err("match must not parse");
    assert!(
        error.to_lowercase().contains("match") || error.to_lowercase().contains("implement"),
        "{error}"
    );
}

#[test]
fn no_cpython_name_mangling() {
    // The facade relies on `__tokeira_internal_*` names resolving uniformly
    // from module level, class bodies, and instance attributes. CPython would
    // mangle these; the pinned Monty must not.
    let value = run("
def __free():
    return 7


class Holder:
    def __init__(self, **kw):
        self.__stash = kw

    def peek(self):
        return [__free(), self.__stash['a']]


h = Holder(a=1)
[h.peek(), hasattr(h, '__stash')]
")
    .expect("mangle-free names");
    assert_eq!(format!("{value}"), "[[7, 1], True]");
}

#[test]
fn kwargs_shells_and_class_attributes_work() {
    let value = run("
class Shell:
    kind_name = 'Shell'

    def __init__(self, **kwargs):
        self.kwargs = kwargs


s = Shell(region='eu', n=2)
[s.kind_name, s.kwargs['region'], s.kwargs['n']]
")
    .expect("kwargs shell");
    assert_eq!(format!("{value}"), "['Shell', 'eu', 2]");
}
