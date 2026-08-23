# tokeira-tkdp

The Python deployment-definition frontend (`.tkdp`), executed by
[Pydantic Monty](https://github.com/pydantic/monty), with a deliberately
restricted `match` statement lowered ahead of execution via Ruff. The logical
peer of the `.tkd` frontend: same typed context in, same completed structural
graph and typed configuration out — a `.tkdp` deployment differs from a
`.tkd` deployment in exactly one recorded fact, its definition format.

Grew out of the standalone `spikes/monty-tkdp` prototype (retired once this
crate landed), which answered the motivating question — *can Monty carry the
`.tkdp` authoring experience, with `match`-shaped algebraic configuration,
without forking Monty?* — with yes. The operator-facing authoring guide lives
in [deployment-definitions.md](../../docs/provisioning/deployment-definitions.md);
this file carries the engineering contract.

## The match boundary

Supported (preflight-enforced, `TKDP0xx` diagnostics):

| Pattern | Example | Lowered probe |
|---|---|---|
| wildcard | `case _:` | `if True:` |
| capture | `case x:` | `if True:` + bind |
| literal | `case "dsql":`, `case -1:` | `subject == (<lit>)` |
| singleton | `case None:` / `True` / `False` | `subject is <lit>` |
| keyword class | `case Dsql(region=r):` | `__tokeira_internal_match(subject, Cls, ["region"])` |
| guard | `case P if expr:` | verbatim `if (expr):` after captures bind |

Rejected with spanned diagnostics: positional class args, sequence, mapping,
OR, `as`, star, value (dotted) patterns, complex literals, dotted class
names, nested sub-patterns in keyword position, duplicate fields/captures,
irrefutable case not last, `__tokeira_internal_` identifiers (hygiene), tab
indentation, and `tokeira` imports outside the facade contract.

## Semantics

CPython-faithful by construction: one subject evaluation; first match wins;
literals compare `==`, singletons `is`; captures bind before the guard and
stay bound when it fails; guards run only after their pattern matched;
`break`/`continue`/`return` in case bodies behave as in real `match`.

Two deliberate deviations, both stated rather than accidental:

- **Strict exhaustion.** A match with no matching case raises
  `RuntimeError("<file>:<line>: match fell through: ...")` instead of
  CPython's silent continue — a config definition that matches nothing is a
  defect, not a no-op. There is no faithful-fall-through mode.
- **Exact variant identity.** Class patterns match `type(subject) is Cls`,
  not `isinstance`. For a closed configuration sum this is the intended
  algebra; PEP 634 subclass admission is explicitly not wanted.

## Pipeline

```text
.tkdp ── preflight ── lower ── assemble ─── execute ──── convert
         ruff parse   splice    facade +    unmodified   envelope →
         subset +     match →   lowered +   Monty,       typed config
         hygiene +    if-chain, driver      failures     + structural
         imports      blank                 mapped       graph
                      imports
```

- `preflight.rs` — subset validation, hygiene, entrypoint arity, tabs, the
  facade import contract, and call-site collection for range correlation.
- `lower.rs` — splicing emitter. Original text outside `match` is copied
  byte-for-byte (comments and formatting survive); the validated facade
  imports are blanked to equal-width `#` padding so every later offset is
  unchanged; case bodies land at their original indentation wherever
  possible, so their map is the identity.
- `facade.rs` — the synthesized `tokeira` surface: kind shells for the whole
  engine inventory, the `Context` class from the serialized platform context,
  the `Deployment`/module/resource builders, the match helper, and the
  structural exporter.
- `program.rs` — assembly of facade + lowered source + driver into one
  transient program.
- `source_map.rs` — contiguous segment map; verbatim segments translate
  positions linearly, generated segments point at the motivating construct.
- `runner.rs` — Monty execution, char-column-correct traceback translation,
  captured print output attached to failures.
- `convert.rs` — envelope → `FrontendOutput`: tagged-struct decoding, kind
  defaults merge, and call-site range correlation for decode failures.
- `diagnostics.rs` — caret-underlined rendering of preflight findings.

The generated program is transient. The operator's file is never rewritten,
the transient text is never persisted, and evaluation always reassembles.
`TkdpFrontend::transient_program` assembles without executing — the
inspection seam (carried from the spike CLI's `lower` / `--show-generated`)
for a future operator verb beside `definition check`; no operator command
surfaces it today.

## Pinning

- `monty` / `monty-types`: git rev `69f8a613e4f42d2f4dc0e659c792569923531e4f`
  (2026-07-31) — first line carrying in-sandbox dataclasses
  (pydantic/monty#626, post-`0.0.19`). Pinned in the workspace manifest.
- `ruff_*`: `0.0.3`, the exact line Monty pins, so cargo unifies both
  parsers. These crates are explicitly unstable upstream; move them only
  together with the Monty rev.
- `get-size2` is held at `0.10.1` in the lockfile: `0.10.3` implements
  `GetSize` for `compact_str 0.10` while the ruff `0.0.3` line still uses
  `0.9`, which fails the build. Restore after an accidental bump with
  `cargo update -p get-size2 --precise 0.10.1`.

The capability probes in `tests/probes.rs` hold the pin to every Monty
behaviour this crate assumes, so a rev bump that breaks an assumption fails
loudly rather than miscompiling definitions.

## Findings against Monty at the pinned rev

- `Stmt::Match` is `ParseError::not_implemented` (`crates/monty/src/parse.rs`)
  — the natural upstream integration point for direct IR lowering (see the
  adoption path below).
- In-sandbox `@dataclass` supports fields, defaults, keyword construction,
  and zero-field variants. Field annotations are stored unevaluated, which is
  what lets variant unions be spelled `A | B` in annotation position.
- Plain classes with methods, `type()`/`getattr`/`hasattr`, closures,
  `**kwargs`: all present. There is no CPython name mangling, so
  double-underscore internals behave uniformly.
- No runtime `X | Y` union on class objects (`types.UnionType`) — union
  spellings work only in (unevaluated) annotations.
- `dataclasses` exports `dataclass` and `is_dataclass` but not `fields`;
  field names come from `type(v).__dataclass_fields__`.
- **In-sandbox dataclass instances cross the host boundary as
  `MontyObject::Repr`**, not structurally — the `MontyObject::Dataclass`
  variant is host-supplied only. That is why the facade carries an in-sandbox
  structural exporter and the driver returns an exported envelope instead of
  raw objects.

## Known caveats

- Re-indented case bodies shift the continuation lines of triple-quoted
  strings (line-based splice, not token-aware). Only affects guarded bodies
  and non-four-space indentation.
- Comments *between* cases and between a case header and its first statement
  are dropped from the transient program (never from the operator's file).
- Monty reports 1-based character columns; translation converts through the
  line content, so non-ASCII lines map correctly.

## Adoption path

1. The source rewrite in this crate is the product mechanism while the
   subset stays small; the lowered form is canonical and deterministic.
2. Lowering `StmtMatch` directly into Monty IR at the `parse.rs` integration
   point would remove the second parse and the generated text entirely; the
   preflight subset and semantics carry over unchanged.
3. The match helper stays in-sandbox Python, or becomes a host function if
   capture secrecy or resource bounds are ever wanted.
