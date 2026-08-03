# spike-monty-tkdp

Prototype of a Python deployment-definition frontend (`.tkdp`) executed by
[Pydantic Monty](https://github.com/pydantic/monty), with a deliberately
restricted `match` statement lowered ahead of execution via Ruff.

Standalone by contract: excluded from the tokeira workspace, no tokeira crate
dependencies, mocked deployment surface. The spike exists to answer one
question — *can Monty carry the `.tkdp` authoring experience today, with
`match`-shaped algebraic configuration, without forking Monty?* Answer: yes.

## What it proves

- **Dataclass-based config authoring works.** Monty's in-sandbox `@dataclass`
  (pydantic/monty#626, post-0.0.19, hence the pinned git rev) covers the
  `Compose`/`Tokeirad`/`Observability` shape, including nested dataclasses,
  defaults, and keyword construction. Plain classes with methods also work
  (the upstream README understates this), so the mocked `Deployment`/`Module`
  builder lives entirely in-sandbox.
- **A restricted `match` is fully lowerable without touching Monty.** Ruff
  parses the definition, preflight validates the subset, and each `match`
  is spliced into a flat done-flag `if` chain executed by unmodified Monty.
- **Exact variant identity is expressible in pure Python.** The prelude
  helper probes with `type(subject) is cls`, giving algebraic-sum-type
  semantics (no `isinstance` inheritance surprises) without a Rust host
  primitive — Monty gives sandbox-defined classes stable identity.
- **Diagnostics map back.** Every byte of the generated program is covered by
  a segment map; Monty tracebacks and parse errors render against `.tkdp`
  positions, with prelude/driver frames labelled internal.

## Usage

```bash
cargo run -q -- check  examples/compose.tkdp
cargo run -q -- lower  examples/compose.tkdp          # print generated program
cargo run -q -- run    examples/compose.tkdp
cargo run -q -- run    examples/managed-dsql.tkdp     # guarded case
tkdp run <file> --faithful-exhaustion                 # CPython fall-through
tkdp run <file> --show-generated
```

## The match boundary

Supported (preflight-enforced, `TKDP0xx` diagnostics):

| Pattern | Example | Lowered probe |
|---|---|---|
| wildcard | `case _:` | `if True:` |
| capture | `case x:` | `if True:` + bind |
| literal | `case "dsql":`, `case -1:` | `subject == (<lit>)` |
| singleton | `case None:` / `True` / `False` | `subject is <lit>` |
| keyword class | `case ManagedDsql(region=r):` | `__tokeira_internal_match(subject, Cls, ["region"])` |
| guard | `case P if expr:` | verbatim `if (expr):` after captures bind |

Rejected with spanned diagnostics: positional class args, sequence, mapping,
OR, `as`, star, value (dotted) patterns, complex literals, dotted class
names, nested sub-patterns in keyword position, duplicate fields/captures,
irrefutable case not last, `__tokeira_internal_` identifiers (hygiene), tab
indentation.

## Semantics

CPython-faithful by construction: one subject evaluation; first match wins;
literals compare `==`, singletons `is`; captures bind before the guard and
stay bound when it fails; guards run only after their pattern matched;
`break`/`continue`/`return` in case bodies behave as in real `match`.

Two deliberate deviations, both stated rather than accidental:

- **Strict exhaustion (default).** A match with no matching case raises
  `RuntimeError("<file>:<line>: match fell through: ...")` instead of
  CPython's silent continue — a config definition that matches nothing is a
  bug. `--faithful-exhaustion` restores CPython behaviour.
- **Exact variant identity.** Class patterns match `type(subject) is Cls`,
  not `isinstance`. For a closed configuration sum this is the intended
  algebra; PEP 634 subclass admission is explicitly not wanted.

## Pipeline

```text
.tkdp ── preflight ── lower ── assemble ── execute
         ruff parse   splice    prelude     unmodified Monty,
         subset +     match →   + user +    failures translated
         hygiene      if-chain  driver      through the map
```

- `preflight.rs` — subset validation, hygiene, entrypoint arity, tabs.
- `lower.rs` — splicing emitter. Original text outside `match` is copied
  byte-for-byte (comments and formatting survive); case bodies land at their
  original indentation wherever possible, so their map is the identity.
- `program.rs` — prelude (mocked Tokeira surface + match helper) and the
  driver that calls `config()` / `deployment(cfg, cx)`.
- `source_map.rs` — contiguous segment map; verbatim segments translate
  positions linearly, generated segments point at the motivating construct.
- `runner.rs` — Monty execution, char-column-correct traceback translation.

The generated program is transient. The operator's file is never rewritten;
`lower`/`--show-generated` exist for inspection only.

## Pinning

- `monty` / `monty-types`: git rev `69f8a613e4f42d2f4dc0e659c792569923531e4f`
  (2026-07-31) — first line carrying in-sandbox dataclasses (#626).
- `ruff_*`: `0.0.3`, the exact line Monty pins, so cargo unifies both parsers.
  These crates are explicitly unstable upstream; move them only together with
  the Monty rev.
- `get-size2` is held at `0.10.1` in the lockfile: `0.10.3` implements
  `GetSize` for `compact_str 0.10` while the ruff `0.0.3` line still uses
  `0.9`, which fails the build. Do not `cargo update` it blindly.

## Findings against Monty at the pinned rev

- `Stmt::Match` is `ParseError::not_implemented` (`crates/monty/src/parse.rs`)
  — the natural upstream integration point for Option B (direct IR lowering).
- In-sandbox `@dataclass` supports fields, defaults, `ClassVar` exclusion,
  keyword/positional init, `repr`/`eq`, and zero-field variants.
- Plain classes with methods, `type()`/`isinstance`/`getattr`/`hasattr`,
  closures, f-strings: all present.
- No runtime `X | Y` union on class objects (`types.UnionType`) — the
  prelude's `Storage` alias is a string annotation instead.
- `MontyObject::Dataclass` crosses the host boundary, so config values can be
  returned to Rust structurally, not just as reprs.

## Known caveats (spike scope)

- Re-indented case bodies shift the continuation lines of triple-quoted
  strings (line-based splice, not token-aware). Only affects guarded bodies
  and non-four-space indentation.
- Comments *between* cases and between a case header and its first statement
  are dropped from the transient program (never from the operator's file).
- Monty reports 1-based character columns; translation converts through the
  line content, so non-ASCII lines map correctly.

## Adoption path (if Monty becomes strategic)

1. Keep Option A (this spike's source rewrite) as the product mechanism while
   the subset stays small; the lowered form is canonical and digestable.
2. Option B — lower `StmtMatch` directly into Monty IR at the
   `parse.rs` integration point — removes the second parse and the generated
   text entirely; the preflight subset and semantics carry over unchanged.
3. The prelude's dataclass surface would be generated from the real `tkp`
   model instead of being hand-mocked; the match helper stays, or becomes a
   host function if capture secrecy/resource bounds are wanted.
