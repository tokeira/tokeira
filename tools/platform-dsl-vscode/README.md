# Tokeira Platform DSL — editor support

Syntax highlighting and basic editing support (comments, bracket matching,
auto-closing, indentation) for tokeira **`.platform`** deployment definition
files — the language compiled by `tokeira-platform-dsl` and authored per platform
crate (see `platforms/compose-dsl/platform/`).

This is a standard VS Code / Kiro language extension: a TextMate grammar plus a
language configuration. There is no language server — highlighting only.

## What it highlights

Grounded in the lexer (`crates/tokeira-platform-dsl/src/token.rs`):

- **Keywords** — `platform`, `use`, `input`, `let`, `module`, `when`, `match`,
  `is`, `namespaces`, `writeback`, `service`, `resource`, `image`, `depends_on`.
- **Declaration names** — the name after `module` / `service` / `resource` /
  `image` / `input` / `let`.
- **Types, kinds, and variants** — any UpperCamelCase name (`String`, `Port`,
  `Path`, `Storage`, `ComposeService`, `DsqlCluster`, `InMemory`, `Dsql`,
  `Managed`, …). The language convention is UpperCamelCase ⇒ type / kind /
  sum-variant constructor, so one rule covers all three.
- **Builtins** — `port`, `bind`, `present_only`, `value_from`, `secret_read`,
  `generated_password` (only in call position).
- **Runtime context** — `ctx` (the closed `RuntimeContext` root).
- **Constants** — `true`, `false`, and the `ro` / `rw` bind-mount modes.
- **Field keys** — `key:` in records, kind fields, and keyword arguments.
- **Operators** — `++`, `=>`, `..`, `.`, `/`, `=` — strings, integers, and
  `//` line comments.

## Install

The extension is unpackaged source. Two ways to load it into Kiro (or VS Code):

### Option A — copy into the user extensions directory

Copy (or symlink) this folder into your editor's extensions directory, then
reload the window:

- VS Code: `~/.vscode/extensions/`
- Kiro: `~/.kiro/extensions/` (Kiro mirrors the VS Code layout; if that path does
  not exist on your install, use Option B, which is path-independent).

```bash
cp -R tools/platform-dsl-vscode ~/.kiro/extensions/tokeira-platform-dsl-0.1.0
```

Then run **Developer: Reload Window** from the command palette.

### Option B — package to a VSIX and install (path-independent)

```bash
npm install -g @vscode/vsce
cd tools/platform-dsl-vscode
vsce package        # produces tokeira-platform-dsl-0.1.0.vsix
```

Then in the command palette: **Extensions: Install from VSIX…** and pick the
generated file.

## Verify

Open any file under `platforms/compose-dsl/platform/` (for example
`compose.platform`). Keywords, kind names, builtins, and strings should be
colored. If not, confirm the file's language mode (bottom-right of the status
bar) reads **Tokeira Platform DSL**; the `.platform` extension is registered by
this extension's `contributes.languages`.

## Scope and limitations

- Highlighting only — no completion, diagnostics, or go-to-definition. The
  authoritative validation is the compiler (`tokeira-platform-dsl`), surfaced by
  `tkr` at plan/apply time.
- The grammar tracks the language by convention, not by the kind library: any
  UpperCamelCase name is treated as a type/kind/variant, so a misspelled kind
  still highlights as a kind (the compiler is what rejects it).
