//! Multi-file `use` import assembly with containment (Requirement 13).
//!
//! This is the **only** place in the compiler that touches the filesystem, and
//! it does so under a hard boundary: it reads exactly the `.platform` files of
//! one deployment definition, reachable from a root file by relative,
//! downward-only `use` imports, and nothing else. It sits at the very front of
//! the pipeline (assemble → lex → parse → resolve → type-check → evaluate),
//! turning a deployment directory into a deterministic, path-sorted
//! [`SourceSet`] the rest of the (pure) compiler consumes.
//!
//! The containment rules are the security boundary of the whole DSL
//! (design §"Import containment is the boundary primitive"): a program can only
//! ever influence its own deployment-local files, never the host filesystem.
//! Every rule below is **fail-closed** — a violation is a blocking diagnostic
//! and no source set is produced:
//!
//! - **Relative, downward only** — an import literal with an absolute prefix or
//!   any `..` component is rejected before any path is touched (Req 13.2).
//! - **Symlink-escape proof** — the target is canonicalized (resolving
//!   symlinks) and its real path MUST be strictly within the canonicalized
//!   deployment root; a symlink pointing outside is rejected (Req 13.2).
//! - **Depth ≤ 1** — a file more than one directory below the deployment root
//!   is rejected (Req 13.3).
//! - **Acyclic** — a `use` edge back onto a file already on the active import
//!   path is a cycle diagnostic (Req 13.4).
//! - **Bounded** — file count, per-file and total bytes, import-graph depth, and
//!   bracket-nesting depth are capped so adversarial input cannot exhaust
//!   resources (Req 12.4).
//!
//! Composition is **deterministic regardless of read order**: files are sorted
//! by their deployment-root-relative path and concatenated into one virtual
//! source, so spans stay meaningful and the resulting program is identical no
//! matter how the OS enumerated directories (Req 13.4 / Property 19). The
//! content digest is taken over the sorted `(relative_path, sha256)` set and is
//! the artifact the provisioner records, retains, and verifies (Req 13.6).
//!
//! Span handling note: each file's tokens are shifted by the file's base offset
//! within the composed virtual source *before* parsing, so every downstream
//! diagnostic (lex, parse, resolve, type-check, evaluate) carries a span that
//! indexes [`SourceSet::composed_source`]. This keeps multi-file diagnostics
//! locatable without threading a file id through every `Span`.

use std::{
    collections::BTreeMap,
    path::{Component, Path, PathBuf},
};

use logos::Logos;
use sha2::{Digest, Sha256};

use crate::{
    ast::{Item, Program, Spanned},
    diagnostic::Diag,
    parser::parse,
    token::{Span, Token},
};

/// Resource caps that bound compilation against adversarial input (Req 12.4).
///
/// Totality already bars non-termination; these caps bar *resource exhaustion*
/// (a definition that is finite but enormous, or pathologically deep). The
/// defaults are generous for any real deployment definition and small enough
/// that a hostile input fails fast with a diagnostic rather than consuming
/// unbounded memory. They are not a security feature against a trusted operator
/// — they are a guard rail with a clear error.
#[derive(Debug, Clone, Copy)]
pub struct Bounds {
    /// Maximum number of files in one deployment definition.
    pub max_files: usize,
    /// Maximum bytes in any single `.platform` file.
    pub max_file_bytes: usize,
    /// Maximum total bytes across the whole definition.
    pub max_total_bytes: usize,
    /// Maximum depth of the `use` import graph (chained imports).
    pub max_import_depth: usize,
    /// Maximum bracket/brace/paren nesting depth within any single file.
    pub max_nesting_depth: usize,
}

impl Default for Bounds {
    fn default() -> Self {
        Self {
            max_files: 64,
            max_file_bytes: 256 * 1024,
            max_total_bytes: 1024 * 1024,
            max_import_depth: 8,
            max_nesting_depth: 64,
        }
    }
}

/// One definition file in a [`SourceSet`], with its place in the virtual source.
#[derive(Debug, Clone)]
pub struct SourceFile {
    /// Deployment-root-relative path, always `/`-separated and canonical, so it
    /// is stable across platforms and read orders (the digest key, Req 13.6).
    pub rel_path: String,
    /// The file's verbatim contents.
    pub content: String,
    /// Hex SHA-256 of `content`; one half of the `(rel_path, sha256)` digest
    /// pair (Req 13.6).
    pub sha256: String,
    /// Byte offset where `content` begins in [`SourceSet::composed_source`].
    /// Tokens from this file are shifted by this amount so diagnostics locate
    /// into the composed source.
    pub base_offset: usize,
}

/// The assembled, path-sorted set of definition files plus its content digest.
///
/// This is the deterministic artifact downstream phases compile and the
/// provisioner retains. Internals are private to preserve the invariant that
/// every [`SourceFile::base_offset`] is consistent with [`Self::composed_source`]
/// and that `files` is path-sorted.
#[derive(Debug, Clone)]
pub struct SourceSet {
    files: Vec<SourceFile>,
    composed: String,
    digest: String,
}

impl SourceSet {
    /// The path-sorted definition files.
    pub fn files(&self) -> &[SourceFile] {
        &self.files
    }

    /// The virtual source: every file's content concatenated in path-sorted
    /// order. Diagnostic spans from any phase index into this string.
    pub fn composed_source(&self) -> &str {
        &self.composed
    }

    /// The content digest over the sorted `(relative_path, sha256)` set
    /// (Req 13.6). Identical inputs in any read order yield this same value.
    pub fn digest(&self) -> &str {
        &self.digest
    }
}

/// A failed assembly: the blocking diagnostics plus the partial composed source
/// they point into, so the caller can render them with locations.
///
/// Assembly returns this (rather than a bare `Vec<Diag>`) because rendering a
/// located diagnostic needs the source the span indexes; on failure the caller
/// has no `SourceSet` to read it from, so it travels with the error.
#[derive(Debug, Clone)]
pub struct AssemblyError {
    /// The blocking diagnostics (at least one error).
    pub diagnostics: Vec<Diag>,
    /// The virtual source the diagnostics' spans index into.
    pub composed_source: String,
}

/// Assemble the deployment definition rooted at `deployment_root/root_definition`
/// with default [`Bounds`].
pub fn assemble(deployment_root: &Path, root_definition: &str) -> Result<SourceSet, AssemblyError> {
    assemble_with_bounds(deployment_root, root_definition, &Bounds::default())
}

/// Assemble with explicit resource bounds.
///
/// Reads the root file, follows every `use` edge under the containment rules,
/// and returns the path-sorted [`SourceSet`]. Any containment, cycle, or bound
/// violation is fail-closed: the function returns [`AssemblyError`] and produces
/// no source set (the partial virtual source is carried for rendering only).
pub fn assemble_with_bounds(
    deployment_root: &Path,
    root_definition: &str,
    bounds: &Bounds,
) -> Result<SourceSet, AssemblyError> {
    // Canonicalize the root up front: every contained path is checked against
    // this real path, so a symlinked deployment dir still has a single, stable
    // boundary. A missing/unreadable root is reported with a no-file span.
    let canonical_root = match deployment_root.canonicalize() {
        Ok(root) => root,
        Err(err) => {
            return Err(AssemblyError {
                diagnostics: vec![Diag::error(
                    0..0,
                    format!(
                        "deployment root `{}` is not a readable directory: {err}",
                        deployment_root.display()
                    ),
                )],
                composed_source: String::new(),
            });
        }
    };

    let root_abs = canonical_root.join(root_definition);
    let (root_rel, root_canon) = match canonicalize_within(&canonical_root, &root_abs) {
        Ok(pair) => pair,
        Err(_) => {
            return Err(AssemblyError {
                diagnostics: vec![
                    Diag::error(
                        0..0,
                        format!("root definition `{root_definition}` not found in deployment root"),
                    )
                    .with_hint(
                        "a deployment definition must have its root file at the deployment root",
                    ),
                ],
                composed_source: String::new(),
            });
        }
    };

    let mut assembler = Assembler {
        canonical_root,
        bounds,
        files: BTreeMap::new(),
        pending: Vec::new(),
        total_bytes: 0,
        stack: Vec::new(),
    };
    assembler.walk(root_rel, root_canon, 0);
    assembler.finish()
}

/// Compose an assembled [`SourceSet`] into a single untyped [`Program`].
///
/// Each file is lexed and parsed with its tokens shifted into the composed
/// virtual source, the platform names are checked to agree across files, and
/// the non-`use` top-level items of every file are unioned (in path-sorted file
/// order) under one program. `use` items are consumed by assembly and dropped
/// here. Cross-file duplicate top-level declarations are detected later by
/// [`crate::resolve`], which already resolves the whole composed program
/// (Req 13.5).
///
/// Returns `(Some(program), diags)` when at least one file carried a `platform`
/// header; callers MUST NOT proceed past resolution while any error diagnostic
/// is present (Property 6).
pub fn compose_program(set: &SourceSet) -> (Option<Program>, Vec<Diag>) {
    let mut diagnostics = Vec::new();
    let mut platform: Option<Spanned<String>> = None;
    let mut items = Vec::new();

    for file in set.files() {
        let base = file.base_offset;
        let (tokens, lex_diags) = crate::lex(&file.content);
        // Shift every span into the composed virtual source so downstream
        // diagnostics are locatable (see module docs).
        for diag in lex_diags {
            diagnostics.push(shift_diag(diag, base));
        }
        let shifted: Vec<(Token, Span)> = tokens
            .into_iter()
            .map(|(token, span)| (token, base + span.start..base + span.end))
            .collect();

        let (program, parse_diags) = parse(&shifted, base + file.content.len());
        diagnostics.extend(parse_diags);

        let Some(program) = program else { continue };
        match &platform {
            None => platform = Some(program.platform.clone()),
            Some(first) if first.node != program.platform.node => {
                // All files compose into one platform; a divergent header would
                // silently mix kind libraries, so it is an error (design §
                // "union of all files' top-level declarations").
                diagnostics.push(
                    Diag::error(
                        program.platform.span.clone(),
                        format!(
                            "all definition files must declare the same platform; expected `{}`, found `{}`",
                            first.node, program.platform.node
                        ),
                    )
                    .with_hint("a deployment definition is a single platform composed from its files"),
                );
            }
            Some(_) => {}
        }
        for item in program.items {
            if !matches!(item, Item::Use(_)) {
                items.push(item);
            }
        }
    }

    let program = platform.map(|platform| Program { platform, items });
    (program, diagnostics)
}

// ── Assembly engine ───────────────────────────────────────────────────────

/// A diagnostic whose span is still file-local; finalized into a composed-source
/// span once base offsets are known.
struct Pending {
    rel_path: String,
    span: Span,
    message: String,
    hint: Option<String>,
}

struct FileData {
    content: String,
    sha256: String,
}

struct Assembler<'a> {
    canonical_root: PathBuf,
    bounds: &'a Bounds,
    /// Ingested files keyed by rel_path; `BTreeMap` gives the path-sorted order
    /// the digest and composition require (Req 13.4).
    files: BTreeMap<String, FileData>,
    pending: Vec<Pending>,
    total_bytes: usize,
    /// Rel paths on the active DFS path, for cycle detection (Req 13.4).
    stack: Vec<String>,
}

impl Assembler<'_> {
    /// Depth-first ingest of `rel_path` and its `use` edges. `depth` is the
    /// import-graph depth for the bound check (Req 12.4).
    fn walk(&mut self, rel_path: String, abs_path: PathBuf, depth: usize) {
        // Diamond imports are fine: a file already ingested is composed once.
        // Cycle detection is the stack check at the edge below, not here.
        if self.files.contains_key(&rel_path) {
            return;
        }

        let content = match std::fs::read_to_string(&abs_path) {
            Ok(content) => content,
            Err(err) => {
                self.pending.push(Pending {
                    rel_path: rel_path.clone(),
                    span: 0..0,
                    message: format!("could not read `{rel_path}`: {err}"),
                    hint: None,
                });
                return;
            }
        };

        if content.len() > self.bounds.max_file_bytes {
            self.pending.push(Pending {
                rel_path: rel_path.clone(),
                span: 0..0,
                message: format!(
                    "`{rel_path}` is {} bytes, over the {}-byte per-file limit",
                    content.len(),
                    self.bounds.max_file_bytes
                ),
                hint: None,
            });
            return;
        }
        self.total_bytes += content.len();
        if self.total_bytes > self.bounds.max_total_bytes {
            self.pending.push(Pending {
                rel_path: rel_path.clone(),
                span: 0..0,
                message: format!(
                    "deployment definition exceeds the {}-byte total limit",
                    self.bounds.max_total_bytes
                ),
                hint: None,
            });
            return;
        }
        if self.files.len() + 1 > self.bounds.max_files {
            self.pending.push(Pending {
                rel_path: rel_path.clone(),
                span: 0..0,
                message: format!(
                    "deployment definition exceeds the {}-file limit",
                    self.bounds.max_files
                ),
                hint: None,
            });
            return;
        }

        let scan = scan_file(&content);
        if let Some(span) = scan.nesting_violation {
            self.pending.push(Pending {
                rel_path: rel_path.clone(),
                span,
                message: format!(
                    "nesting exceeds the depth limit of {}",
                    self.bounds.max_nesting_depth
                ),
                hint: Some("flatten deeply nested records/lists or split the definition".into()),
            });
            // Keep going: still ingest and follow imports so other diagnostics
            // surface in one pass (Requirement 6.2).
        }

        let sha256 = sha256_hex(content.as_bytes());
        self.files.insert(
            rel_path.clone(),
            FileData {
                content: content.clone(),
                sha256,
            },
        );

        let importer_dir = abs_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.canonical_root.clone());

        self.stack.push(rel_path.clone());
        for (target, span) in scan.edges {
            match self.resolve_edge(&rel_path, &importer_dir, &target, span.clone()) {
                Ok((child_rel, child_abs)) => {
                    if self.stack.contains(&child_rel) {
                        self.pending.push(Pending {
                            rel_path: rel_path.clone(),
                            span,
                            message: format!("import cycle: `{rel_path}` → `{child_rel}`"),
                            hint: Some("the `use` graph must be acyclic".into()),
                        });
                    } else if depth + 1 > self.bounds.max_import_depth {
                        self.pending.push(Pending {
                            rel_path: rel_path.clone(),
                            span,
                            message: format!(
                                "import depth exceeds the limit of {}",
                                self.bounds.max_import_depth
                            ),
                            hint: None,
                        });
                    } else {
                        self.walk(child_rel, child_abs, depth + 1);
                    }
                }
                Err(pending) => self.pending.push(pending),
            }
        }
        self.stack.pop();
    }

    /// Validate one `use` target against the containment rules, returning the
    /// canonical `(rel_path, abs_path)` on success or a located [`Pending`] on
    /// any violation (Req 13.2, 13.3).
    fn resolve_edge(
        &self,
        importer_rel: &str,
        importer_dir: &Path,
        target: &str,
        span: Span,
    ) -> Result<(String, PathBuf), Pending> {
        let reject = |message: String, hint: &str| Pending {
            rel_path: importer_rel.to_string(),
            span: span.clone(),
            message,
            hint: Some(hint.to_string()),
        };

        let target_path = Path::new(target);
        // Lexical checks first — never touch the filesystem for an obviously
        // out-of-bounds literal (Req 13.2).
        if target_path.is_absolute() {
            return Err(reject(
                format!("import `{target}` must be a relative path"),
                "imports are relative and downward within the deployment root",
            ));
        }
        if target_path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
        {
            return Err(reject(
                format!("import `{target}` must not contain `..`"),
                "imports may not climb out of the deployment root",
            ));
        }

        // Canonicalize to resolve symlinks, then prove containment against the
        // real deployment root — this is what makes a symlink escape fail-closed.
        let joined = importer_dir.join(target_path);
        let canonical = joined.canonicalize().map_err(|err| {
            reject(
                format!("import `{target}` could not be resolved: {err}"),
                "the target must exist within the deployment root",
            )
        })?;
        let rel = canonical.strip_prefix(&self.canonical_root).map_err(|_| {
            reject(
                format!("import `{target}` resolves outside the deployment root"),
                "imports may not reference files outside the deployment root (no symlink escapes)",
            )
        })?;

        // Depth ≤ 1: at most one directory level below the root (Req 13.3).
        let dir_depth = rel
            .components()
            .filter(|component| matches!(component, Component::Normal(_)))
            .count()
            .saturating_sub(1);
        if dir_depth > 1 {
            return Err(reject(
                format!("import `{target}` is more than one directory below the deployment root"),
                "definition files live at the root or one directory below it",
            ));
        }

        Ok((to_forward_slash(rel), canonical))
    }

    /// Finalize: assign base offsets, build the composed source and digest, and
    /// convert pending diagnostics into composed-source-located [`Diag`]s.
    fn finish(self) -> Result<SourceSet, AssemblyError> {
        let mut files = Vec::with_capacity(self.files.len());
        let mut composed = String::new();
        let mut offsets: BTreeMap<String, usize> = BTreeMap::new();
        for (rel_path, data) in self.files {
            let base_offset = composed.len();
            offsets.insert(rel_path.clone(), base_offset);
            composed.push_str(&data.content);
            // Separator so the last token of one file and the first of the next
            // never merge, and line counting stays sane across the boundary.
            composed.push('\n');
            files.push(SourceFile {
                rel_path,
                content: data.content,
                sha256: data.sha256,
                base_offset,
            });
        }

        let digest = definition_digest(&files);

        let diagnostics: Vec<Diag> = self
            .pending
            .into_iter()
            .map(|pending| {
                let base = offsets.get(&pending.rel_path).copied().unwrap_or(0);
                let span = base + pending.span.start..base + pending.span.end;
                let mut diag = Diag::error(span, pending.message);
                if let Some(hint) = pending.hint {
                    diag = diag.with_hint(hint);
                }
                diag
            })
            .collect();

        if diagnostics.iter().any(Diag::is_error) {
            return Err(AssemblyError {
                diagnostics,
                composed_source: composed,
            });
        }

        Ok(SourceSet {
            files,
            composed,
            digest,
        })
    }
}

/// The result of a single-pass token scan over one file.
struct Scan {
    /// `(target_literal, span)` for each well-formed `use "…"` edge.
    edges: Vec<(String, Span)>,
    /// Span of the opener that first exceeded the nesting bound, if any.
    nesting_violation: Option<Span>,
}

/// Scan a file's tokens for `use` edges and a nesting-depth violation.
///
/// Edge discovery is intentionally syntactic and tolerant: `use` is a keyword
/// valid only as a top-level item, so any `use` immediately followed by a string
/// literal is an import edge. A malformed `use` (not followed by a string) is
/// left for the parse phase to report; this scan only needs the graph. Lex
/// errors are ignored here — the compose phase re-lexes and reports them.
fn scan_file(content: &str) -> Scan {
    let bounds = Bounds::default();
    let mut edges = Vec::new();
    let mut nesting_violation = None;
    let mut depth: usize = 0;

    let spanned: Vec<(Token, Span)> = Token::lexer(content)
        .spanned()
        .filter_map(|(result, span)| result.ok().map(|token| (token, span)))
        .collect();

    let mut index = 0;
    while index < spanned.len() {
        let (token, span) = &spanned[index];
        match token {
            Token::LBrace | Token::LBracket | Token::LParen => {
                depth += 1;
                if depth > bounds.max_nesting_depth && nesting_violation.is_none() {
                    nesting_violation = Some(span.clone());
                }
            }
            Token::RBrace | Token::RBracket | Token::RParen => {
                depth = depth.saturating_sub(1);
            }
            Token::Use => {
                if let Some((Token::Str(target), target_span)) = spanned.get(index + 1) {
                    edges.push((target.clone(), target_span.clone()));
                }
            }
            _ => {}
        }
        index += 1;
    }

    Scan {
        edges,
        nesting_violation,
    }
}

/// Canonicalize `abs` and confirm it is within `canonical_root`, returning its
/// `/`-joined relative path. Used for the root file (its own containment check).
fn canonicalize_within(
    canonical_root: &Path,
    abs: &Path,
) -> Result<(String, PathBuf), std::io::Error> {
    let canonical = abs.canonicalize()?;
    match canonical.strip_prefix(canonical_root) {
        Ok(rel) => Ok((to_forward_slash(rel), canonical)),
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "path outside deployment root",
        )),
    }
}

/// Render a relative path as a stable `/`-separated string (digest/composition
/// key). Non-`Normal` components cannot occur after `strip_prefix` of canonical
/// paths and are dropped defensively.
fn to_forward_slash(rel: &Path) -> String {
    rel.components()
        .filter_map(|component| match component {
            Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// Shift a diagnostic's span into the composed virtual source.
fn shift_diag(diag: Diag, base: usize) -> Diag {
    Diag {
        span: base + diag.span.start..base + diag.span.end,
        ..diag
    }
}

/// The content digest over the sorted `(relative_path, sha256)` set (Req 13.6).
///
/// `files` is already path-sorted (it came from a `BTreeMap`); the digest binds
/// both the path and the content hash of every file, so renaming a file or
/// editing its bytes both change the result, and read order never can.
fn definition_digest(files: &[SourceFile]) -> String {
    let mut hasher = Sha256::new();
    for file in files {
        hasher.update(file.rel_path.as_bytes());
        hasher.update([0u8]);
        hasher.update(file.sha256.as_bytes());
        hasher.update([b'\n']);
    }
    hex::encode(hasher.finalize())
}

/// Hex SHA-256 of `bytes`, matching the workspace convention (tokeira-state,
/// tokeira-aws).
fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{kind::KindLibrary, resolve::resolve};
    use std::fs;
    use tempfile::TempDir;

    const ROOT: &str = "compose.platform";

    /// Write `files` into a fresh deployment root and return the temp dir.
    fn deployment(files: &[(&str, &str)]) -> TempDir {
        let dir = tempfile::tempdir().unwrap();
        for (rel, content) in files {
            let path = dir.path().join(rel);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(path, content).unwrap();
        }
        dir
    }

    fn error_messages(err: &AssemblyError) -> Vec<&str> {
        err.diagnostics.iter().map(|d| d.message.as_str()).collect()
    }

    #[test]
    fn composes_multiple_files_in_path_sorted_order() {
        let dir = deployment(&[
            (
                ROOT,
                r#"platform compose {
                    use "runtime.platform"
                    use "infra.platform"
                    input region: String = "eu-west-2"
                }"#,
            ),
            (
                "infra.platform",
                r#"platform compose {
                    module local_state { resource s = LocalStateDir { } }
                }"#,
            ),
            (
                "runtime.platform",
                r#"platform compose {
                    module runtime {
                        depends_on [ local_state ]
                        service tokeirad = ComposeService { image: "tokeirad:latest" }
                    }
                }"#,
            ),
        ]);

        let set = assemble(dir.path(), ROOT).expect("assembles");
        // Path-sorted: compose, infra, runtime.
        let order: Vec<&str> = set.files().iter().map(|f| f.rel_path.as_str()).collect();
        assert_eq!(
            order,
            vec!["compose.platform", "infra.platform", "runtime.platform"]
        );

        let (program, diags) = compose_program(&set);
        assert!(diags.is_empty(), "compose diags: {diags:?}");
        let program = program.expect("program");
        assert_eq!(program.platform.node, "compose");
        // input + two modules, `use` items dropped.
        assert_eq!(program.items.len(), 3);

        // The composed program resolves clean across files (whole-program names).
        let resolve_diags = resolve(&program, &KindLibrary::compose());
        assert!(resolve_diags.is_empty(), "resolve diags: {resolve_diags:?}");
    }

    #[test]
    fn digest_is_a_pure_function_of_the_root_relative_file_set() {
        // Two byte-identical definitions in different absolute locations must
        // share a digest: rel paths are root-relative, so the temp dir path
        // never leaks in, and composition is path-sorted regardless of read
        // order (Req 13.6 / Property 19).
        let files = [
            (
                ROOT,
                "platform compose { use \"a.platform\"\nuse \"b.platform\" }",
            ),
            ("a.platform", "platform compose { input x: String = \"1\" }"),
            ("b.platform", "platform compose { input y: String = \"2\" }"),
        ];
        let set_a = assemble(deployment(&files).path(), ROOT).expect("a");
        let set_b = assemble(deployment(&files).path(), ROOT).expect("b");
        assert_eq!(
            set_a.digest(),
            set_b.digest(),
            "digest must depend only on the root-relative file set, not its location"
        );
    }

    #[test]
    fn digest_changes_when_content_changes() {
        let base = assemble(
            deployment(&[(ROOT, "platform compose { input x: String = \"1\" }")]).path(),
            ROOT,
        )
        .expect("base")
        .digest()
        .to_string();
        let edited = assemble(
            deployment(&[(ROOT, "platform compose { input x: String = \"2\" }")]).path(),
            ROOT,
        )
        .expect("edited")
        .digest()
        .to_string();
        assert_ne!(base, edited);
    }

    #[test]
    fn parent_dir_import_is_rejected() {
        let dir = deployment(&[(ROOT, "platform compose { use \"../escape.platform\" }")]);
        let err = assemble(dir.path(), ROOT).expect_err("must reject `..`");
        assert!(
            error_messages(&err)
                .iter()
                .any(|m| m.contains("must not contain `..`")),
            "got: {:?}",
            error_messages(&err)
        );
    }

    #[test]
    fn absolute_import_is_rejected() {
        let dir = deployment(&[(ROOT, "platform compose { use \"/etc/passwd\" }")]);
        let err = assemble(dir.path(), ROOT).expect_err("must reject absolute");
        assert!(
            error_messages(&err)
                .iter()
                .any(|m| m.contains("must be a relative path")),
            "got: {:?}",
            error_messages(&err)
        );
    }

    #[test]
    fn import_more_than_one_level_deep_is_rejected() {
        let dir = deployment(&[
            (ROOT, "platform compose { use \"a/b/deep.platform\" }"),
            (
                "a/b/deep.platform",
                "platform compose { input z: String = \"z\" }",
            ),
        ]);
        let err = assemble(dir.path(), ROOT).expect_err("must reject depth > 1");
        assert!(
            error_messages(&err)
                .iter()
                .any(|m| m.contains("more than one directory below")),
            "got: {:?}",
            error_messages(&err)
        );
    }

    #[test]
    fn one_level_deep_import_is_allowed() {
        let dir = deployment(&[
            (ROOT, "platform compose { use \"sub/child.platform\" }"),
            (
                "sub/child.platform",
                "platform compose { input z: String = \"z\" }",
            ),
        ]);
        let set = assemble(dir.path(), ROOT).expect("depth 1 is allowed");
        assert!(
            set.files()
                .iter()
                .any(|f| f.rel_path == "sub/child.platform")
        );
    }

    #[test]
    fn import_cycle_is_rejected() {
        let dir = deployment(&[
            (ROOT, "platform compose { use \"a.platform\" }"),
            ("a.platform", "platform compose { use \"b.platform\" }"),
            ("b.platform", "platform compose { use \"a.platform\" }"),
        ]);
        let err = assemble(dir.path(), ROOT).expect_err("must reject cycle");
        assert!(
            error_messages(&err)
                .iter()
                .any(|m| m.contains("import cycle")),
            "got: {:?}",
            error_messages(&err)
        );
    }

    #[test]
    fn diamond_import_is_not_a_cycle() {
        let dir = deployment(&[
            (
                ROOT,
                "platform compose { use \"a.platform\"\nuse \"b.platform\" }",
            ),
            ("a.platform", "platform compose { use \"shared.platform\" }"),
            ("b.platform", "platform compose { use \"shared.platform\" }"),
            (
                "shared.platform",
                "platform compose { input s: String = \"s\" }",
            ),
        ]);
        let set = assemble(dir.path(), ROOT).expect("diamond is fine");
        // `shared` ingested exactly once.
        assert_eq!(
            set.files()
                .iter()
                .filter(|f| f.rel_path == "shared.platform")
                .count(),
            1
        );
    }

    #[test]
    fn missing_import_target_is_rejected() {
        let dir = deployment(&[(ROOT, "platform compose { use \"absent.platform\" }")]);
        let err = assemble(dir.path(), ROOT).expect_err("must reject missing target");
        assert!(
            error_messages(&err)
                .iter()
                .any(|m| m.contains("could not be resolved")),
            "got: {:?}",
            error_messages(&err)
        );
    }

    #[test]
    fn missing_root_definition_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let err = assemble(dir.path(), ROOT).expect_err("must reject missing root");
        assert!(
            error_messages(&err)
                .iter()
                .any(|m| m.contains("root definition")),
            "got: {:?}",
            error_messages(&err)
        );
    }

    #[test]
    fn file_count_bound_is_enforced() {
        let dir = deployment(&[
            (
                ROOT,
                "platform compose { use \"a.platform\"\nuse \"b.platform\" }",
            ),
            ("a.platform", "platform compose { input x: String = \"1\" }"),
            ("b.platform", "platform compose { input y: String = \"2\" }"),
        ]);
        let bounds = Bounds {
            max_files: 2,
            ..Bounds::default()
        };
        let err = assemble_with_bounds(dir.path(), ROOT, &bounds).expect_err("over file limit");
        assert!(
            error_messages(&err)
                .iter()
                .any(|m| m.contains("file limit")),
            "got: {:?}",
            error_messages(&err)
        );
    }

    #[test]
    fn cross_file_duplicate_declaration_is_a_resolve_diagnostic() {
        // Req 13.5: whole-program resolution spans files; duplicates are caught.
        let dir = deployment(&[
            (
                ROOT,
                "platform compose { use \"a.platform\"\ninput region: String = \"x\" }",
            ),
            (
                "a.platform",
                "platform compose { input region: String = \"y\" }",
            ),
        ]);
        let set = assemble(dir.path(), ROOT).expect("assembles");
        let (program, diags) = compose_program(&set);
        assert!(diags.is_empty(), "compose diags: {diags:?}");
        let program = program.expect("program");
        let resolve_diags = resolve(&program, &KindLibrary::compose());
        assert!(
            resolve_diags.iter().any(|d| d
                .message
                .contains("duplicate top-level declaration `region`")),
            "expected cross-file duplicate diagnostic, got: {:?}",
            resolve_diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn diagnostic_spans_locate_into_the_composed_source() {
        // A parse error in an imported file must point at that file's text
        // within the composed virtual source, not the root's.
        let dir = deployment(&[
            (ROOT, "platform compose { use \"broken.platform\" }"),
            ("broken.platform", "platform compose { let bad }"),
        ]);
        let set = assemble(dir.path(), ROOT).expect("assembles (parse errors are compose-phase)");
        let (_program, diags) = compose_program(&set);
        assert!(!diags.is_empty(), "expected a parse diagnostic");
        let diag = &diags[0];
        let slice =
            &set.composed_source()[diag.span.start..diag.span.end.min(set.composed_source().len())];
        // The span lands in the imported file region (after the root's bytes).
        let broken = set
            .files()
            .iter()
            .find(|f| f.rel_path == "broken.platform")
            .unwrap();
        assert!(
            diag.span.start >= broken.base_offset,
            "span {:?} should be within broken.platform (base {}); slice={slice:?}",
            diag.span,
            broken.base_offset
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlink_escaping_the_root_is_rejected() {
        use std::os::unix::fs::symlink;

        let outside = tempfile::tempdir().unwrap();
        fs::write(
            outside.path().join("secret.platform"),
            "platform compose { }",
        )
        .unwrap();

        let dir = deployment(&[(ROOT, "platform compose { use \"link.platform\" }")]);
        // `link.platform` is a relative, no-`..` literal, but the symlink target
        // canonicalizes outside the deployment root — must fail-closed.
        symlink(
            outside.path().join("secret.platform"),
            dir.path().join("link.platform"),
        )
        .unwrap();

        let err = assemble(dir.path(), ROOT).expect_err("symlink escape must be rejected");
        assert!(
            error_messages(&err)
                .iter()
                .any(|m| m.contains("outside the deployment root")),
            "got: {:?}",
            error_messages(&err)
        );
    }
}
