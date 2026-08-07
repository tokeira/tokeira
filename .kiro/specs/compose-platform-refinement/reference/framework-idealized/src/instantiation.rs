//! The instantiation stack: from the outer `PlatformDeclaration` to the
//! complete platform graph.
//!
//! Each level names what is constructed, from what, and which facts enter
//! scope there. A fact is available to a level exactly when some earlier
//! level put it in scope — so "how does the declaration inform
//! `DescribedDeployment`" is answered by reading the stack.
//!
//! A platform describes its infra + services. Both planes are first-class
//! through every level: one definition states both, one evaluation carries
//! both, and each plane keeps its own realization, store, engine, and verbs.
//!
//! ```text
//! L0  BIND (process start, once)
//!     platform() ─────────────────────▶ PlatformDeclaration
//!         static facts: the provider export (kind set, ops surface,
//!         reachability probe), the auxiliary selections (aws kinds), and
//!         every selection's extension constructors — the registration
//!         ingredients the deployment runs at L5, one per plane the
//!         selection participates in (infra / deploy / image)
//!     BoundPlatform::bind(id, format, declaration)
//!         + composed Vocabulary (collision-checked; namespace-aware:
//!           qualified references always work, bare names stay sugar
//!           while unique)
//!     Engine::new(bound_platform, frontend)
//!         + the marriage (format agreement)
//!
//! L1  ADMIT (once per command, at the shell boundary)
//!     platform.admit_deployment(dir) ─▶ Admitted {
//!         metadata,                      // name, recorded {format, path}
//!         deployment_ref,                // { name, dir }
//!     }
//!         admission runs ONCE per command invocation; every engine verb
//!         the command drives receives the same Admitted value — identity
//!         is never re-derived, metadata never re-read, the executable
//!         never re-verified between the verbs of one command
//!
//! L2  EVALUATE
//!     engine.evaluate(&admitted, override)
//!         frontend × vocabulary × EvaluationContext { project_name }
//!     ────────────────────────────────▶ EvaluatedDefinition
//!         + the config value (carrying provider attribute namespaces,
//!           e.g. `aws.region`)
//!         + the structural graph, born split: the definition states each
//!           node's plane (`.resource(` / `.service(`) and evaluation
//!           refuses a mismatch with what the kind decodes to; one
//!           dependency space (a service may depend on resources and
//!           services; a resource depending on a service is refused)
//!         + namespaces, writeback, and the deployment's authored tags
//!
//! L3  REALIZE
//!     verify_definition ─▶ realize(deployment_id, dir, definition_dir,
//!                                  graph.tags)
//!         per node: PlacementContext { deployment facts, module,
//!         logical_id, dependencies, dependency_content, tags (the
//!         graph's — never an empty map), provider_attributes (the
//!         namespace blocks) }
//!     ────────────────────▶ two realizations from the one graph:
//!         infra nodes   ▶ resources, manifests, index
//!         service nodes ▶ the deploy engine's service set (+ images
//!                         where declared); restart-on-config-change
//!                         rides the hashed manifest
//!         Attachment is for facts a resource OWNS — its identity, its
//!         region, its content digests, its effective tags. Shared
//!         runtime handles (clients, platform connections) are NOT
//!         attached: they are extensions, delivered at L5 through the
//!         registration contract, and read from the context at the
//!         mechanics moment.
//!
//! L4  ASSEMBLE
//!     ────────────────────────────────▶ ExecutionState, both planes:
//!         infra: modules, resources-by-module, namespaces, writeback
//!                declarations, index, manifests
//!         deploy: the service set, images
//!         + the graph's tags, carried to the contexts' standard fields
//!         (identity is NOT carried — it travels as the Admitted value)
//!
//! L5  OPEN (per operation)
//!     platform.execution().probe(&admitted.deployment_ref)
//!         Some(issue): plan blocks — plans nothing, renders the issue
//!         document; apply/destroy refuse outright
//!     DescribedDeployment::new(execution_state, admitted.deployment_ref,
//!                              declaration.extension_constructors)
//!     state stores: constructed from the deployment's RECORDED
//!         state-backend option — an operator choice tkr surfaces at
//!         create (local by default, remote when selected); never
//!         platform data, never framework-hardwired — one store per
//!         plane: state/infra, state/deploy
//!     InfraEngine / the deploy engine, as the operation needs
//!         └─ THE REGISTRATION CONTRACT, unchanged:
//!            register_infra_extensions(config, ctx)
//!                sets ctx.project_name (from Admitted) and ctx.tags
//!                (from the graph), then runs every selection's infra
//!                constructor:
//!                  compose provider ▶ ComposePlatform + recovery hook
//!                  aws selection    ▶ AwsClients from its `aws.region`
//!            register_deploy_extensions / register_image_extensions
//!                the same contract on the other planes: each runs the
//!                selections' corresponding phase constructors (compose's
//!                deploy plane needs none; ECS's and EKS's arrive with
//!                their onboarding)
//!
//! L6  OPERATE
//!     infra  ▶ plan / apply / destroy
//!     deploy ▶ plan / apply — the sibling verb family, reconciling the
//!              service set through the deploy engine
//!     ops    ▶ logs / ports (scale joins as local and ECS onboard) —
//!              the declaration's ops surface answering live substrate
//!              questions, outside the lifecycle path
//!     collect_writeback(execution, recorded state) ▶ tokeirad.toml
//!     compose derives docker-compose.yml at deploy apply — a standard,
//!         non-authoritative artifact of deployment, owned by the
//!         provider, not a framework capability
//! ```
//!
//! # Stated properties (deliberate, not defects)
//!
//! Every verb reaches L5 through L2–L4: destroy requires an evaluable
//! definition. Recovery reconstructs live resources from recorded state
//! within an operation; it does not substitute for the definition.
//!
//! The probe runs after evaluation, so a broken definition surfaces before
//! an unreachable substrate. Both properties are today's behaviour, kept
//! on purpose — changing either is a design decision, not a fix.
//!
//! # What informs `DescribedDeployment` (the L5 answer)
//!
//! Three inputs, each from a named earlier level:
//!
//! 1. **The execution state (L4)** — every graph answer, both planes:
//!    bootstrap (the root module), infra modules, namespaces, writeback,
//!    the index, the service set and images — and the graph's tags for the
//!    contexts' standard fields.
//! 2. **The admitted deployment ref (L1)** — the coordinates the
//!    registration constructors and the recovery hook consume, and the
//!    project name the contexts carry.
//! 3. **The declaration's extension constructors (L0), with each
//!    selection's attribute block (L2)** — the registration ingredients.
//!    Registration happens inside `register_*` and nowhere else: the
//!    deployment runs the constructors; the constructors put handles into
//!    the context; resources read the context. `ProviderExecution::install`
//!    does not exist — the probe alone remains on the provider export,
//!    because reachability is a substrate question and its answer is data,
//!    not registration.
