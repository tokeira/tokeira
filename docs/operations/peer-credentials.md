# Runtime peer credentials

Every controller-managed node requires a peer credential file before readiness, including a cluster with one
replica. A single node without a placement controller and all embedded engines need no file and build no peer
components. An embedded public listener does not change that exemption. The existing single-node claim and
embedded ownership safeguards are unchanged.

## Generate and install

Provisioning belongs to deployment tooling outside this repository.

Create a protected directory, then generate one file for the cluster:

```sh
tokeirad peer-credential generate --out /etc/tokeira/peer.json
```

The parent directory must already exist. The command creates a mode-0600 file, refuses to overwrite an existing
file, and rejects symlinks in the path. It prints only this non-secret configuration, with the actual absolute path:

```toml
[infrastructure.peer]
path = "/etc/tokeira/peer.json"
```

Stop every node. Securely install the same file on every controller-managed node, owned by the runtime user with
mode 0600, and add its local absolute path to each node's configuration before restarting. Protect the containing
directory against access by unrelated users. Paths must contain no symlinks. Never generate independent files for
nodes in the same cluster, and never put the document in ordinary configuration, logs, manifests or images.

The version-1 JSON document contains a random cluster UUID, an ES256 key as base64url-encoded PKCS#8 DER, and its
public profile. The profile contains the exact issuer/audience/subject, public JWKS, key ID and SHA-256 fingerprint
of the uncompressed SEC1 public point. The file supplies the verifier profile; there is no separate sidecar.
Startup bounds both the document and JWKS to 64 KiB, checks private/public agreement and exercises signing before
readiness. Public Worker/Admin authentication is unchanged.

## Restart, rotation and repair

Restart reads the existing file without changing it. Each node signs five-minute tokens locally and reuses a
cached token only while at least one minute remains. Renewal needs no file read or external service.

For rotation, explicitly generate a new file at a new path. Stop every node, install that same replacement on
every controller-managed node, update each configured path if needed, then restart. The replacement has a fresh
cluster identity and key; only its key is trusted and old tokens fail. Rolling or mixed-key rotation is unsupported.

Missing configuration or a missing file reports `peer credentials missing`. Invalid permissions, corrupt contents,
mismatched keys/profile or failed signing report `peer credentials inconsistent`. Both errors give the same remedy:
“generate a credential file with `tokeirad peer-credential generate --out <path>`, stop every node, install the same
file on every peer-capable node, and restart”. Neither error includes key, token or rejected document content.
A node never generates a replacement automatically or disables peer authentication to become ready.

## Offer-only receiver policy

The signed issuer must equal `urn:tokeira:peer:<cluster-uuid>`, the audience must contain exactly the configured
`urn:tokeira:workflow-offers:<cluster-uuid>` value, and the subject must be `tokeira-runtime-peer`. The pinned key
and ES256 algorithm are mandatory. Numeric `iat` and `exp` are required; `nbf`, when present, must also be numeric.
Validity permits at most 30 seconds of clock leeway, requires expiry after issuance and limits lifetime to one hour.
Anonymous access, public Worker/Admin roles and Worker scope claims do not confer the separate peer grant.
Public authentication and worker authorization at authoritative start remain unchanged.

The shared cluster key authenticates the offer capability, not a unique process or ownership lease. Compromised
material requires a stopped-node rotation. Key/token-bearing types redact Debug output, and errors contain only
fixed diagnostics.

## Internal wire contract

`OfferWorkflowTask` is installed only for peer-capable daemons. Valid authenticated requests currently return
`Unavailable`: the existing broker cannot yet back admission acknowledgements. No request calls its publisher or
writes durable state. No outbound sender is activated by this change.

| Field | Shape policy |
| --- | --- |
| `run_key`, `namespace_id` | Non-nil 16-byte UUIDs. |
| `logical_seq` | Positive durable incarnation representable by the existing signed sequence type. |
| `execution_home`, `destination_home` | Unsigned home IDs; authority/placement agreement belongs to later admission/start. |
| `selected_queue`, `queue_kind` | Nonempty full queue name, optional deployment/build coordinates with presence preserved, known normal/sticky kind. A build coordinate requires a deployment coordinate. |
| `routing` | Known live/exact mode; live has no fixed coordinates, exact requires a deployment. |
| `version_directive` | Known behavior; pinned requires deployment/build. Assigned build remains optional source routing information. |
| `scheduled_at`, `priority` | Valid protobuf timestamp; finite nonnegative fairness weight. Original scheduling time and priority are preserved. |
| `sticky_origin` | Required for sticky delivery; both original queues, worker identity and valid deadline at or after scheduling are required. Sticky selection matches the original sticky queue; normal fallback retains the original normal name and deadline. |
| Outcome | Known `Admitted`, `Known`, `StickyUnavailable`, `NotHome`, `Full` or `Expired`; a known reason is present exactly for `StickyUnavailable`. No outcome is emitted before admission is activated. |

Unknown enum values and inconsistent shapes are `InvalidArgument`. Unknown protobuf fields use normal protobuf
compatibility rules but still count against the pre-decode frame bound. Offers have a 64 KiB request budget,
4 KiB client response budget, 8 KiB total metadata budget (including an individually bounded Bearer value),
64 concurrent incoming handlers and a 500 ms attempt deadline. Overload returns `ResourceExhausted` immediately;
there is no handler wait queue. The existing start method retains its 128 MiB response budget.
