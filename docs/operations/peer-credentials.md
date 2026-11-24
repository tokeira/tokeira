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

`OfferWorkflowTask` is installed only for peer-capable daemons. A run's execution home pushes each workflow task it
publishes to the task queue's home, which admits the task to its broker or refuses it with one of the outcomes
below. Requests never write durable state. A sticky refusal sends the task to its normal queue's home with the
original deadline, and a wrong home is retried once after a placement refresh. A full broker, or a lost request or
reply, leaves the task to the execution home's sticky sweep or to the queue home's discovery.

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
| `workflow_id`, `use_ramping_version` | Nonempty workflow ID and the version directive's ramping flag: inputs to the receiver's own current or ramping version selection. |
| Outcome | Known `Admitted`, `Known`, `StickyUnavailable`, `NotHome`, `Full` or `Expired`; a known reason is present exactly for `StickyUnavailable`. |

Unknown enum values and inconsistent shapes are `InvalidArgument`. Unknown protobuf fields use normal protobuf
compatibility rules but still count against the pre-decode frame bound. Offers have a 64 KiB request budget,
4 KiB client response budget, 8 KiB total metadata budget (including an individually bounded Bearer value),
64 concurrent incoming handlers and a 500 ms attempt deadline. Overload returns `ResourceExhausted` immediately;
there is no handler wait queue. The existing start method retains its 128 MiB response budget.

`OfferActivityTask` is installed only for peer-capable daemons and uses the same offer-only credential policy.
The execution owner pushes each due activity it publishes to the activity queue's home after commit; retries
push when due, and acquisition restores delivery obligations after repair and activation. Requests write nothing
durable and acknowledge only broker retention. Discovery covers a lost, refused or unanswered push. Prompt push
is a tested obligation, not a proved latency bound.

| Field | Shape policy |
| --- | --- |
| `run_key`, `namespace_id` | Non-nil 16-byte UUIDs. |
| `activity_id` | Nonempty; bounded by the complete encoded request size. |
| `schedule_event_id`, `attempt` | Positive signed 64-bit schedule event ID and nonzero unsigned 32-bit attempt. |
| `stamp` | Full unsigned 64-bit value; zero is valid. |
| `execution_home`, `destination_home` | Unsigned IDs below the configured home count; zero is valid. Admission recomputes the destination from namespace and queue name and requires serving ownership. Start derives the execution home again from authoritative state. |
| `selected_queue` | Required, with a nonempty name. Optional deployment/build coordinates preserve presence, including present empty values; a build without a deployment is accepted. The method implies a normal activity queue; no task-kind or sticky field is sent. |
| `dispatch_revision` | Full signed 64-bit value, including zero and negative values. |
| `dispatch_at`, `expires_at` | Required timestamps, with seconds from -62,135,596,800 through 253,402,300,799 and nanos from 0 through 999,999,999, representable by the runtime clock. Expiry must be strictly after eligibility. |
| `priority` | Optional; preserves absence, signed priority key and fairness key. Fairness weight must be finite and nonnegative; zero retains unset semantics. |
| Outcome | Known `Admitted`, `Known`, `NotHome`, `Full`, `Expired` or `NotDue`. Zero/unspecified and unknown values are malformed replies. |

Invalid request shapes are `InvalidArgument`. Unknown protobuf fields follow normal compatibility rules and
count against the pre-decode frame bound. Priority and original eligibility are preserved; the receiving home
assigns its own fairness/FIFO position. No input, header, heartbeat payload, start result or redirect endpoint
crosses this offer method.

| Outcome or refusal | gRPC status | Meaning and sender action |
| --- | --- | --- |
| `Admitted` | `OK` | Retained for a compatible poll; end this attempt without removing the dispatch row. |
| `Known` | `OK` | Already retained; end this attempt without extending its expiry. |
| `NotHome` | `OK` | Wrong destination or no serving acquisition; refresh trusted placement once within the original budget. |
| `Full` | `OK` | Broker count, byte or reservation limit; end this attempt and leave recovery to discovery. |
| `Expired` | `OK` | Absolute hint deadline elapsed; retain nothing and end this attempt. |
| `NotDue` | `OK` | Eligibility is still in the future; retain nothing and end this attempt. |
| Malformed request | `InvalidArgument` | No admission; end this attempt. |
| Missing, invalid, ambiguous or expired credential | `Unauthenticated` | No admission; end this attempt. |
| Verified identity outside the push policy | `PermissionDenied` | No admission; end this attempt. |
| Request/metadata limit or incoming handler exhaustion | `ResourceExhausted` | Immediate refusal without a handler wait queue; end this attempt. |
| Receiver peer clock unavailable | `Unavailable` | Retry within the original budget, then rely on discovery. |
| Attempt deadline elapsed | `DeadlineExceeded` | Admission is uncertain; retry within the original budget, then rely on discovery. |
| Absent method, including an older peer | `Unimplemented` | End this attempt; discovery covers the task. |

Offers have a 64 KiB request budget, 4 KiB client response budget and 8 KiB total metadata budget.
The 64 incoming handlers are shared with `OfferWorkflowTask`. Activity sending has independent accounting:
1,024 retained items and 16 MiB including active work, 32 active calls and eight per destination, 500 ms per
attempt, one second total including queue wait, and at most three attempts. Coalescing and retries retain the
original budget; source-acquisition loss or shutdown cancels pending work. Local placement or transport failures
may retry within that budget; local signing, capacity and decoded-reply validation failures end the attempt.
These local failures are not receiver gRPC statuses. A decoded unknown/unspecified outcome ends the attempt;
a malformed initial `grpc-status=0` reply becomes client `Internal` and follows the bounded transport retry path.

The broker admits local, pushed and discovered activities through the same limits: 256 per selected queue/version,
8,192 per home including reservations, and 64 MiB of retained activity hints across the broker, including in-flight
records. Ready retention ends at the earlier of five seconds and the absolute hint deadline; taking an offer
starts a separate five-second in-flight lease. The sender chooses expiry at most five seconds from creation,
capped by applicable committed schedule deadlines. Acknowledgement never authorizes start: the execution owner
still checks worker admission, incarnation, routing, rules and its current fence. No new credential, installation
or rollout step is required.
