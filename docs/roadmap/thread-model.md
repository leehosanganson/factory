# Controller Thread model

## Status and scope

The current direct-request Thread slice implements authenticated `POST /v1/threads`, `GET /v1/threads/{id}`, `POST /v1/threads/{id}/jobs`, bodyless `POST /v1/threads/{id}/complete`, and `GET /v1/notifications` on memory, SQLite, and PostgreSQL stores. Creation atomically admits one Thread and initial Job; member addition atomically appends the next ordered Job; explicit completion records owner and timestamp independently of derived status and rejects new members. Creation and member idempotency use separate principal-scoped key namespaces, and adding work to an open Done Thread recomputes status as In Progress. When an unclosed Thread first becomes Done, one notification row for its submitter is committed in the same transaction as the terminal Job transition; a later Done cycle creates another row. The authenticated owner inbox exposes only bounded identity/status/timestamp fields, has bounded retention and pagination, and is committed availability rather than email/push/chat delivery. Reads do not create rows or record acknowledgements, and notifications never change Job or Thread state. Job-only `/v1/jobs` admission remains available for standalone/scheduled Jobs. Webhook/provider ingress and controller/unit placement remain unimplemented. This is a partial implementation of [#301](https://github.com/leehosanganson/factory/issues/301), not the full Thread target. Unit registration and placement are separate in [#297](https://github.com/leehosanganson/factory/issues/297). See the [Factory-unit MVP architecture](factory-unit-mvp.md) for the broader controller, unit, and harness boundaries.

## Ingress and membership

A **Thread** is a user-facing work context, not an operating-system thread. The controller creates one for each authenticated direct user request or supported webhook/provider/project-management event. Source identity, authorization, idempotency, and event-deduplication rules are defined by each ingress adapter; retries of the same stable source event must not create duplicate Threads or Jobs. Unrelated events must not be appended to an existing Thread without an explicit correlation rule.

A Thread owns one or more Jobs induced by that request/event. Job admission, assignment, execution, verification, provider publication, and outcome remain the ordinary Factory Job lifecycle. A Thread groups and summarizes those Jobs; it does not create a second execution queue or change Job lifecycle authority.

A **Scheduled Task** is a schedule definition. Every firing creates a standalone scheduled Job, outside Thread membership. It uses the same controller admission, assignment, execution, and outcome lifecycle. Scheduled Jobs and Task definitions remain inspectable through controller interfaces, but do not gain a Thread implicitly.

## Aggregate status and completion

Thread status is a deterministic classifier/reducer over persisted member-Job state and events, not an unconstrained model judgment. Apply this precedence:

1. **Need Attention** if any member Job awaits human input, recovery, or unresolved operator disposition. For the current Job model, this includes a `running` Job classified by its durable Store as requiring operator recovery, such as after restart or expired ownership; a live running Job is not in this category.
2. Otherwise **In Progress** if any member Job is queued or live-running.
3. Otherwise **Done** when every member Job is terminal (`succeeded`, `failed`, or `canceled`) and none needs input or recovery.

The classifier summarizes state only: it cannot mutate Job state, cancel/retry Jobs, authorize recovery, or close a Thread. If lifecycle state is unknown or needs operator disposition, classify as Need Attention rather than Done. In this first slice, terminal failure/cancellation count as complete; future human-input states may refine the status mapping without changing Job lifecycle authority.

On transition to Done, the REST store commits one idempotent owner-inbox notification atomically with the lifecycle transition; this is not external delivery. Done is not closure. Only an authenticated owner explicitly marks a Thread completed through `POST /v1/threads/{id}/complete`; explicit closure is permitted at any aggregate status and does not alter member Jobs or their derived status. Repeated completion is idempotent and inspectable through `completed_at` and `completed_by`. Completion serializes against member admission: if a new Job commits first, explicit completion can then close the Thread; if completion commits first, later new member admission is rejected. Exact retries of an already-admitted member remain idempotent replays. The system must not silently discard a Done Thread or automatically close it after a timeout.

For the first API slice, Thread creation uses the same create-role policy as `POST /v1/jobs`: the legacy shared operator key, named operators, and named submitters may create. Operators may inspect any Thread but may append Jobs or explicitly complete only Threads attributed to their authenticated principal; submitters may inspect, append to, and explicitly complete only their own Threads. Missing and differently attributed Thread IDs return the same not-found result. Thread creation and member keys use separate principal-scoped namespaces, distinct from standalone Job keys. Each Thread/member Job consumes one existing Job admission slot; no second queue or capacity budget is created. Thread/member records are retained with Jobs, and idempotent replay is guaranteed only while the associated records and keys remain retained under store policy.

## Concurrency and projections

Controller replicas are interchangeable replicas of one logical control plane sharing authoritative durable state. Concurrent ingress, duplicate delivery, Job state changes, status reduction, notification, and user completion must converge on one consistent Thread view. Persist enough event/source identity and membership ordering to make retries deterministic and status recomputable.

Thread detail lists only its own Jobs in stable order. Aggregate listings must enforce existing submitter/operator visibility rules and avoid exposing raw task payloads, transcripts, provider secrets, or unbounded output. A Thread's summary is not a correctness verdict; users inspect the relevant Job evidence and provider outcome separately.

## Boundaries

- Thread ingress and membership do not grant a submitter permission to select unconfigured repositories, harnesses, executables, credentials, host authority, or expanded Job policy.
- Threads do not place Jobs on units. Named-unit pinning and simple eligible-unit scheduling are controller responsibilities specified in the [Factory-unit MVP architecture](factory-unit-mvp.md).
- Thread status does not depend on durable agent handoffs, Kitsume, or an LLM classifier. Optional communication and memory integrations do not own Thread or Job state.
- Scheduled Tasks remain outside Threads. No autonomous issue scheduler, arbitrary callbacks, automatic follow-up Jobs, or automatic Thread closure is implied.
- Factory makes no sandbox or security-isolation guarantee. Users/operators own repository and model/harness threat risk; operator-configured repositories, enabled harness profiles, credentials, and bounded Job policy remain authoritative.

These are future design rules, not claims of shipped behavior or deployment authorization.
