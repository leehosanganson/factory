# Controller Thread model

## Status and scope

This is a target design for the future Factory-unit control plane, not current REST behavior. It defines user-facing work contexts and their relationship to Jobs and scheduled Tasks. For the first direct-request implementation slice, each newly admitted request creates one Thread and its initial Job atomically; retries under the same authenticated principal and `Idempotency-Key` return that same pair, while a distinct key creates a distinct Thread. Job-only `/v1/jobs` admission remains available for standalone/scheduled Jobs. This first slice does not define webhook/provider event idempotency or Thread reuse. Factory's current REST service admits and tracks Jobs but does not implement controller-managed Threads. Implementation is tracked in [#301](https://github.com/leehosanganson/factory/issues/301); unit registration and placement are separate in [#297](https://github.com/leehosanganson/factory/issues/297). See the [Factory-unit MVP architecture](factory-unit-mvp.md) for the broader controller, unit, and harness boundaries.

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

On transition to Done, the controller notifies the user idempotently. Done is not closure. Only the user explicitly marks the Thread completed at the controller; that action closes it. If the user adds a Job before closure, recompute aggregate status and move it to In Progress or Need Attention as applicable. The system must not silently discard a Done Thread or automatically close it after a timeout.

For the first API slice, Thread creation uses the same create-role policy as `POST /v1/jobs`: the legacy shared operator key, named operators, and named submitters may create; operators may inspect any Thread, while submitters may inspect only Threads attributed to their principal. An unauthorized or missing submitter Thread returns the same not-found result. Thread keys are in a separate idempotency namespace from standalone Job keys so `/v1/jobs` behavior does not change. A Thread and initial Job consume one existing Job admission slot; they do not create a second queue or capacity budget. Thread/member records are retained with the Job, and idempotent replay is guaranteed only while the associated record and key are retained under store policy.

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
