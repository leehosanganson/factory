# Controller Thread model

## Status

The current API already supports direct-request Threads, ordered Job membership, explicit completion, and an owner-filtered in-product notification inbox. It also has an optional GitHub issue webhook, but issue poller/webhook intake is deferred from the agreed Worker MVP. Current routes and ownership details are documented in [REST server operations](../features/rest-server-operations.md) and the [GitHub webhook guide](../features/github-issue-webhook.md).

The target Controller/Worker placement and harness boundary are in [Factory Worker MVP architecture](factory-unit-mvp.md). This contract focuses on the concepts and invariants to preserve, not route syntax.

## Threads, Jobs, and schedules

A Thread is a user-facing work context, not an operating-system thread. It groups one or more Jobs from a user's request and presents their derived status. It does not create a second queue or change Job lifecycle authority. Users can inspect Thread Jobs and results, append work while the Thread is open, and explicitly complete the Thread.

A recurring schedule creates ordinary standalone Jobs through the same admission, assignment, execution, and outcome path. Scheduled Jobs remain outside Threads. The Controller owns schedules, firing, and operator management through the required browser UI. The current API implements durable five-field UTC cron and revision-guarded mutations; detailed current behavior is in the [REST server guide](../features/rest-server.md) and [server configuration reference](../features/rest-server-config.md). The target MVP does not yet settle recurrence syntax or missed-run policy.

## Status, completion, and notifications

Thread status is derived deterministically from its member Jobs. It is **In Progress** while work is queued or running, **Need Attention** when a Job has unresolved recovery or needs a human decision, and **Done** when all member Jobs have outcomes and none needs attention. A Thread summary cannot change Job state or authorize cancellation, retry, or recovery.

Done is not closure. The owner explicitly completes a Thread; completion does not rewrite or cancel Jobs, and a completed Thread rejects new work. When an unclosed Thread becomes Done, the Controller creates one in-product notification for its owner. Notification reads do not change Job or Thread state. The inbox is not email, chat, or push delivery.

Access must preserve user ownership: users see and manage their own Threads; operator permissions do not make a Thread a shared queue. Admission and member appends use idempotency so retries do not duplicate work. Exact route, role, pagination, and transaction semantics are in the current REST operations guide.

## Boundaries

- Threads group work; Jobs remain the units of admission, assignment, execution, cancellation, and outcomes.
- The Controller assigns Jobs. Users cannot select a Worker or one of its locally configured harnesses in a request.
- Schedules create standalone Jobs. They do not add work to a Thread implicitly.
- The MVP does not require issue poller or webhook intake, automatic Thread closure, external notification delivery, or multi-harness collaboration within a Job.
- Factory makes no sandbox or security-isolation guarantee. Operators own repository, harness, credential, and deployment risk; bounds and cancellation do not contain hostile code.

This is target guidance, not a claim that the Controller-mediated Worker architecture or its browser workbench has shipped.
