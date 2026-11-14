# Roadmap

Factory is evolving into a Compose-deployable service for managing long-running, agent-driven work:

**browser request → Thread/Job → Controller assignment → Worker-selected harness → inspectable result and notification**

The target Controller owns user work, Jobs, Threads, recurring schedules, assignment, notifications, and outcomes. Workers are separate Controller clients; each selects one harness from its locally configured set. PostgreSQL stores durable shared state. The browser UI is the user-facing workflow. A successful harness result completes a Job; provider publication is optional.

## MVP scope

**Keep:** Controller, PostgreSQL, separate Workers, browser-based Thread/Job management, recurring schedules and their UI, notifications, bounded execution and cancellation, inspectable results.

**Defer:** local CLI as the user-facing path; GitHub issue poller/webhook; Worker mTLS and identity/session management; remote host/harness provisioning; collaboration among multiple harnesses in one Job; automatic unsafe replay/reassignment; HA and general fleet management.

The initial Worker-to-Controller credential is a basic operator-configured shared secret. Requests cannot select a Worker’s harness, executable, repository path, credentials, or host authority. Factory does not merge, release, or deploy. A container or harness is not a security sandbox.

## What exists today

The repository already includes a REST job service, PostgreSQL storage, schedules, Threads, notifications, an operator dashboard, and separate Worker processes. This is a foundation, not the target architecture:

- Compose currently starts the Controller and PostgreSQL without a Worker by default.
- Current separate Workers connect directly to PostgreSQL; Controller-mediated assignment and result reporting are not implemented.
- Current server execution config selects one harness; Worker-local multi-harness choice is target behavior.
- Current dashboard is operator-focused, and configured-provider jobs require confirmed PR publication for success.
- Issue poller and GitHub webhook are implemented optional features, but deferred from this MVP.

For shipped behavior, start at [implemented features](../features/README.md). The target architecture is specified in [Factory Worker MVP](factory-unit-mvp.md); lifecycle invariants are in the [REST job contract](rest-api-contract.md) and [Thread model](thread-model.md). Operational, configuration, and protocol detail remains in the linked feature and contract references rather than duplicated here.
