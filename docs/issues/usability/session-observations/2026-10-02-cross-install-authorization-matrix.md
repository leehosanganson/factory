# Cross-installation authorization test matrix

- **Type:** Design and test-planning observation for the REST MVP; not a user-reported finding.
- **What worked:** The approved policy that clients do not select installations and the linked user grant is checked against App access per requested repository can be expressed as a bounded source/target matrix. It includes same-installation and distinct-installation success cases, denied or unavailable checks, no-queue-write outcomes, and rechecking before provider writes.
- **Scope status:** Added future acceptance criteria to the REST API contract only. No admission, OAuth, GitHub provider calls, queue behavior, or worker execution was implemented. Cross-installation support remains gated on implementation tests.
- **Friction:** The design described source-issue and target-repository authorization separately but did not spell out outcomes when checks were denied, stale, unavailable, or from distinct installations. A matrix gives implementers concrete cases without expanding the chosen identity model.
