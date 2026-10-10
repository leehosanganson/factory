# Session observations: REST parallel subtasks

- The initial process E2E failed as expected because runtime explicitly cleared parallel implementation configuration. Once enabled, it exposed the executor's second explicit disable point.
- Replacing a JSON string fragment after marshaling was brittle for this test setup; configuring the typed REST config before marshaling avoids dependence on serialized key order/format.
- A plan-driven process fixture can use barriers and a fake provider marker to check overlap, dependent-wave visibility, and publication ordering without live provider access.
