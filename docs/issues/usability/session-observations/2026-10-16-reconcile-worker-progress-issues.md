# Session observations: reconcile submitter and worker-progress issues

- Audited #247 and #262 against merged #254 and current REST server/config/operations documentation. Named submitter/operator authorization is shipped; stale issue text still described a single shared-key-only server and unimplemented auth decisions.
- Prepared an updated #247 body limiting remaining scope to worker progress after #248 implementation. Shipped current behavior remains separately linked in canonical feature docs. #262's criteria are satisfied as an audit/reconciliation task; proposed closing it avoids duplicating #247.
- Updated #251 dependency wording to ensure its UI may only use implemented progress-safe fields and cannot invent remote worker status. No code or API changes.
- Work remains documentation/issue maintenance. Validate update previews/comments, preserve issue history in comments, and do not close #247 since worker progress remains blocked.
