# REST reconciliation operator documentation session

- **Scope:** Updated the REST operations guide for the merged explicit provider-outcome reconciliation route and recorded an operator recipe.
- **What worked:** The merged route, SQLite transaction, executor identity checks, and process E2E provide concrete source-of-truth details for documentation; the existing README and feature index already link to this guide.
- **Friction:** The operations section previously described recovery generically and its process-E2E paragraph did not distinguish startup-interrupted running jobs from terminal failed jobs later eligible for explicit reconciliation. Checking the merged implementation and test clarified the boundary without needing broader docs edits.
- **Verification boundary:** Documentation-only local link and whitespace checks; no service behavior or live provider was exercised. CI status will be checked on the resulting pull request.
