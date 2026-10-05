# Approved organization identity validation

- **What worked:** Adding `FACTORY_LIVE_GITHUB_APPROVED_ORGANIZATION` as an exact configuration value makes the approval boundary explicit instead of treating any repository owner as acceptable. The repository API identity check is read-only and runs before branch/PR mutations, including cleanup.
- **Evidence:** Credential-free tests cover exact organization login/type, mismatched organizations, user-owned repositories, and refusal to proceed to cleanup mutations after identity mismatch. No GitHub credentials or live requests were used.
- **Remaining setup:** The approved organization and sandbox are still unprovisioned under #134, so live identity and workflow behavior remain unverified.
