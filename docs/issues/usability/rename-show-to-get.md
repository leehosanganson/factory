# Rename `show` to `get`

- **Finding:** The `show` command name does not match the requested vocabulary.
- **Desired outcome:** Use `get`. The original finding did not specify command
  families.
- **Status:** Implemented in the current CLI source for job, run, and monitor
  inspection. `get` is canonical; the former `show` (job/run) and `describe`
  (monitor) aliases are rejected with guidance to use the canonical command.
  The integrated tree is verified by the current `make test` and `make vet`
  checks.
