# Job inspection hides implementation publication outcome

- **Finding:** The compact default `factory job get`/`job list` table omits the
  persisted publication outcome for completed implementation jobs.
- **Desired outcome:** Keep the default table compact while showing publication
  status when an implementation job has one; leave other rows unchanged.
- **Status:** Implemented in `internal/factory/job.go` with a `PUBLICATION`
  column containing the outcome for implementation rows only. Rows without an
  outcome and non-implementation rows keep this cell blank. Behavioral tests
  cover `published`, `unpublished`, `no-op`, absent outcomes, both default
  commands, and the unchanged one-header/one-row `job get` shape. Verified by
  `make test`, `make vet`, and `make build`.
- **Reproduction:** Inspect completed implementation jobs with
  `factory job get <id>` and `factory job list`.
- **Impact:** Users cannot distinguish successfully published work from
  unpublished or no-op results without opening detailed output.
