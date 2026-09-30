# Principal bearer verifier implementation

- Implemented the approved loader/verifier in `internal/factory` without changing CLI startup, adding network behavior, or touching the separate REST-server draft.
- Behavioral tests cover valid principal resolution, non-matches, strict JSON/schema and verifier validation, file safety constraints, and absence of raw bearer values from verifier representation and parse errors.
- `make test`, `make vet`, `make build`, and `git diff --check` completed successfully.
- No Factory CLI interaction was needed for this server-independent library slice; no product usability observation was made.
