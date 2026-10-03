# REST config field diagnostics

- **Scope:** Addressed issue #90's sanitized field-specific config diagnostics, separate from PR #98's operator walkthrough. Wrong JSON types for API-key, provider-token, and SQLite path fields now report the field and type problem without echoing the supplied value.
- **Evidence:** Before the change, focused tests failed because `LoadConfig` returned only `invalid JSON schema` for each wrong-typed selected dependency/credential field. After the change, focused tests and `umask 000; make test`, `make vet`, and `make build` passed; `git diff --check` passed.
- **Friction:** The loader used `DisallowUnknownFields` but collapsed all decoding errors into one schema message, losing useful `UnmarshalTypeError.Field` information. Extracting only the field path avoids formatting the underlying error, which may contain operator-supplied values.
