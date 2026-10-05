# Pipeline-check first-use documentation

- Verified in `config.json.example` that copied configuration includes `pipeline_checks: [["make", "test"]]`; the setup guide previously directed users to choose project-appropriate checks without calling out the shipped value.
- Checking `factory doctor` implementation made it possible to state its limit precisely: it loads configuration, checks required executables on `PATH`, and reports the configured-check count; it does not inspect or execute those commands.
- Improvement: mention example-specific defaults at the copy/setup step and distinguish configuration shape from command validity.
