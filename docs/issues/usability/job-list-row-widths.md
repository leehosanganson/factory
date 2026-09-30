# Keep default job-list rows within a predictable width

- **Finding:** `factory job list` bounds the description and activity text, but
  the `TARGET` column prints the full path. A long target makes rows much wider
  than the fixed-width columns suggest and pushes the output beyond common
  terminal widths.
- **Evidence:** Running `./bin/factory job list --limit 5` against the local
  store produced 211- and 253-character data rows (the header was 179
  characters). The rows with long paths exceeded the terminal width and the
  neighboring columns remained fixed-width.
- **Desired outcome:** Keep the compact list at a predictable row width without
  hiding target identity; retain a way to inspect the exact path.
- **Acceptance criteria:** Default list output respects a documented maximum
  data-row width for supported fields; long paths are shortened recognizably;
  short paths remain unchanged; an existing detailed command shows the exact
  unabridged target; tests measure row widths, include Unicode paths, and verify
  the full-path escape hatch.
- **Status:** Implemented in compact job-list/get rows: full IDs remain visible,
  while status, description, activity, publication, and target hints are bounded
  to keep data rows within 120 characters. `job get <id> --details` retains the
  full values. Tests cover measured widths, long paths, and details.
