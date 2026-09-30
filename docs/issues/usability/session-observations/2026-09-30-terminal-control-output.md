# Session observation: persisted control sequences in job rows

- A locally generated job with ANSI CSI escapes in its task description caused
  `factory job list` to emit the raw escape sequences. Source review found an
  existing terminal text sanitizer already used for agent output and log
  summaries.
- Compact description and activity display now pass through that filter; the
  record itself is not modified, and detailed output remains available as
  stored. Focused regressions exercise both CSI description text and OSC
  activity text.
