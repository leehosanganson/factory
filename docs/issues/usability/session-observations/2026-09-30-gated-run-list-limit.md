# Session observation: gated-run list limit

- A generated state containing 25 managed gated-run records rendered 26 lines
  in `factory run list`; the local configured store had no gated runs, so the
  large-list reproduction used synthetic managed records.
- The enhancement adds an optional positive-integer limit after all records are
  read and validated, preserving the existing `UpdatedAt`/ID order and
  unbounded default. Help and implementation feature docs now describe the
  option.
