# Gated run list update time

Local `factory run list` had no records, so I could not verify a populated list
interactively. Source inspection showed the list was already ordered by
`UpdatedAt` but did not display that timestamp; a deterministic seeded test now
checks the rendered values and ordering. Existing empty-list output remains
unchanged.
