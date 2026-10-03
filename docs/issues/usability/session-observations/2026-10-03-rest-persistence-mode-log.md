# REST persistence mode startup log

- Verified the REST runtime can report the selected backend with one concise startup line without exposing configured paths, API keys, provider tokens, or config values.
- Added runtime-buffer coverage for default memory and selected SQLite, plus startup failure before listening and unchanged `/healthz` response.
- No additional usability friction observed in this focused implementation session.
