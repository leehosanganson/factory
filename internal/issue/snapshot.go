// Package issue contains provider-neutral issue snapshot data.
package issue

import "time"

// IssueSnapshot is a provider-neutral view of issue content and state. Version
// is a stable SHA-256 fingerprint of the normalized snapshot and changes when
// any exposed issue field changes. UpdatedAt is the provider's last-update time.
type IssueSnapshot struct {
	Repository string    `json:"repository"`
	Number     int       `json:"number"`
	Title      string    `json:"title"`
	Body       string    `json:"body"`
	State      string    `json:"state"`
	URL        string    `json:"url"`
	UpdatedAt  time.Time `json:"updated_at"`
	Version    string    `json:"version"`
}
