package restclient

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/leehosanganson/factory/internal/restjobs"
)

// Client calls only the versioned REST job API.
type Client struct {
	baseURL string
	token   string
	http    *http.Client
}

func New(config Config) *Client {
	return &Client{baseURL: strings.TrimRight(config.BaseURL, "/"), token: config.Token, http: &http.Client{Timeout: 30 * time.Second}}
}

type Submission struct {
	Job      restjobs.Snapshot `json:"job"`
	Replayed bool              `json:"replayed"`
	Links    struct {
		Self    string `json:"self"`
		History string `json:"history"`
	} `json:"links"`
}

func (c *Client) Submit(ctx context.Context, repository, task, idempotencyKey string) (Submission, error) {
	if idempotencyKey == "" {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return Submission{}, errors.New("could not create an idempotency key")
		}
		idempotencyKey = hex.EncodeToString(random[:])
	}
	body, _ := json.Marshal(restjobs.Request{Repository: repository, Task: task})
	var result Submission
	err := c.request(ctx, http.MethodPost, "/v1/jobs", body, http.StatusAccepted, func(response *http.Response) error {
		return json.NewDecoder(response.Body).Decode(&result)
	}, map[string]string{"Content-Type": "application/json", "Idempotency-Key": idempotencyKey})
	return result, err
}

func (c *Client) Get(ctx context.Context, id string) (restjobs.Snapshot, error) {
	var result restjobs.Snapshot
	err := c.request(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(id), nil, http.StatusOK, decodeJSON(&result), nil)
	return result, err
}

func (c *Client) List(ctx context.Context, limit int, after, snapshot uint64) (restjobs.JobPage, error) {
	values := url.Values{}
	if limit > 0 {
		values.Set("limit", strconv.Itoa(limit))
	}
	if after != 0 || snapshot != 0 {
		values.Set("after", strconv.FormatUint(after, 10))
		values.Set("snapshot", strconv.FormatUint(snapshot, 10))
	}
	path := "/v1/jobs"
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var result restjobs.JobPage
	err := c.request(ctx, http.MethodGet, path, nil, http.StatusOK, decodeJSON(&result), nil)
	return result, err
}

func (c *Client) History(ctx context.Context, id string) (restjobs.History, error) {
	var result restjobs.History
	err := c.request(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(id)+"/history", nil, http.StatusOK, decodeJSON(&result), nil)
	return result, err
}

func (c *Client) Operations(ctx context.Context) (restjobs.OperationalSummary, error) {
	var result restjobs.OperationalSummary
	err := c.request(ctx, http.MethodGet, "/v1/operations", nil, http.StatusOK, decodeJSON(&result), nil)
	return result, err
}

func (c *Client) Cancel(ctx context.Context, id string) (restjobs.Snapshot, error) {
	var result restjobs.Snapshot
	err := c.request(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(id)+"/cancel", nil, []int{http.StatusOK, http.StatusAccepted}, decodeJSON(&result), nil)
	return result, err
}

// Disposition selects a non-success outcome for an eligible interrupted SQLite job.
func (c *Client) Disposition(ctx context.Context, id string, disposition restjobs.InterruptedDisposition) (restjobs.Snapshot, error) {
	var result restjobs.Snapshot
	path := "/v1/jobs/" + url.PathEscape(id) + "/disposition/" + url.PathEscape(string(disposition))
	err := c.request(ctx, http.MethodPost, path, nil, http.StatusOK, decodeJSON(&result), nil)
	return result, err
}

// Reconcile requests read-only provider confirmation for an eligible SQLite job.
func (c *Client) Reconcile(ctx context.Context, id string) (restjobs.Snapshot, error) {
	var result restjobs.Snapshot
	err := c.request(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(id)+"/reconcile", nil, http.StatusOK, decodeJSON(&result), nil)
	return result, err
}

// Event is the deliberately small allowlisted payload carried by the versioned SSE route.
type Event struct {
	ID   uint64 `json:"id"`
	Type string `json:"type"`
	At   string `json:"at"`
}

// FollowEvents reads one bounded connection to the existing event route. It
// returns the last event cursor and whether the server requires status/history
// reconciliation before reconnecting without that expired cursor.
func (c *Client) FollowEvents(ctx context.Context, id string, cursor uint64, onEvent func(Event)) (uint64, bool, error) {
	path := "/v1/jobs/" + url.PathEscape(id) + "/events"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return cursor, false, errors.New("invalid REST client request")
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if cursor != 0 {
		req.Header.Set("Last-Event-ID", strconv.FormatUint(cursor, 10))
	}
	response, err := c.http.Do(req)
	if err != nil {
		return cursor, false, errors.New("REST event stream is unavailable or interrupted")
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusConflict {
		return cursor, true, safeHTTPError(response)
	}
	if response.StatusCode != http.StatusOK {
		return cursor, false, safeHTTPError(response)
	}
	reader := bufio.NewScanner(io.LimitReader(response.Body, 1<<20))
	reader.Buffer(make([]byte, 4096), 64<<10)
	var eventID, eventType string
	var data strings.Builder
	cursorExpired := false
	for reader.Scan() {
		line := reader.Text()
		if line == "" {
			if eventID != "" && eventType != "" && data.Len() > 0 {
				sequence, parseErr := strconv.ParseUint(eventID, 10, 64)
				var event Event
				if parseErr != nil || json.Unmarshal([]byte(data.String()), &event) != nil {
					return cursor, false, errors.New("REST event stream returned an invalid event")
				}
				event.ID, event.Type = sequence, eventType
				if sequence > cursor {
					cursor = sequence
					if onEvent != nil {
						onEvent(event)
					}
				}
			}
			eventID, eventType = "", ""
			data.Reset()
			continue
		}
		if strings.HasPrefix(line, "id: ") {
			eventID = strings.TrimPrefix(line, "id: ")
		} else if strings.HasPrefix(line, "event: ") {
			eventType = strings.TrimPrefix(line, "event: ")
			if eventType == "reset" {
				cursorExpired = true
			}
		} else if strings.HasPrefix(line, "data: ") {
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimPrefix(line, "data: "))
		}
	}
	if err := reader.Err(); err != nil {
		return cursor, false, errors.New("REST event stream could not be read")
	}
	if cursorExpired {
		return cursor, true, errors.New("event cursor expired; reconcile status and history before reconnecting")
	}
	return cursor, false, nil
}

func decodeJSON(destination any) func(*http.Response) error {
	return func(response *http.Response) error { return json.NewDecoder(response.Body).Decode(destination) }
}

func (c *Client) request(ctx context.Context, method, path string, body []byte, expected any, decode func(*http.Response) error, headers map[string]string) error {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return errors.New("invalid REST client request")
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	response, err := c.http.Do(req)
	if err != nil {
		return errors.New("REST server is unreachable or the request was interrupted")
	}
	defer response.Body.Close()
	if !statusMatches(response.StatusCode, expected) {
		return safeHTTPError(response)
	}
	if err := decode(response); err != nil {
		return errors.New("REST server returned an invalid response")
	}
	return nil
}

func statusMatches(status int, expected any) bool {
	switch value := expected.(type) {
	case int:
		return status == value
	case []int:
		for _, candidate := range value {
			if status == candidate {
				return true
			}
		}
	}
	return false
}

func safeHTTPError(response *http.Response) error {
	code := ""
	var payload struct {
		Code  string `json:"code"`
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	data, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	if json.Unmarshal(data, &payload) == nil {
		if payload.Code == "" {
			payload.Code = payload.Error.Code
		}
		switch payload.Code {
		case "unauthenticated":
			code = " (check the REST client token configuration)"
		case "not_ready":
			code = " (server is not ready; retry shortly)"
		case "job_not_cancelable":
			code = " (job is not eligible for cancellation)"
		case "not_found":
			code = " (job was not found or is no longer retained)"
		case "cursor_expired":
			code = " (event cursor expired; reconciling status and history)"
		}
	}
	return fmt.Errorf("REST request failed with HTTP %d%s", response.StatusCode, code)
}
