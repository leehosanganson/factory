package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const readinessProbeTimeout = 2 * time.Second
const readinessProbeBodyLimit = 4096

func runProbe(args []string, out io.Writer) error {
	if len(args) != 2 || args[0] != "--url" {
		return errors.New("usage: factory probe --url <readyz-url>")
	}
	endpoint, err := url.Parse(args[1])
	if err != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.Path != "/readyz" || endpoint.RawPath != "" {
		return errors.New("probe URL must be an HTTP(S) /readyz URL without credentials or query parameters")
	}
	client := &http.Client{
		Timeout: readinessProbeTimeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return errors.New("readiness probe redirects are not followed")
		},
	}
	request, err := http.NewRequest(http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return errors.New("invalid readiness probe URL")
	}
	response, err := client.Do(request)
	if err != nil {
		return errors.New("readiness probe request failed")
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, readinessProbeBodyLimit+1))
	if err != nil || len(body) > readinessProbeBodyLimit {
		return errors.New("readiness probe response was unavailable or too large")
	}
	var result struct {
		Status string `json:"status"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	if response.StatusCode != http.StatusOK || decoder.Decode(&result) != nil || result.Status != "ready" {
		return fmt.Errorf("readiness probe returned HTTP %d", response.StatusCode)
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("readiness probe response was invalid")
	}
	fmt.Fprintln(out, "ready")
	return nil
}
