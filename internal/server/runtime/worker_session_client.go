package runtime

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	restserver "github.com/leehosanganson/factory/internal/server"
)

const (
	workerSessionHeartbeatInterval = 5 * time.Minute
	workerSessionRequestTimeout    = 10 * time.Second
	workerSessionResponseLimit     = 64 << 10
)

type workerSessionRegistration struct {
	ID                  string     `json:"id"`
	PrincipalID         string     `json:"principal_id"`
	BootID              string     `json:"boot_id"`
	Status              string     `json:"status"`
	CreatedAt           time.Time  `json:"created_at"`
	Capability          string     `json:"capability"`
	CapabilityExpiresAt *time.Time `json:"capability_expires_at,omitempty"`
}

type workerSessionClient struct {
	baseURL string
	client  *http.Client
	token   string
}

func newWorkerSessionClient(config restserver.WorkerControllerConfig) (*workerSessionClient, error) {
	certificate, err := restserver.LoadWorkerClientCertificate(config.ClientCertFile, config.ClientKeyFile)
	if err != nil {
		return nil, errors.New("load worker controller client certificate")
	}
	caPEM, err := os.ReadFile(config.ServerCAFile)
	if err != nil {
		return nil, errors.New("load worker controller server CA")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("worker controller server CA is invalid")
	}
	parsed, err := url.Parse(config.BaseURL)
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return nil, errors.New("worker controller URL is invalid")
	}
	return &workerSessionClient{
		baseURL: strings.TrimRight(parsed.String(), "/"),
		client: &http.Client{
			Transport:     &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}, RootCAs: roots}},
			Timeout:       workerSessionRequestTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}, nil
}

func (c *workerSessionClient) register(ctx context.Context, bootID string) error {
	body, err := json.Marshal(struct {
		BootID string `json:"boot_id"`
	}{BootID: bootID})
	if err != nil {
		return errors.New("encode worker session registration")
	}
	response, cancel, err := c.request(ctx, http.MethodPost, "/v1/worker/session", body, "")
	if err != nil {
		return errors.New("register worker controller session")
	}
	defer cancel()
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
		return errors.New("worker controller rejected session registration")
	}
	var registration workerSessionRegistration
	body, err = io.ReadAll(io.LimitReader(response.Body, workerSessionResponseLimit+1))
	if err != nil || len(body) > workerSessionResponseLimit {
		return errors.New("worker controller registration response is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&registration) != nil || registration.Capability == "" || registration.BootID == "" || registration.BootID != bootID || registration.PrincipalID == "" || registration.ID == "" || registration.Status != "active" || registration.CapabilityExpiresAt == nil || !registration.CapabilityExpiresAt.After(time.Now()) || decoder.Decode(new(any)) != io.EOF {
		return errors.New("worker controller returned an invalid session registration")
	}
	c.token = registration.Capability
	return nil
}

func (c *workerSessionClient) close() {
	if transport, ok := c.client.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
	c.token = ""
}

func (c *workerSessionClient) heartbeat(ctx context.Context) error {
	if c.token == "" {
		return errors.New("worker session is not registered")
	}
	response, cancel, err := c.request(ctx, http.MethodPost, "/v1/worker/session/heartbeat", []byte(`{}`), c.token)
	if err != nil {
		return errors.New("worker controller heartbeat failed")
	}
	defer cancel()
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errors.New("worker controller rejected heartbeat")
	}
	_, err = io.Copy(io.Discard, io.LimitReader(response.Body, workerSessionResponseLimit))
	if err != nil {
		return errors.New("worker controller heartbeat response is invalid")
	}
	return nil
}

func (c *workerSessionClient) request(ctx context.Context, method, path string, body []byte, capability string) (*http.Response, context.CancelFunc, error) {
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, nil, errors.New("build worker controller request")
	}
	request.Header.Set("Content-Type", "application/json")
	if capability != "" {
		request.Header.Set("Authorization", "Bearer "+capability)
	}
	requestCtx, cancel := context.WithTimeout(request.Context(), workerSessionRequestTimeout)
	response, err := c.client.Do(request.WithContext(requestCtx))
	if err != nil {
		cancel()
		return nil, nil, err
	}
	return response, cancel, nil
}

// workerSessionHeartbeat runs until cancellation or an authenticated heartbeat
// failure. It never retires the session because process cancellation does not
// prove that external execution processes have stopped.
func (c *workerSessionClient) workerSessionHeartbeat(ctx context.Context, failed func()) <-chan struct{} {
	return c.runHeartbeats(ctx, workerSessionHeartbeatInterval, failed)
}

func (c *workerSessionClient) runHeartbeats(ctx context.Context, interval time.Duration, failed func()) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := c.heartbeat(ctx); err != nil {
					failed()
					return
				}
			}
		}
	}()
	return done
}
