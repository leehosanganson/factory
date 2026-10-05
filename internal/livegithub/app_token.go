package livegithub

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func mintInstallationToken(ctx context.Context, cfg Config) (string, error) {
	return mintInstallationTokenAt(ctx, &http.Client{Transport: newDirectGitHubTransport(), Timeout: 20 * time.Second}, cfg, "https://api.github.com")
}

func mintInstallationTokenAt(ctx context.Context, client *http.Client, cfg Config, apiOrigin string) (string, error) {
	if ctx == nil || client == nil || cfg.AppID <= 0 || cfg.InstallationID <= 0 || cfg.PrivateKey == nil || cfg.Repository == "" {
		return "", errors.New("GitHub App token configuration is invalid")
	}
	jwt, err := appJWT(cfg.AppID, cfg.PrivateKey, time.Now())
	if err != nil {
		return "", errors.New("GitHub App authentication could not be prepared")
	}
	endpoint, err := url.JoinPath(apiOrigin, "app", "installations", strconv.FormatInt(cfg.InstallationID, 10), "access_tokens")
	if err != nil {
		return "", errors.New("GitHub App token endpoint is invalid")
	}
	_, name, _ := strings.Cut(cfg.Repository, "/")
	payload, err := json.Marshal(struct {
		Repositories []string          `json:"repositories"`
		Permissions  map[string]string `json:"permissions"`
	}{Repositories: []string{name}, Permissions: map[string]string{"contents": "write", "pull_requests": "write"}})
	if err != nil {
		return "", errors.New("GitHub App token request could not be encoded")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return "", errors.New("GitHub App token request could not be created")
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	response, err := client.Do(req)
	if err != nil {
		return "", errors.New("GitHub App installation token request failed")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", errors.New("GitHub App installation token request was rejected")
	}
	var result struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result); err != nil || result.Token == "" || strings.ContainsAny(result.Token, " \t\r\n") {
		return "", errors.New("GitHub App installation token response was invalid")
	}
	return result.Token, nil
}

func appJWT(appID int64, privateKey *rsa.PrivateKey, now time.Time) (string, error) {
	if appID <= 0 || privateKey == nil || privateKey.N.BitLen() < 2048 {
		return "", errors.New("invalid GitHub App signing configuration")
	}
	encode := base64.RawURLEncoding.EncodeToString
	header, err := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT"})
	if err != nil {
		return "", err
	}
	claims, err := json.Marshal(map[string]int64{"iat": now.Add(-30 * time.Second).Unix(), "exp": now.Add(8 * time.Minute).Unix(), "iss": appID})
	if err != nil {
		return "", err
	}
	unsigned := encode(header) + "." + encode(claims)
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, digest[:])
	if err != nil {
		return "", errors.New("GitHub App JWT signing failed")
	}
	return unsigned + "." + encode(signature), nil
}
