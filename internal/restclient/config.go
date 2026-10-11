package restclient

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// Config contains client-only connection settings. The bearer credential lives
// in a separate private file and is never serialized or included in errors.
type Config struct {
	BaseURL string
	Token   string
}

type configFile struct {
	BaseURL   string `json:"base_url"`
	TokenFile string `json:"token_file"`
}

// DefaultConfigPath locates the REST client configuration independently of the
// server configuration and local detached-job state.
func DefaultConfigPath() string {
	if root := os.Getenv("XDG_CONFIG_HOME"); root != "" {
		return filepath.Join(root, "factory", "rest-client.json")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".config", "factory", "rest-client.json")
}

// LoadConfig reads a strict client-only configuration and a private bearer
// token file. The token file path is relative to the config when not absolute.
func LoadConfig(path string) (Config, error) {
	if path == "" {
		path = DefaultConfigPath()
	}
	if path == "" {
		return Config{}, errors.New("REST client config path is unavailable; set XDG_CONFIG_HOME or pass --config")
	}
	data, err := readPrivateFile(path)
	if err != nil {
		return Config{}, errors.New("REST client config is unavailable or not private")
	}
	var file configFile
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&file); err != nil {
		return Config{}, errors.New("REST client config is invalid")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return Config{}, errors.New("REST client config is invalid")
	}
	parsed, err := url.Parse(file.BaseURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Scheme == "http" && !isLoopbackHost(parsed.Hostname())) || strings.TrimSpace(file.TokenFile) != file.TokenFile || file.TokenFile == "" {
		return Config{}, errors.New("REST client config is invalid")
	}
	tokenPath := file.TokenFile
	if !filepath.IsAbs(tokenPath) {
		tokenPath = filepath.Join(filepath.Dir(path), tokenPath)
	}
	tokenBytes, err := readPrivateFile(tokenPath)
	if err != nil {
		return Config{}, errors.New("REST client token file is unavailable or not private")
	}
	token := strings.TrimSpace(string(tokenBytes))
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return Config{}, errors.New("REST client token file is invalid")
	}
	return Config{BaseURL: strings.TrimRight(file.BaseURL, "/"), Token: token}, nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	addr, err := netip.ParseAddr(host)
	return err == nil && addr.IsLoopback()
}

func readPrivateFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("not a private regular file")
	}
	return os.ReadFile(path)
}
