package livegithub

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	EnvOptIn                = "FACTORY_LIVE_GITHUB_OPT_IN"
	EnvAppID                = "FACTORY_LIVE_GITHUB_APP_ID"
	EnvPrivateKey           = "FACTORY_LIVE_GITHUB_PRIVATE_KEY"
	EnvInstallationID       = "FACTORY_LIVE_GITHUB_INSTALLATION_ID"
	EnvRepository           = "FACTORY_LIVE_GITHUB_REPOSITORY"
	EnvRepositoryAllowlist  = "FACTORY_LIVE_GITHUB_ALLOWED_REPOSITORY"
	EnvExpectedOwner        = "FACTORY_LIVE_GITHUB_OWNER"
	EnvApprovedOrganization = "FACTORY_LIVE_GITHUB_APPROVED_ORGANIZATION"
	EnvExpectedRepository   = "FACTORY_LIVE_GITHUB_REPOSITORY_NAME"
	EnvInstallationToken    = "FACTORY_LIVE_GITHUB_INSTALLATION_TOKEN"

	sandboxRepositorySuffix = "-factory-live-sandbox"
)

type Config struct {
	AppID                int64
	PrivateKey           *rsa.PrivateKey
	InstallationID       int64
	Repository           string
	ApprovedOrganization string
	Token                string
}

type LookupEnv func(string) string

func LoadProviderConfig(getenv LookupEnv) (Config, error) {
	cfg, err := loadRepositoryConfig(getenv)
	if err != nil {
		return Config{}, err
	}
	return attachToken(getenv, cfg)
}

func attachToken(getenv LookupEnv, cfg Config) (Config, error) {
	if getenv == nil {
		return Config{}, errors.New("live GitHub test configuration is unavailable")
	}
	token := getenv(EnvInstallationToken)
	if token == "" || strings.TrimSpace(token) != token || strings.ContainsAny(token, " \t\r\n") {
		return Config{}, errors.New("live GitHub test installation token is missing or invalid")
	}
	cfg.Token = token
	return cfg, nil
}

func LoadTargetConfig(getenv LookupEnv) (Config, error) {
	cfg, err := loadRepositoryConfig(getenv)
	if err != nil {
		return Config{}, err
	}
	appID, err := positiveID(getenv(EnvAppID))
	if err != nil {
		return Config{}, errors.New("live GitHub test App ID is missing or invalid")
	}
	key, err := parsePrivateKey(getenv(EnvPrivateKey))
	if err != nil {
		return Config{}, errors.New("live GitHub test App private key is missing or invalid")
	}
	installationID, err := positiveID(getenv(EnvInstallationID))
	if err != nil {
		return Config{}, errors.New("live GitHub test installation ID is missing or invalid")
	}
	cfg.AppID = appID
	cfg.PrivateKey = key
	cfg.InstallationID = installationID
	return cfg, nil
}

func ValidateTarget(getenv LookupEnv) error {
	_, err := LoadTargetConfig(getenv)
	return err
}

func loadRepositoryConfig(getenv LookupEnv) (Config, error) {
	if getenv == nil || getenv(EnvOptIn) != "true" {
		return Config{}, errors.New("live GitHub test requires explicit opt-in")
	}
	repository, err := validateRepositoryConfig(getenv)
	if err != nil {
		return Config{}, err
	}
	return Config{Repository: repository, ApprovedOrganization: getenv(EnvApprovedOrganization)}, nil
}

func validateRepositoryConfig(getenv LookupEnv) (string, error) {
	repository := getenv(EnvRepository)
	allowed := getenv(EnvRepositoryAllowlist)
	if repository == "" || allowed == "" || repository != allowed {
		return "", errors.New("live GitHub test repository does not match the explicit sandbox allowlist")
	}
	owner, name, ok := strings.Cut(repository, "/")
	if !ok || owner == "" || name == "" || strings.Contains(name, "/") || !strings.HasSuffix(strings.ToLower(name), sandboxRepositorySuffix) {
		return "", errors.New("live GitHub test repository must be an exact *-factory-live-sandbox repository")
	}
	expectedOwner, expectedRepo := getenv(EnvExpectedOwner), getenv(EnvExpectedRepository)
	approvedOrganization := getenv(EnvApprovedOrganization)
	if !validRepositoryComponent(owner) || !validRepositoryComponent(name) || !validRepositoryComponent(approvedOrganization) || owner != expectedOwner || owner != approvedOrganization || name != expectedRepo || !strings.HasSuffix(strings.ToLower(expectedRepo), sandboxRepositorySuffix) {
		return "", errors.New("live GitHub test repository does not match the configured exact sandbox owner/repository")
	}
	return repository, nil
}

func positiveID(value string) (int64, error) {
	id, err := strconv.ParseInt(value, 10, 64)
	if err != nil || id <= 0 || strconv.FormatInt(id, 10) != value {
		return 0, errors.New("invalid numeric ID")
	}
	return id, nil
}

func parsePrivateKey(value string) (*rsa.PrivateKey, error) {
	block, rest := pem.Decode([]byte(value))
	if block == nil || strings.TrimSpace(string(rest)) != "" {
		return nil, errors.New("invalid PEM")
	}
	var key *rsa.PrivateKey
	if block.Type == "RSA PRIVATE KEY" {
		key, _ = x509.ParsePKCS1PrivateKey(block.Bytes)
	} else if block.Type == "PRIVATE KEY" {
		parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err == nil {
			key, _ = parsed.(*rsa.PrivateKey)
		}
	}
	if key == nil || key.N.BitLen() < 2048 {
		return nil, errors.New("invalid RSA private key")
	}
	return key, nil
}

func MaskActionsSecret(secret string) error {
	if secret == "" {
		return errors.New("invalid secret to mask")
	}
	secret = strings.ReplaceAll(secret, "%", "%25")
	secret = strings.ReplaceAll(secret, "\r", "%0D")
	secret = strings.ReplaceAll(secret, "\n", "%0A")
	if _, err := fmt.Fprintf(os.Stdout, "::add-mask::%s\n", secret); err != nil {
		return errors.New("secret masking failed")
	}
	return nil
}

func validRepositoryComponent(value string) bool {
	if value == "." || value == ".." || value == "" {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
			return false
		}
	}
	return true
}
