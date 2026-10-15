package restserver

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"syscall"
	"unicode"
	"unicode/utf8"
)

const apiKeyFileLimit = 4096
const principalTokenFileLimit = 4096

// APIKey is an immutable in-memory shared API key. Its value is never
// included in formatting or JSON output. Load a new value to apply rotation.
type APIKey struct {
	digest   [sha256.Size]byte
	set      bool
	resolver *PrincipalResolver
}

type principalCredential struct {
	id     string
	role   string
	digest [sha256.Size]byte
}

// PrincipalResolver immutably maps bearer tokens to configured identities and roles.
// It retains only SHA-256 digests, never raw bearer tokens.
type PrincipalResolver struct {
	entries []principalCredential
}

// Principal is the identity and role resolved from a bearer token.
type Principal struct {
	ID   string
	Role string
}

type principalTokenVerifier struct {
	Version    *int                     `json:"version"`
	Principals *[]*principalTokenRecord `json:"principals"`
}

type principalTokenRecord struct {
	ID          *string `json:"id"`
	TokenSHA256 *string `json:"token_sha256"`
}

// LoadAPIKey reads a single shared API key from a protected regular file.
// File updates do not affect an already loaded value; reload or restart to rotate.
func LoadAPIKey(path string) (APIKey, error) {
	if path == "" {
		return APIKey{}, fmt.Errorf("API key file path must not be empty")
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return APIKey{}, fmt.Errorf("open API key file: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return APIKey{}, fmt.Errorf("stat API key file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return APIKey{}, fmt.Errorf("API key file must be a regular file")
	}
	if info.Mode().Perm()&^0o600 != 0 || info.Mode().Perm()&0o400 == 0 {
		return APIKey{}, fmt.Errorf("API key file must be owner-readable and inaccessible to group and others")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return APIKey{}, fmt.Errorf("API key file must be owned by the effective user")
	}
	if info.Size() > apiKeyFileLimit {
		return APIKey{}, fmt.Errorf("API key file exceeds %d bytes", apiKeyFileLimit)
	}
	data, err := io.ReadAll(io.LimitReader(file, apiKeyFileLimit+1))
	if err != nil {
		return APIKey{}, fmt.Errorf("read API key file: %w", err)
	}
	if len(data) > apiKeyFileLimit {
		return APIKey{}, fmt.Errorf("API key file exceeds %d bytes", apiKeyFileLimit)
	}
	if len(data) > 0 && data[len(data)-1] == '\n' {
		data = data[:len(data)-1]
		if len(data) > 0 && data[len(data)-1] == '\r' {
			data = data[:len(data)-1]
		}
	}
	if !validAPIKey(data) {
		return APIKey{}, fmt.Errorf("API key file must contain one nonempty token without whitespace or line breaks")
	}
	return APIKey{digest: sha256.Sum256(data), set: true}, nil
}

type ProviderToken struct{ value string }

func (token ProviderToken) Value() string { return token.value }

func LoadProviderToken(path string) (ProviderToken, error) {
	if path == "" {
		return ProviderToken{}, fmt.Errorf("provider token file path must not be empty")
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return ProviderToken{}, fmt.Errorf("open provider token file: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return ProviderToken{}, fmt.Errorf("stat provider token file: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || info.Mode().Perm()&^0o600 != 0 || info.Mode().Perm()&0o400 == 0 || !ok || stat.Uid != uint32(os.Geteuid()) {
		return ProviderToken{}, errors.New("provider token file must be a private, owner-readable regular file owned by the effective user")
	}
	if info.Size() > apiKeyFileLimit {
		return ProviderToken{}, errors.New("provider token file is too large")
	}
	data, err := io.ReadAll(io.LimitReader(file, apiKeyFileLimit+1))
	if err != nil || len(data) > apiKeyFileLimit {
		return ProviderToken{}, errors.New("read provider token file")
	}
	if info.Size() > int64(apiKeyFileLimit) {
		return ProviderToken{}, errors.New("provider token file is too large")
	}
	if len(data) > 0 && data[len(data)-1] == '\n' {
		data = data[:len(data)-1]
		if len(data) > 0 && data[len(data)-1] == '\r' {
			data = data[:len(data)-1]
		}
	}
	if !validAPIKey(data) {
		return ProviderToken{}, errors.New("provider token file must contain one nonempty token without whitespace")
	}
	return ProviderToken{value: string(data)}, nil
}

func (token ProviderToken) Authenticate(value string) bool {
	if token.value == "" || value == "" {
		return false
	}
	want, got := sha256.Sum256([]byte(token.value)), sha256.Sum256([]byte(value))
	return subtle.ConstantTimeCompare(want[:], got[:]) == 1
}

func (ProviderToken) String() string   { return "[REDACTED]" }
func (ProviderToken) GoString() string { return "[REDACTED]" }

// LoadPrincipalResolver loads named credentials and the legacy shared key into an immutable resolver.
func LoadPrincipalResolver(config Config) (*PrincipalResolver, error) {
	key, err := LoadAPIKey(config.APIKeyFile)
	if err != nil {
		return nil, err
	}
	entries := []principalCredential{{id: "legacy-shared", role: "operator", digest: key.digest}}
	seenDigests := map[[sha256.Size]byte]struct{}{entries[0].digest: {}}
	for _, principal := range config.Principals {
		if !principal.Enabled {
			continue
		}
		digest, err := loadPrincipalTokenDigest(principal.TokenFile, principal.ID)
		if err != nil {
			return nil, fmt.Errorf("principal %q token file is unavailable or invalid", principal.ID)
		}
		if _, exists := seenDigests[digest]; exists {
			return nil, errors.New("principal token verifiers must be unique")
		}
		seenDigests[digest] = struct{}{}
		entries = append(entries, principalCredential{id: principal.ID, role: principal.Role, digest: digest})
	}
	return &PrincipalResolver{entries: entries}, nil
}

func loadPrincipalTokenDigest(path, principalID string) ([sha256.Size]byte, error) {
	var empty [sha256.Size]byte
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return empty, err
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return empty, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || info.Mode().Perm()&^0o600 != 0 || info.Mode().Perm()&0o400 == 0 || !ok || stat.Uid != uint32(os.Geteuid()) {
		return empty, errors.New("unsafe verifier file")
	}
	const verifierLimit = 1 << 20
	if info.Size() > verifierLimit {
		return empty, errors.New("verifier file too large")
	}
	data, err := io.ReadAll(io.LimitReader(file, verifierLimit+1))
	if err != nil || len(data) > verifierLimit {
		return empty, errors.New("verifier file read failed")
	}
	if err := validatePrincipalVerifierJSON(data); err != nil {
		return empty, errors.New("invalid verifier schema")
	}
	var verifier principalTokenVerifier
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&verifier); err != nil || verifier.Version == nil || *verifier.Version != 1 || verifier.Principals == nil || len(*verifier.Principals) == 0 {
		return empty, errors.New("invalid verifier schema")
	}
	ids, digests := map[string]bool{}, map[string]bool{}
	var selected [sha256.Size]byte
	found := false
	for _, record := range *verifier.Principals {
		if record == nil || record.ID == nil || !principalIDPattern.MatchString(*record.ID) || record.TokenSHA256 == nil || !principalBearerDigestPattern.MatchString(*record.TokenSHA256) || ids[*record.ID] || digests[*record.TokenSHA256] {
			return empty, errors.New("invalid verifier principal")
		}
		ids[*record.ID], digests[*record.TokenSHA256] = true, true
		if *record.ID == principalID {
			decoded, _ := hex.DecodeString(*record.TokenSHA256)
			copy(selected[:], decoded)
			found = true
		}
	}
	if !found {
		return empty, errors.New("verifier identity mismatch")
	}
	return selected, nil
}

var principalBearerDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func validatePrincipalVerifierJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var consume func(string) error
	consume = func(parent string) error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok || seen[key] {
					return errors.New("duplicate or invalid field")
				}
				allowed := parent == "" && (key == "version" || key == "principals") || parent == "principal" && (key == "id" || key == "token_sha256")
				if !allowed {
					return errors.New("unknown verifier field")
				}
				seen[key] = true
				child := ""
				if key == "principals" {
					child = "principals"
				}
				if err := consume(child); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		case '[':
			child := ""
			if parent == "principals" {
				child = "principal"
			}
			for decoder.More() {
				if err := consume(child); err != nil {
					return err
				}
			}
			_, err := decoder.Token()
			return err
		default:
			return errors.New("invalid JSON delimiter")
		}
	}
	if err := consume(""); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON value")
	}
	return nil
}

// Resolve returns the configured principal matching token; disabled credentials never resolve.
func (resolver *PrincipalResolver) Resolve(token string) (Principal, bool) {
	if resolver == nil || token == "" {
		return Principal{}, false
	}
	got := sha256.Sum256([]byte(token))
	matchedIndex, matched := 0, 0
	for index, entry := range resolver.entries {
		equal := subtle.ConstantTimeCompare(got[:], entry.digest[:])
		if equal == 1 {
			matchedIndex = index
		}
		matched |= equal
	}
	if matched != 1 {
		return Principal{}, false
	}
	entry := resolver.entries[matchedIndex]
	return Principal{ID: entry.id, Role: entry.role}, true
}

func (resolver *PrincipalResolver) APIKey() APIKey {
	return APIKey{resolver: resolver}
}

// Resolve returns the configured principal matching token. Direct legacy APIKey
// values resolve as the shared operator principal.
func (key APIKey) Resolve(token string) (Principal, bool) {
	if key.resolver != nil {
		return key.resolver.Resolve(token)
	}
	if !key.set || token == "" {
		return Principal{}, false
	}
	got := sha256.Sum256([]byte(token))
	if subtle.ConstantTimeCompare(key.digest[:], got[:]) != 1 {
		return Principal{}, false
	}
	return Principal{ID: "legacy-shared", Role: "operator"}, true
}

// Authenticate reports whether token matches the shared key or a configured principal.
func (key APIKey) Authenticate(token string) bool {
	_, ok := key.Resolve(token)
	return ok
}

// String prevents accidental disclosure through ordinary formatting.
func (APIKey) String() string { return "[REDACTED]" }

// GoString prevents accidental disclosure through Go-syntax formatting.
func (APIKey) GoString() string { return "[REDACTED]" }

func validAPIKey(data []byte) bool {
	if len(data) == 0 || !utf8.Valid(data) {
		return false
	}
	for len(data) > 0 {
		r, size := utf8.DecodeRune(data)
		if r == 0 || unicode.IsSpace(r) {
			return false
		}
		data = data[size:]
	}
	return true
}
