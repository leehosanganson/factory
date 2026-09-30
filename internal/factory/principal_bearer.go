package factory

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
)

const principalBearerConfigLimit = 1 << 20

var principalBearerIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)
var principalBearerDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type principalBearerConfig struct {
	Version    *int                          `json:"version"`
	Principals *[]*principalBearerCredential `json:"principals"`
}

type principalBearerCredential struct {
	ID          *string `json:"id"`
	TokenSHA256 *string `json:"token_sha256"`
}

type principalBearerEntry struct {
	id     string
	digest [sha256.Size]byte
}

// PrincipalBearerVerifier is an immutable set of principal IDs and token digests.
// Raw bearer tokens are never stored in the verifier.
type PrincipalBearerVerifier struct {
	entries []principalBearerEntry
}

// LoadPrincipalBearerVerifier loads a private, version-1 JSON principal verifier file.
// The configuration is fixed for this instance; reload it after changing the file.
func LoadPrincipalBearerVerifier(path string) (*PrincipalBearerVerifier, error) {
	if path == "" {
		return nil, fmt.Errorf("principal bearer verifier path must not be empty")
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open principal bearer verifier file: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat principal bearer verifier file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("principal bearer verifier must be a regular file")
	}
	if info.Mode().Perm()&^0o600 != 0 || info.Mode().Perm()&0o400 == 0 {
		return nil, fmt.Errorf("principal bearer verifier file must be owner-readable and inaccessible to group and others")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return nil, fmt.Errorf("principal bearer verifier file must be owned by the effective user")
	}
	if info.Size() > principalBearerConfigLimit {
		return nil, fmt.Errorf("principal bearer verifier file exceeds %d bytes", principalBearerConfigLimit)
	}

	data, err := io.ReadAll(io.LimitReader(file, principalBearerConfigLimit+1))
	if err != nil {
		return nil, fmt.Errorf("read principal bearer verifier file: %w", err)
	}
	if len(data) > principalBearerConfigLimit {
		return nil, fmt.Errorf("principal bearer verifier file exceeds %d bytes", principalBearerConfigLimit)
	}

	if err := rejectDuplicateJSONFields(data); err != nil {
		return nil, fmt.Errorf("parse principal bearer verifier file: invalid JSON")
	}
	var config principalBearerConfig
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("parse principal bearer verifier file: invalid JSON or schema")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("parse principal bearer verifier file: expected one JSON value")
	}
	if config.Version == nil || *config.Version != 1 || config.Principals == nil || len(*config.Principals) == 0 {
		return nil, fmt.Errorf("principal bearer verifier requires version 1 and at least one principal")
	}

	entries := make([]principalBearerEntry, 0, len(*config.Principals))
	ids := make(map[string]struct{}, len(*config.Principals))
	digests := make(map[string]struct{}, len(*config.Principals))
	for index, credential := range *config.Principals {
		if credential == nil || credential.ID == nil || credential.TokenSHA256 == nil {
			return nil, fmt.Errorf("principal bearer verifier principal %d requires id and token_sha256", index)
		}
		id, digestText := *credential.ID, *credential.TokenSHA256
		if !principalBearerIDPattern.MatchString(id) {
			return nil, fmt.Errorf("principal bearer verifier principal %d has an invalid id", index)
		}
		if !principalBearerDigestPattern.MatchString(digestText) {
			return nil, fmt.Errorf("principal bearer verifier principal %d has an invalid token_sha256", index)
		}
		if _, exists := ids[id]; exists {
			return nil, fmt.Errorf("principal bearer verifier contains duplicate principal IDs")
		}
		if _, exists := digests[digestText]; exists {
			return nil, fmt.Errorf("principal bearer verifier contains duplicate token verifiers")
		}
		ids[id] = struct{}{}
		digests[digestText] = struct{}{}
		decoded, _ := hex.DecodeString(digestText)
		var digest [sha256.Size]byte
		copy(digest[:], decoded)
		entries = append(entries, principalBearerEntry{id: id, digest: digest})
	}
	return &PrincipalBearerVerifier{entries: entries}, nil
}

func rejectDuplicateJSONFields(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := consumeJSONValue(decoder, principalBearerJSONFields("")); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("expected one JSON value")
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder, allowedFields map[string]struct{}) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		keys := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("invalid JSON object key")
			}
			if _, exists := keys[key]; exists {
				return fmt.Errorf("duplicate JSON field")
			}
			keys[key] = struct{}{}
			if allowedFields != nil {
				if _, exists := allowedFields[key]; !exists {
					return fmt.Errorf("unexpected JSON field")
				}
			}
			if err := consumeJSONValue(decoder, principalBearerJSONFields(key)); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder, allowedFields); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return fmt.Errorf("invalid JSON delimiter")
	}
}

func principalBearerJSONFields(parent string) map[string]struct{} {
	switch parent {
	case "":
		return map[string]struct{}{"version": {}, "principals": {}}
	case "principals":
		return map[string]struct{}{"id": {}, "token_sha256": {}}
	default:
		return nil
	}
}

// PrincipalID returns the matched principal ID. Empty and unmatched tokens do not authenticate.
func (v *PrincipalBearerVerifier) PrincipalID(token string) (string, bool) {
	if v == nil || token == "" {
		return "", false
	}
	digest := sha256.Sum256([]byte(token))
	matchedID := ""
	matched := 0
	for _, entry := range v.entries {
		equal := subtle.ConstantTimeCompare(digest[:], entry.digest[:])
		if equal == 1 {
			matchedID = entry.id
		}
		matched |= equal
	}
	return matchedID, matched == 1
}
