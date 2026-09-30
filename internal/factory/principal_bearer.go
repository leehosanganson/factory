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
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect principal bearer verifier file: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("principal bearer verifier must be a regular, non-symlink file")
	}
	if info.Mode().Perm()&^0o600 != 0 || info.Mode().Perm()&0o400 == 0 {
		return nil, fmt.Errorf("principal bearer verifier file must be owner-readable and inaccessible to group and others")
	}
	if info.Size() > principalBearerConfigLimit {
		return nil, fmt.Errorf("principal bearer verifier file exceeds %d bytes", principalBearerConfigLimit)
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open principal bearer verifier file: %w", err)
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat principal bearer verifier file: %w", err)
	}
	pathInfo, err := os.Lstat(path)
	if err != nil || pathInfo.Mode()&os.ModeSymlink != 0 || !pathInfo.Mode().IsRegular() || !os.SameFile(info, openedInfo) || !os.SameFile(openedInfo, pathInfo) {
		return nil, fmt.Errorf("principal bearer verifier file changed while opening")
	}
	if openedInfo.Mode().Perm()&^0o600 != 0 || openedInfo.Mode().Perm()&0o400 == 0 {
		return nil, fmt.Errorf("principal bearer verifier file must be owner-readable and inaccessible to group and others")
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
	if err := consumeJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("expected one JSON value")
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder) error {
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
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return fmt.Errorf("invalid JSON delimiter")
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
