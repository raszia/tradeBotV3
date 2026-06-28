// Package secrets implements at-rest encryption for exchange API credentials
// (PR20a). The on-disk/DB format is AES-256-GCM with the layout
//
//	nonce || ciphertext || tag
//
// where the 12-byte GCM nonce is prepended and the 16-byte GCM tag is appended by
// Seal (Go's gcm.Seal returns ciphertext||tag). The AES-256 key is derived from the
// bootstrap master key as SHA-256(master_key_bytes), so the operator's master key
// string need not itself be exactly 32 bytes. Plaintext exists ONLY in memory and is
// never logged, returned, or persisted. No second/incompatible format is introduced.
package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
)

// AlgorithmAESGCM is the only supported encryption_algorithm value (the schema
// default). An empty string is treated as this default.
const AlgorithmAESGCM = "AES-256-GCM"

var (
	// ErrNoMasterKey means the bootstrap master key is empty; credential loading is
	// disabled safely (no panic, no live execution).
	ErrNoMasterKey = errors.New("secrets: master key not configured")
	// ErrUnsupportedAlgorithm means a credential row names an algorithm we do not
	// implement; it is treated as unusable rather than guessed.
	ErrUnsupportedAlgorithm = errors.New("secrets: unsupported encryption algorithm")
	// ErrDecrypt is returned for any decryption failure (wrong key, corrupt blob,
	// truncated nonce). It deliberately carries NO plaintext and NO key material.
	ErrDecrypt = errors.New("secrets: decryption failed")
)

// Cipher encrypts/decrypts credential blobs with a master-key-derived AES-256-GCM key.
type Cipher struct {
	aead cipher.AEAD
}

// NewCipher derives the AES-256 key from the master key and builds the AEAD. An empty
// master key returns ErrNoMasterKey (callers disable credential loading safely); it
// never panics.
func NewCipher(masterKey string) (*Cipher, error) {
	if masterKey == "" {
		return nil, ErrNoMasterKey
	}
	sum := sha256.Sum256([]byte(masterKey)) // 32 bytes -> AES-256
	block, err := aes.NewCipher(sum[:])
	if err != nil {
		return nil, fmt.Errorf("secrets: build cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("secrets: build GCM: %w", err)
	}
	return &Cipher{aead: aead}, nil
}

// SupportedAlgorithm reports whether algo is one this package can decrypt. The empty
// string maps to the schema default (AES-256-GCM).
func SupportedAlgorithm(algo string) bool {
	return algo == "" || algo == AlgorithmAESGCM
}

// Encrypt seals plaintext as nonce || ciphertext || tag with a fresh random nonce.
// Used by tests and (future) credential provisioning tooling — symmetric with Decrypt.
func (c *Cipher) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("secrets: nonce: %w", err)
	}
	// Seal appends ciphertext||tag to its first arg, so prefixing nonce yields the
	// stored layout nonce || ciphertext || tag.
	return c.aead.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt reverses Encrypt. Any failure returns ErrDecrypt with no sensitive detail.
func (c *Cipher) Decrypt(blob []byte) ([]byte, error) {
	ns := c.aead.NonceSize()
	if len(blob) < ns {
		return nil, ErrDecrypt
	}
	nonce, ct := blob[:ns], blob[ns:]
	pt, err := c.aead.Open(nil, nonce, ct, nil)
	if err != nil {
		return nil, ErrDecrypt
	}
	return pt, nil
}
