package secrets

import (
	"bytes"
	"strings"
	"testing"
)

func TestEncryptDecryptRoundTrip(t *testing.T) {
	c, err := NewCipher("a-strong-master-key")
	if err != nil {
		t.Fatal(err)
	}
	plain := []byte("super-secret-api-key-value")
	blob, err := c.Encrypt(plain)
	if err != nil {
		t.Fatal(err)
	}
	// Layout is nonce(12) || ciphertext || tag(16): blob is longer than plaintext by
	// nonce+tag, and never equals the plaintext.
	if len(blob) != len(plain)+c.aead.NonceSize()+c.aead.Overhead() {
		t.Errorf("blob len=%d, want %d", len(blob), len(plain)+c.aead.NonceSize()+c.aead.Overhead())
	}
	if bytes.Contains(blob, plain) {
		t.Error("ciphertext must not contain the plaintext")
	}
	got, err := c.Decrypt(blob)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, plain) {
		t.Errorf("decrypt = %q, want %q", got, plain)
	}
}

func TestWrongKeyFailsSafely(t *testing.T) {
	enc, _ := NewCipher("key-one")
	blob, _ := enc.Encrypt([]byte("topsecret-plaintext"))
	dec, _ := NewCipher("key-two-different")
	got, err := dec.Decrypt(blob)
	if err == nil {
		t.Fatal("decrypt with the wrong key must fail")
	}
	if err != ErrDecrypt {
		t.Errorf("err = %v, want ErrDecrypt", err)
	}
	// The error must not leak any plaintext or key material.
	if strings.Contains(err.Error(), "topsecret") || strings.Contains(err.Error(), "key-one") {
		t.Errorf("error leaked sensitive data: %q", err.Error())
	}
	if got != nil {
		t.Error("no plaintext should be returned on failure")
	}
}

func TestMissingMasterKeyFailsSafely(t *testing.T) {
	if _, err := NewCipher(""); err != ErrNoMasterKey {
		t.Errorf("empty master key err = %v, want ErrNoMasterKey", err)
	}
}

func TestDecryptRejectsTruncatedAndCorrupt(t *testing.T) {
	c, _ := NewCipher("mk")
	if _, err := c.Decrypt([]byte{1, 2, 3}); err != ErrDecrypt { // shorter than a nonce
		t.Errorf("truncated err = %v, want ErrDecrypt", err)
	}
	blob, _ := c.Encrypt([]byte("data"))
	blob[len(blob)-1] ^= 0xff // flip a tag bit
	if _, err := c.Decrypt(blob); err != ErrDecrypt {
		t.Errorf("corrupt-tag err = %v, want ErrDecrypt", err)
	}
}

func TestSupportedAlgorithm(t *testing.T) {
	for _, ok := range []string{"", AlgorithmAESGCM} {
		if !SupportedAlgorithm(ok) {
			t.Errorf("%q should be supported", ok)
		}
	}
	for _, bad := range []string{"AES-128-CBC", "rot13", "PLAIN"} {
		if SupportedAlgorithm(bad) {
			t.Errorf("%q must NOT be supported", bad)
		}
	}
}
