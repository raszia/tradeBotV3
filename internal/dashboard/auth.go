package dashboard

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
)

// Operator roles (PR17). Read-only viewing stays open; MUTATING routes require a
// bearer token mapped to a sufficient role. Roles separate a read-only viewer from a
// config operator (and a credential operator / admin for later operator tooling) — no
// dashboard user is treated as fully trusted.
const (
	RoleViewer             = "viewer"
	RoleConfigOperator     = "config_operator"
	RoleCredentialOperator = "credential_operator"
	RoleAdmin              = "admin"
)

// operator is an authenticated dashboard caller.
type operator struct {
	name string
	role string
}

// canEditConfig reports whether a role may edit operational config (symbol/exchange/
// fee/regime/enable-flags). Credential editing (a later PR) will gate on a separate
// role; an admin can do everything.
func canEditConfig(role string) bool {
	return role == RoleConfigOperator || role == RoleAdmin
}

// authenticate validates the bearer token against dashboard_tokens (matching on the
// SHA-256 hash; the plaintext is never stored or compared in the clear). It returns
// the operator + true on success.
func (s *Server) authenticate(r *http.Request) (operator, bool) {
	tok := bearerToken(r.Header.Get("Authorization"))
	if tok == "" {
		return operator{}, false
	}
	sum := sha256.Sum256([]byte(tok))
	hash := hex.EncodeToString(sum[:])
	var name, role string
	if err := s.db.QueryRowContext(r.Context(),
		"SELECT name, role FROM dashboard_tokens WHERE token_hash=? AND enabled=1", hash).Scan(&name, &role); err != nil {
		return operator{}, false
	}
	// Best-effort touch of last_used_at (never blocks auth on failure).
	_, _ = s.db.ExecContext(context.Background(), "UPDATE dashboard_tokens SET last_used_at=NOW(6) WHERE token_hash=?", hash)
	return operator{name: name, role: role}, true
}

// requireConfigOperator wraps a mutating handler with authentication + authorization:
// 401 when no/invalid token, 403 when the role is insufficient. The authenticated
// operator (for the audit changed_by) is passed to the handler.
func (s *Server) requireConfigOperator(h func(http.ResponseWriter, *http.Request, operator)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		op, ok := s.authenticate(r)
		if !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "authentication required"})
			return
		}
		if !canEditConfig(op.role) {
			writeJSON(w, http.StatusForbidden, map[string]any{"error": "insufficient role for config editing"})
			return
		}
		h(w, r, op)
	}
}

func bearerToken(header string) string {
	const p = "Bearer "
	if strings.HasPrefix(header, p) {
		return strings.TrimSpace(header[len(p):])
	}
	return ""
}
