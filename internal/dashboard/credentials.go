package dashboard

// Credential provisioning HTTP layer (PR22). Wraps internal/credentials with the current session
// auth model and the exact-set credential capability (credential_operator or admin).
//
// The safe workflow the endpoints support: create an INACTIVE credential → validate that exact
// credential through a READ-ONLY exchange request → activate it (which atomically disables the
// previously active one) → optionally disable an old credential. The currently working credential
// stays active until the new one is validated AND activated. Secrets are encrypted at rest with the
// master key; no endpoint or audit row ever returns plaintext or an encrypted blob. When no valid
// master key is configured, the WRITE endpoints are disabled (503) — there is no plaintext fallback.

import (
	"net/http"

	"v3TradeBot/internal/credentials"
)

// credWriteReady reports whether credential WRITE operations are available (a valid master key was
// configured). Reads (list/audit — metadata only, no secrets) work regardless.
func (s *Server) credWriteReady(w http.ResponseWriter) bool {
	if s.provisioner == nil || s.credBuilder == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "credential management is disabled: no valid master key is configured"})
		return false
	}
	return true
}

// respondCred maps a provisioner result to an HTTP status (400 validation, 500 otherwise). Real
// errors are logged, never leaked.
func (s *Server) respondCred(w http.ResponseWriter, okBody map[string]any, err error) {
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, okBody)
	case credentials.IsCredentialValidation(err):
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
	default:
		s.log.Warn("credential operation failed", "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "credential operation failed"})
	}
}

// credentialList returns credential METADATA only — never encrypted_* blobs or plaintext.
func (s *Server) credentialList(w http.ResponseWriter, r *http.Request) {
	s.list(w, r,
		"SELECT ec.id, ec.exchange_id, e.code AS exchange_code, ec.label, ec.status, ec.enabled, "+
			"ec.key_version, ec.encryption_algorithm, ec.last_checked_at, ec.last_auth_error, "+
			"ec.created_at, ec.updated_at "+
			"FROM exchange_credentials ec JOIN exchanges e ON e.id = ec.exchange_id "+
			"ORDER BY ec.exchange_id, ec.key_version DESC LIMIT ?",
		s.limit(r))
}

// credentialAudit returns the immutable credential audit trail (no secrets).
func (s *Server) credentialAudit(w http.ResponseWriter, r *http.Request) {
	s.list(w, r,
		"SELECT id, exchange_id, credential_id, operator, action, old_status, new_status, "+
			"old_key_version, new_key_version, reason, created_at FROM credential_audit "+
			"ORDER BY id DESC LIMIT ?",
		s.limit(r))
}

// credentialCreate creates a new INACTIVE, UNVALIDATED credential (the service forces
// enabled=false/status=unknown/key_version). The plaintext secrets are encrypted immediately and
// never stored or returned.
func (s *Server) credentialCreate(w http.ResponseWriter, r *http.Request) {
	if !s.credWriteReady(w) {
		return
	}
	var body struct {
		ExchangeCode string `json:"exchange_code"`
		Label        string `json:"label"`
		Algorithm    string `json:"algorithm"`
		APIKey       string `json:"api_key"`
		APISecret    string `json:"api_secret"`
		Passphrase   string `json:"passphrase"`
		Reason       string `json:"reason"`
	}
	if !decodeStrictJSON(w, r, &body) {
		return
	}
	if !s.validReason(w, body.Reason) {
		return
	}
	op := sessionFrom(r.Context()).Username
	id, err := s.provisioner.Create(r.Context(), credentials.CreateInput{
		ExchangeCode: body.ExchangeCode, Label: body.Label, Algorithm: body.Algorithm,
		APIKey: body.APIKey, APISecret: body.APISecret, Passphrase: body.Passphrase,
		Reason: body.Reason, Operator: op,
	})
	s.respondCred(w, map[string]any{"credential_id": id, "enabled": false, "status": "unknown"}, err)
}

// credentialValidate validates the EXACT selected credential via a read-only exchange request.
func (s *Server) credentialValidate(w http.ResponseWriter, r *http.Request) {
	if !s.credWriteReady(w) {
		return
	}
	id, ok := s.pathID(w, r)
	if !ok {
		return
	}
	op := sessionFrom(r.Context()).Username
	// Build a client bound to THIS credential (decrypts only this row). Read-only path only.
	client, err := s.credBuilder.BuildForCredential(r.Context(), id)
	if err != nil {
		s.respondCred(w, nil, err)
		return
	}
	verr := s.provisioner.ValidateCredential(r.Context(), id, client, op)
	if credentials.IsCredentialValidation(verr) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": verr.Error()})
		return
	}
	// Report the recorded outcome (authoritative from the DB). An auth rejection is a legitimate
	// result (credential now 'invalid'), not a server error.
	var status string
	if e := s.db.QueryRowContext(r.Context(), "SELECT status FROM exchange_credentials WHERE id=?", id).Scan(&status); e != nil {
		s.fail(w, e)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"credential_id": id, "status": status, "validated": verr == nil})
}

// credentialActivateOrRotate makes the validated credential the single active one for its exchange,
// atomically disabling any previously active one.
func (s *Server) credentialActivateOrRotate(w http.ResponseWriter, r *http.Request) {
	if !s.credWriteReady(w) {
		return
	}
	id, ok := s.pathID(w, r)
	if !ok {
		return
	}
	reason, okr := s.reasonBody(w, r)
	if !okr {
		return
	}
	op := sessionFrom(r.Context()).Username
	err := s.provisioner.ActivateOrRotate(r.Context(), id, reason, op)
	s.respondCred(w, map[string]any{"credential_id": id, "status": "active", "enabled": true}, err)
}

// credentialDisable disables a credential (refused for the last usable one under open live risk).
func (s *Server) credentialDisable(w http.ResponseWriter, r *http.Request) {
	if !s.credWriteReady(w) {
		return
	}
	id, ok := s.pathID(w, r)
	if !ok {
		return
	}
	reason, okr := s.reasonBody(w, r)
	if !okr {
		return
	}
	op := sessionFrom(r.Context()).Username
	err := s.provisioner.Disable(r.Context(), id, reason, op)
	s.respondCred(w, map[string]any{"credential_id": id, "status": "disabled", "enabled": false}, err)
}

// reasonBody decodes a {"reason": "..."} body and requires a non-empty reason.
func (s *Server) reasonBody(w http.ResponseWriter, r *http.Request) (string, bool) {
	var body struct {
		Reason string `json:"reason"`
	}
	if !decodeStrictJSON(w, r, &body) {
		return "", false
	}
	if !s.validReason(w, body.Reason) {
		return "", false
	}
	return body.Reason, true
}
