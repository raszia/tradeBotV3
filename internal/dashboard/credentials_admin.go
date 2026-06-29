package dashboard

import (
	"net/http"

	"v3TradeBot/internal/credentials"
)

// Credential provisioning handlers (PR22). create/rotate/disable require
// credential_operator/admin. The request body carries plaintext secrets ONCE (encrypted
// in memory, never stored/logged/returned); responses return only the credential id +
// status. The audit (credential_audit) holds no secrets.

// credentialAudit returns the credential operation audit history (read-only, no secrets).
func (s *Server) credentialAudit(w http.ResponseWriter, r *http.Request) {
	s.list(w, r, `
SELECT ca.id, e.code AS exchange_code, ca.credential_id, ca.operator, ca.action,
  ca.old_status, ca.new_status, ca.old_key_version, ca.new_key_version, ca.reason, ca.created_at
FROM credential_audit ca JOIN exchanges e ON e.id=ca.exchange_id
ORDER BY ca.id DESC LIMIT ?`, s.limit(r))
}

func (s *Server) createCredential(w http.ResponseWriter, r *http.Request, op operator) {
	if s.provisioner == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "credential provisioning disabled (no master key configured)"})
		return
	}
	var in credentials.CreateInput
	if !decodeBody(w, r, &in) {
		return
	}
	in.Operator = op.name
	id, err := s.provisioner.Create(r.Context(), in)
	if err != nil {
		s.respondCredErr(w, err, "create credential")
		return
	}
	// Return ONLY the new id + status — never the plaintext or the encrypted blob.
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "credential_id": id})
}

func (s *Server) rotateCredential(w http.ResponseWriter, r *http.Request, op operator) {
	if s.provisioner == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "credential provisioning disabled (no master key configured)"})
		return
	}
	var in credentials.RotateInput
	if !decodeBody(w, r, &in) {
		return
	}
	in.Operator = op.name
	id, err := s.provisioner.Rotate(r.Context(), in)
	if err != nil {
		s.respondCredErr(w, err, "rotate credential")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "new_credential_id": id})
}

func (s *Server) disableCredential(w http.ResponseWriter, r *http.Request, op operator) {
	if s.provisioner == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "credential provisioning disabled (no master key configured)"})
		return
	}
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var body struct {
		Reason string `json:"reason"`
	}
	if !decodeBody(w, r, &body) {
		return
	}
	if err := s.provisioner.Disable(r.Context(), id, body.Reason, op.name); err != nil {
		s.respondCredErr(w, err, "disable credential")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// respondCredErr maps a provisioner error: operator-input validation → 400, else 500. The
// error text never contains secret material (the provisioner guarantees that).
func (s *Server) respondCredErr(w http.ResponseWriter, err error, what string) {
	if credentials.IsCredentialValidation(err) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	s.log.Warn(what+" failed", "err", err)
	writeJSON(w, http.StatusInternalServerError, map[string]any{"error": what + " failed"})
}
