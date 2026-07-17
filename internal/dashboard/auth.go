package dashboard

import (
	"context"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// PR17 dashboard authentication: a small set of trusted internal users with
// username + salted PBKDF2-SHA256 password hash + role, and server-side sessions
// (only the sha256 hash of an opaque random token is stored). Passwords and session
// tokens are NEVER stored in plaintext. Only /healthz and /login are unauthenticated;
// every other route (UI, reads, mutations, WebSocket) requires a valid session.

// Config-ladder roles, lowest→highest CONFIG privilege. A config route requires a MINIMUM role
// via roleAtLeast/requireRole.
const (
	RoleViewer         = "viewer"          // read-only
	RoleConfigOperator = "config_operator" // read + edit normal config
	RoleAdmin          = "admin"           // + high-risk config changes
	// RoleReconcileOperator is an ORTHOGONAL capability, NOT a rung on the config ladder (PR21).
	// It is deliberately ABSENT from roleRank, so roleAtLeast(reconcile_operator, config_operator)
	// is false — a reconcile_operator can NEVER pass a config-edit gate. Reconciliation routes use
	// the exact-set requireReconcileCapable check instead of the ladder.
	RoleReconcileOperator = "reconcile_operator" // resolve NEEDS_RECONCILE cases only (no config edit)
)

// roleRank is the CONFIG ladder ONLY. reconcile_operator is intentionally not present.
var roleRank = map[string]int{RoleViewer: 1, RoleConfigOperator: 2, RoleAdmin: 3}

// validRoles is the full set a dashboard user may hold — decoupled from roleRank so
// reconcile_operator is a creatable role WITHOUT gaining any config-ladder rank.
var validRoles = map[string]bool{
	RoleViewer: true, RoleConfigOperator: true, RoleAdmin: true, RoleReconcileOperator: true,
}

func validRole(r string) bool { return validRoles[r] }

// roleAtLeast reports whether `have` meets the `need` minimum on the CONFIG ladder. A role not on
// the ladder (e.g. reconcile_operator) never meets any minimum.
func roleAtLeast(have, need string) bool {
	h, ok := roleRank[have]
	return ok && h >= roleRank[need]
}

// canReconcile reports whether a role may perform reconciliation. It is an exact-set membership
// test (reconcile_operator OR admin) — never the config ladder — so config_operator/viewer are
// excluded and reconcile_operator never leaks config-edit permission (PR21).
func canReconcile(role string) bool {
	return role == RoleReconcileOperator || role == RoleAdmin
}

const (
	sessionCookie = "v3dash_session"
	pbkdf2Iter    = 210000 // OWASP-range PBKDF2-HMAC-SHA256 work factor
	pbkdf2SaltLen = 16
	pbkdf2KeyLen  = 32
)

var errAuthFailed = errors.New("authentication failed")

// hashPassword returns a self-describing PBKDF2-SHA256 hash string
// ($pbkdf2-sha256$<iter>$<saltHex>$<hashHex>). A per-user random salt is used; the
// plaintext password is never stored.
func hashPassword(pw string) (string, error) {
	if len(pw) < 8 {
		return "", errors.New("password too short (min 8 chars)")
	}
	salt := make([]byte, pbkdf2SaltLen)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return "", err
	}
	key, err := pbkdf2.Key(sha256.New, pw, salt, pbkdf2Iter, pbkdf2KeyLen)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("$pbkdf2-sha256$%d$%s$%s", pbkdf2Iter, hex.EncodeToString(salt), hex.EncodeToString(key)), nil
}

// verifyPassword checks pw against an encoded PBKDF2-SHA256 hash in constant time.
func verifyPassword(encoded, pw string) bool {
	parts := strings.Split(encoded, "$") // ["", "pbkdf2-sha256", iter, saltHex, hashHex]
	if len(parts) != 5 || parts[1] != "pbkdf2-sha256" {
		return false
	}
	iter, err := strconv.Atoi(parts[2])
	if err != nil || iter <= 0 {
		return false
	}
	salt, err := hex.DecodeString(parts[3])
	if err != nil {
		return false
	}
	want, err := hex.DecodeString(parts[4])
	if err != nil || len(want) == 0 {
		return false
	}
	got, err := pbkdf2.Key(sha256.New, pw, salt, iter, len(want))
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare(got, want) == 1
}

// tokenHash is the sha256 hex of a session token (what we store; the token itself is
// only ever held in the client's cookie).
func tokenHash(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// session is the authenticated caller derived from a valid session cookie.
type session struct {
	UserID   int64
	Username string
	Role     string
}

type ctxKey int

const sessionKey ctxKey = 0

func sessionFrom(ctx context.Context) session {
	s, _ := ctx.Value(sessionKey).(session)
	return s
}

// CreateUser inserts a dashboard user with a hashed password. Used to bootstrap the first
// admin (cmd/dashboard -create-user) and by the admin user-management flow. It never stores
// a plaintext password.
func (s *Server) CreateUser(ctx context.Context, username, password, role string) (int64, error) {
	if strings.TrimSpace(username) == "" {
		return 0, errors.New("username is required")
	}
	if !validRole(role) {
		return 0, fmt.Errorf("invalid role %q (want viewer|config_operator|admin)", role)
	}
	ph, err := hashPassword(password)
	if err != nil {
		return 0, err
	}
	res, err := s.db.ExecContext(ctx,
		"INSERT INTO dashboard_users (username, password_hash, role, active) VALUES (?, ?, ?, 1)", username, ph, role)
	if err != nil {
		return 0, err
	}
	id, _ := res.LastInsertId()
	return id, nil
}

// authenticate verifies a username/password against an ENABLED user. A disabled user, an
// unknown user, or a wrong password all return the same errAuthFailed (no enumeration).
func (s *Server) authenticate(ctx context.Context, username, password string) (session, error) {
	var (
		id     int64
		ph     string
		role   string
		active bool
	)
	err := s.db.QueryRowContext(ctx,
		"SELECT id, password_hash, role, active FROM dashboard_users WHERE username=?", username).
		Scan(&id, &ph, &role, &active)
	if errors.Is(err, sql.ErrNoRows) {
		return session{}, errAuthFailed
	}
	if err != nil {
		return session{}, err
	}
	if !active || !verifyPassword(ph, password) {
		return session{}, errAuthFailed
	}
	return session{UserID: id, Username: username, Role: role}, nil
}

// startSession mints an opaque random token, stores only its sha256 hash with the user +
// role snapshot + expiry, stamps last_login_at, and returns the RAW token (for the cookie).
func (s *Server) startSession(ctx context.Context, u session) (string, error) {
	buf := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		return "", err
	}
	raw := hex.EncodeToString(buf)
	if _, err := s.db.ExecContext(ctx, `
INSERT INTO dashboard_sessions (token_hash, user_id, username, role, expires_at)
VALUES (?, ?, ?, ?, NOW(6) + INTERVAL ? SECOND)`,
		tokenHash(raw), u.UserID, u.Username, u.Role, int(s.cfg.SessionTTL.Seconds())); err != nil {
		return "", err
	}
	if _, err := s.db.ExecContext(ctx, "UPDATE dashboard_users SET last_login_at=NOW(6) WHERE id=?", u.UserID); err != nil {
		s.log.Warn("dashboard: last_login_at update failed", "err", err)
	}
	return raw, nil
}

// sessionFromRequest resolves the session cookie to a non-revoked, non-expired session
// belonging to a STILL-ACTIVE user, reading the user's CURRENT role/username by joining
// dashboard_users. Access decisions therefore reflect live user state: disabling a user
// (active=0) immediately breaks their existing sessions (→ 401), and a role change takes
// effect on the next request — no stale session-row snapshot is trusted.
func (s *Server) sessionFromRequest(ctx context.Context, r *http.Request) (session, bool) {
	c, err := r.Cookie(sessionCookie)
	if err != nil || c.Value == "" {
		return session{}, false
	}
	var sess session
	err = s.db.QueryRowContext(ctx, `
SELECT u.id, u.username, u.role
FROM dashboard_sessions s
JOIN dashboard_users u ON u.id = s.user_id
WHERE s.token_hash=? AND s.revoked_at IS NULL AND s.expires_at > NOW(6) AND u.active = 1`, tokenHash(c.Value)).
		Scan(&sess.UserID, &sess.Username, &sess.Role)
	if err != nil {
		return session{}, false
	}
	return sess, true
}

// requireSession wraps a handler so it runs only for an authenticated caller (401
// otherwise), attaching the session to the request context.
func (s *Server) requireSession(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sess, ok := s.sessionFromRequest(r.Context(), r)
		if !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "authentication required"})
			return
		}
		h(w, r.WithContext(context.WithValue(r.Context(), sessionKey, sess)))
	}
}

// requireRole wraps requireSession and additionally enforces a minimum role (403 if the
// authenticated user's role is insufficient).
func (s *Server) requireRole(minRole string, h http.HandlerFunc) http.HandlerFunc {
	return s.requireSession(func(w http.ResponseWriter, r *http.Request) {
		if !roleAtLeast(sessionFrom(r.Context()).Role, minRole) {
			writeJSON(w, http.StatusForbidden, map[string]any{"error": "insufficient role"})
			return
		}
		h(w, r)
	})
}

// requireReconcileCapable wraps requireSession and enforces the EXACT-SET reconciliation
// capability (reconcile_operator OR admin) — NOT the config ladder. Every reconciliation endpoint
// (including read-only ones) uses this, so an unauthenticated caller gets 401 and any role without
// the reconcile capability (viewer, config_operator) gets 403; reconciliation details are never
// returned to an unauthenticated or unauthorized user (PR21).
func (s *Server) requireReconcileCapable(h http.HandlerFunc) http.HandlerFunc {
	return s.requireSession(func(w http.ResponseWriter, r *http.Request) {
		if !canReconcile(sessionFrom(r.Context()).Role) {
			writeJSON(w, http.StatusForbidden, map[string]any{"error": "reconcile capability required"})
			return
		}
		h(w, r)
	})
}

func (s *Server) cookie(name, value string, maxAge int) *http.Cookie {
	return &http.Cookie{
		Name:     name,
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		Secure:   s.cfg.SecureCookies, // set true behind HTTPS
		SameSite: http.SameSiteLaxMode,
		MaxAge:   maxAge,
	}
}

// login authenticates a username/password and starts a session (sets the cookie). Strict
// single-JSON body. Any failure → 401 with a generic message.
func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if !decodeStrictJSON(w, r, &body) {
		return
	}
	sess, err := s.authenticate(r.Context(), body.Username, body.Password)
	if err != nil {
		if !errors.Is(err, errAuthFailed) {
			s.log.Warn("dashboard login error", "err", err)
		}
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "invalid username or password"})
		return
	}
	raw, err := s.startSession(r.Context(), sess)
	if err != nil {
		s.fail(w, err)
		return
	}
	http.SetCookie(w, s.cookie(sessionCookie, raw, int(s.cfg.SessionTTL.Seconds())))
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "username": sess.Username, "role": sess.Role})
}

// logout revokes the current session and clears the cookie.
func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil && c.Value != "" {
		if _, err := s.db.ExecContext(r.Context(),
			"UPDATE dashboard_sessions SET revoked_at=NOW(6) WHERE token_hash=? AND revoked_at IS NULL", tokenHash(c.Value)); err != nil {
			s.log.Warn("dashboard logout revoke failed", "err", err)
		}
	}
	http.SetCookie(w, s.cookie(sessionCookie, "", -1))
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// me returns the authenticated caller's identity (handy for the UI).
func (s *Server) me(w http.ResponseWriter, r *http.Request) {
	sess := sessionFrom(r.Context())
	writeJSON(w, http.StatusOK, map[string]any{"username": sess.Username, "role": sess.Role})
}
