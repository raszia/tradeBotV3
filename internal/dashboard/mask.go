package dashboard

import "regexp"

// Sensitive key names whose values are redacted in api-call-log AND app-log display. The
// IO logger already masks these at storage time (PR4), and secrets should never reach
// app_logs; this is defence in depth so a secret can never reach the browser even if an
// upstream masking gap existed.
var sensitiveKey = regexp.MustCompile(`(?i)("?(?:authorization|bearer|api[-_]?key|x-api-key|apikey|client[-_]?secret|api[-_]?secret|secret|signature|sign|access[-_]?token|refresh[-_]?token|token|password|passphrase|x-mexc-apikey|cookie)"?\s*[:=]\s*)("?)([^"&,}\s][^"&,}]*)`)

// maskSecrets redacts the values of known-sensitive keys in a header/body/url string.
func maskSecrets(s string) string {
	if s == "" {
		return s
	}
	return sensitiveKey.ReplaceAllString(s, `${1}${2}***`)
}
