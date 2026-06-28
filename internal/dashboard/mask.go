package dashboard

import "regexp"

// Sensitive key names whose values are redacted in api-call-log display. The IO logger
// already masks these at storage time (PR4); this is defence in depth so a secret can
// never reach the browser even if an upstream masking gap existed.
var sensitiveKey = regexp.MustCompile(`(?i)("?(?:authorization|api[-_]?key|x-api-key|apikey|secret|api[-_]?secret|signature|sign|token|access[-_]?token|password|x-mexc-apikey|cookie)"?\s*[:=]\s*)("?)([^"&,}\s][^"&,}]*)`)

// maskSecrets redacts the values of known-sensitive keys in a header/body/url string.
func maskSecrets(s string) string {
	if s == "" {
		return s
	}
	return sensitiveKey.ReplaceAllString(s, `${1}${2}***`)
}
