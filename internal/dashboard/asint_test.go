package dashboard

import "testing"

// TestAsIntParsesDriverTypes (PR26 audit) guards the bug fixed in PR25: asInt must parse
// the id/count types the MySQL driver actually returns into interface{} — int64, []byte,
// string, float64 — or session-view counts + the live-audit export correlation silently
// resolve to 0 (losing rows). Mirrors audit requirement 16.
func TestAsIntParsesDriverTypes(t *testing.T) {
	cases := []struct {
		in   any
		want int
	}{
		{int64(42), 42}, {int(42), 42}, {uint64(42), 42}, {float64(42), 42},
		{[]byte("42"), 42}, {"42", 42},
		{nil, 0}, {[]byte("not-a-number"), 0}, {"", 0},
	}
	for _, c := range cases {
		if got := asInt(c.in); got != c.want {
			t.Errorf("asInt(%v [%T]) = %d, want %d", c.in, c.in, got, c.want)
		}
	}
}
