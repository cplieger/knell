package config

import (
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// FuzzParseBeats pins the parser's safety invariants: it never panics, and
// every accepted result respects the documented grammar and caps.
//
// The invariants are stated independently of beatIDPattern and minDeadline on
// purpose. They describe what an id and a deadline are USED for — a /beat/{id}
// path element, a Prometheus label, an alert cadence — so widening the
// production pattern or lowering the production constant fails here instead of
// relaxing the assertion along with the code.
func FuzzParseBeats(f *testing.F) {
	f.Add("api:20m")
	f.Add("a:30s,b:26h")
	f.Add("")
	f.Add(",,,")
	f.Add("api")
	f.Add("api:")
	f.Add(":20m")
	f.Add("api:20m,api:20m")
	f.Add("api beat:20m")
	f.Add("api:1ns")
	f.Add("🚨:20m")
	f.Add(strings.Repeat("a", 64) + ":20m")
	overCap := make([]string, 0, maxBeats+1)
	for i := range maxBeats + 1 {
		overCap = append(overCap, "b"+strconv.Itoa(i)+":20m")
	}
	f.Add(strings.Join(overCap, ","))
	f.Fuzz(func(t *testing.T, raw string) {
		beats, err := parseBeats(raw)
		if err != nil {
			return
		}
		if len(beats) == 0 || len(beats) > 64 {
			t.Fatalf("accepted result has %d beats, want 1..64 (the documented cap)", len(beats))
		}
		// Completeness: an accepted BEATS spec must yield one watched beat per
		// non-blank entry. Every other invariant here constrains the beats that
		// come BACK, so a parser that silently discarded an entry would satisfy
		// them all while leaving that sender unwatched for good - no state, no
		// metric series, no deadline, and no notice however long it stays
		// silent. That is the one failure a dead-man switch cannot report on
		// itself.
		configured := 0
		for entry := range strings.SplitSeq(raw, ",") {
			if strings.TrimSpace(entry) != "" {
				configured++
			}
		}
		if len(beats) != configured {
			t.Fatalf("accepted %d beats for %d non-blank entries in %q: an entry that parses but is not returned is a sender nobody watches", len(beats), configured, raw)
		}
		seen := make(map[string]struct{}, len(beats))
		for _, b := range beats {
			if len(b.ID) == 0 || len(b.ID) > 64 {
				t.Fatalf("accepted id %q has byte length %d, want 1..64", b.ID, len(b.ID))
			}
			for i, r := range b.ID {
				alnum := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
				safe := alnum || (i > 0 && (r == '_' || r == '-'))
				if !safe {
					t.Fatalf("accepted id %q: rune at byte %d (%q) is not URL-path and metric-label safe", b.ID, i, r)
				}
			}
			if url.PathEscape(b.ID) != b.ID {
				t.Fatalf("accepted id %q needs escaping to sit in the /beat/{id} route", b.ID)
			}
			if b.Deadline < 30*time.Second {
				t.Fatalf("accepted deadline %s is below the documented 30s floor (a shorter deadline turns sender hiccups into alert spam)", b.Deadline)
			}
			if _, dup := seen[b.ID]; dup {
				t.Fatalf("accepted duplicate id %q (duplicate ids collapse two beats onto one metric series)", b.ID)
			}
			seen[b.ID] = struct{}{}
			if !strings.Contains(raw, b.ID) {
				t.Fatalf("accepted id %q does not appear in the spec %q: an id the parser invented is a beat no sender can ping, so its first sweep declares a phantom outage", b.ID, raw)
			}
		}
	})
}
