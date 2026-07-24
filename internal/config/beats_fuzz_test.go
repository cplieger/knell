package config

import (
	"strconv"
	"strings"
	"testing"
)

// FuzzParseBeats pins the parser's safety invariants: it never panics, and
// every accepted result respects the documented grammar and caps.
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
	overCap := make([]string, 0, MaxBeats+1)
	for i := range MaxBeats + 1 {
		overCap = append(overCap, "b"+strconv.Itoa(i)+":20m")
	}
	f.Add(strings.Join(overCap, ","))
	f.Fuzz(func(t *testing.T, raw string) {
		beats, err := ParseBeats(raw)
		if err != nil {
			return
		}
		if len(beats) == 0 || len(beats) > MaxBeats {
			t.Fatalf("accepted result has %d beats", len(beats))
		}
		seen := make(map[string]struct{}, len(beats))
		for _, b := range beats {
			if !beatIDPattern.MatchString(b.ID) {
				t.Fatalf("accepted id %q violates grammar", b.ID)
			}
			if b.Deadline < MinDeadline {
				t.Fatalf("accepted deadline %s below minimum", b.Deadline)
			}
			if _, dup := seen[b.ID]; dup {
				t.Fatalf("accepted duplicate id %q", b.ID)
			}
			seen[b.ID] = struct{}{}
		}
	})
}
