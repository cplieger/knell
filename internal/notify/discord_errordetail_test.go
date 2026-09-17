package notify

import (
	"strings"
	"testing"
)

// blamedList is the bracketed part of a rendered detail, or the empty string
// when the detail names no blamed field. The caps are declared on that list, so
// measuring the whole detail would charge them for knell's own prefix.
func blamedList(detail string) string {
	open := strings.Index(detail, "[")
	if open < 0 || !strings.HasSuffix(detail, "]") {
		return ""
	}
	return detail[open+1 : len(detail)-1]
}

// TestStatusDetailBoundsAHostileErrorObject drives statusDetail directly so
// each bound fails on its own. The response body is untrusted upstream input
// and its "errors" object is recursive, so a malformed or hostile one must
// produce a bounded, control-character-free line or none at all, never an
// echo of remote bytes and never an unbounded walk.
func TestStatusDetailBoundsAHostileErrorObject(t *testing.T) {
	t.Parallel()

	// A code at the class limit, and one rune past it. The tag keeps the four
	// long codes of the length case distinguishable.
	atLimit := func(tag string) string { return "A_" + strings.Repeat("Z", maxErrorCodeBytes-3) + tag }
	pastLimit := "A_" + strings.Repeat("Z", maxErrorCodeBytes-1)
	blames := func(code string) string { return `{"_errors":[{"code":"` + code + `"}]}` }

	for name, tc := range map[string]struct {
		body    string
		want    []string
		notWant []string
	}{
		// The walk stops before the deepest _errors, so nothing is published
		// from below the cap and the code is still reported.
		"nesting past the depth cap publishes no path": {
			body: `{"code":50035,"errors":{"embeds":{"0":{"fields":{"0":{"name":{"value":` +
				`{"title":{"description":` + blames("BASE_TYPE_REQUIRED") + `}}}}}}}}}`,
			want:    []string{"Discord error code 50035"},
			notWant: []string{"[", "BASE_TYPE_REQUIRED"},
		},
		"more blamed fields than the entry cap ends truncated": {
			body: `{"code":50035,"errors":{` +
				`"color":` + blames("A_B") + `,` +
				`"content":` + blames("A_B") + `,` +
				`"description":` + blames("A_B") + `,` +
				`"name":` + blames("A_B") + `,` +
				`"timestamp":` + blames("A_B") + `,` +
				`"title":` + blames("A_B") + `}}`,
			want:    []string{"Discord error code 50035", "color: A_B", "truncated]"},
			notWant: []string{"timestamp", "title"},
		},
		// Four entries short enough to clear the entry cap but not the length
		// cap, so this case fails only when the length cap is gone.
		"a list past the length cap ends truncated": {
			body: `{"code":50035,"errors":{` +
				`"color":` + blames(atLimit("1")) + `,` +
				`"content":` + blames(atLimit("2")) + `,` +
				`"description":` + blames(atLimit("3")) + `,` +
				`"name":` + blames(atLimit("4")) + `}}`,
			want:    []string{atLimit("1"), "truncated]"},
			notWant: []string{atLimit("4")},
		},
		"a key outside knell's own payload is published as a question mark": {
			body:    `{"code":50035,"errors":{"1234567890abcdef":` + blames("A_B") + `}}`,
			want:    []string{"[?: A_B]"},
			notWant: []string{"1234567890abcdef"},
		},
		"an index too wide for the payload is published as a question mark": {
			body:    `{"code":50035,"errors":{"embeds":{"123":` + blames("A_B") + `}}}`,
			want:    []string{"[embeds.?: A_B]"},
			notWant: []string{"123:"},
		},
		// A code is refused rather than rewritten, so a control character can
		// never reach the line: the path is still published.
		"a code carrying control characters is dropped": {
			body:    `{"code":50035,"errors":{"content":` + blames("BASE\\nTYPE\\r\\u0001") + `}}`,
			want:    []string{"[content]"},
			notWant: []string{"BASE"},
		},
		"a credential-shaped code is dropped": {
			body:    `{"code":50035,"errors":{"content":` + blames("n0tAcode_plainsegment") + `}}`,
			want:    []string{"[content]"},
			notWant: []string{"plainsegment", "n0tAcode"},
		},
		"a code past the code cap is dropped": {
			body:    `{"code":50035,"errors":{"content":` + blames(pastLimit) + `}}`,
			want:    []string{"[content]"},
			notWant: []string{pastLimit},
		},
		// The endpoint answering the POST holds the whole webhook URL, so it is
		// the one party able to echo the path's own id back inside a code that
		// otherwise clears the class. A snowflake is a long digit run.
		"a code carrying a path id is dropped": {
			body:    `{"code":50035,"errors":{"content":` + blames("A_1401234567890123456") + `}}`,
			want:    []string{"[content]"},
			notWant: []string{"1401234567890123456"},
		},
		// The boundary from the other side: a code at the digit-run limit is
		// still published, so the limit cannot be tightened without a red test.
		"a code at the digit-run limit is published": {
			body: `{"code":50035,"errors":{"content":` + blames("BASE_TYPE_MAX_25") + `}}`,
			want: []string{"[content: BASE_TYPE_MAX_25]"},
		},
		"an errors value that is not an object leaves the code alone": {
			body:    `{"code":50035,"errors":"remote prose about the request"}`,
			want:    []string{"Discord error code 50035"},
			notWant: []string{"[", "remote prose"},
		},
		"_errors at the root blames the whole message": {
			body: `{"code":50035,"errors":` + blames("BASE_TYPE_REQUIRED") + `}`,
			want: []string{"[_errors: BASE_TYPE_REQUIRED]"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if len(tc.body) > maxErrorBodyBytes {
				t.Fatalf("setup: the body is %d bytes, past the %d-byte read cap, so it is dropped before the walk and this case asserts nothing", len(tc.body), maxErrorBodyBytes)
			}
			detail := statusDetail(strings.NewReader(tc.body))
			for _, want := range tc.want {
				if !strings.Contains(detail, want) {
					t.Errorf("statusDetail = %q, want it to report %q", detail, want)
				}
			}
			for _, notWant := range tc.notWant {
				if strings.Contains(detail, notWant) {
					t.Errorf("statusDetail = %q, must not report %q", detail, notWant)
				}
			}
			for _, r := range detail {
				if r < 0x20 {
					t.Errorf("statusDetail = %q carries the control character %#U: an upstream string must not be able to forge log structure", detail, r)
				}
			}
			list := blamedList(detail)
			if runes := len([]rune(list)); runes > maxErrorDetailRunes {
				t.Errorf("statusDetail's blamed list is %d characters, want at most %d", runes, maxErrorDetailRunes)
			}
			if list == "" {
				return
			}
			// One entry over the cap is the truncation marker itself.
			if entries := strings.Split(list, "; "); len(entries) > maxErrorFields+1 {
				t.Errorf("statusDetail's blamed list has %d entries (%q), want at most %d plus the truncation marker", len(entries), list, maxErrorFields)
			}
		})
	}
}
