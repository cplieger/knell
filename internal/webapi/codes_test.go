package webapi

import (
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cplieger/go/httpserver/v2"
	"github.com/cplieger/go/httpserver/v2/httpservertest"
)

// The package writes exactly codeSpecs plus the httpserver codes it answers
// itself, and every code it writes reaches the client with a remediation.
func TestCodeSpecs_matchSource(t *testing.T) {
	written := maps.Clone(codeSpecs)
	lib := httpserver.CodeSpecs()
	for _, code := range []httpserver.ErrorCode{"method_not_allowed", "too_many_auth_failures"} {
		written[code] = lib[code]
	}
	httpservertest.CheckCodeSpecs(t, ".", written)
	for code := range written {
		if spec, ok := catalog.Lookup(code); !ok || spec.Remediation == "" {
			t.Errorf("catalog has no remediation for %q", code)
		}
	}
}

// A refusal answers the catalog's status and remediation whatever status the
// writer passed, and the host refusal names ALLOWED_HOSTS in its remediation.
func TestErrorsAnswerTheCatalog(t *testing.T) {
	for code, want := range map[httpserver.ErrorCode]int{"unknown_beat": http.StatusNotFound, hostNotAllowed: http.StatusForbidden} {
		rec := httptest.NewRecorder()
		Errors(rec, httptest.NewRequest(http.MethodPost, "/beat/x", nil), http.StatusTeapot, code, "refused")
		var body httpserver.ErrorResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || rec.Code != want || body.Code != code || body.Remediation == "" || body.RequestID == "" {
			t.Errorf("Errors(%s) = %d %s, want %d with a remediation and a request id", code, rec.Code, rec.Body.String(), want)
		}
	}
}
