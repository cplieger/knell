package webapi

import (
	"net/http"

	"github.com/cplieger/go/httpserver/v2"
	"github.com/cplieger/go/httpwire"
	"github.com/cplieger/knell/internal/obs"
)

// codeSpecs are the codes knell answers that no library lists;
// httpservertest.CheckCodeSpecs holds them to the source. None of the refused
// requests recorded anything, so each is safe to resend once fixed.
var codeSpecs = map[httpserver.ErrorCode]httpserver.CodeSpec{
	"unauthorized":     {Status: http.StatusUnauthorized, Remediation: "Send a token from BEAT_TOKENS as Authorization: Bearer <token>.", NoEffect: true},
	"beat_not_allowed": {Status: http.StatusForbidden, Remediation: "Send the token whose BEAT_TOKENS line names this beat, or add the beat to that line.", NoEffect: true},
	"unknown_beat":     {Status: http.StatusNotFound, Remediation: "Check the beat id against BEATS, and remove any extra or repeated slash from the URL.", NoEffect: true},
	"shutting_down":    {Status: http.StatusServiceUnavailable, Remediation: "Send the ping again once knell has restarted.", NoEffect: true},
}

const hostNotAllowed httpserver.ErrorCode = "host_not_allowed"

// catalog is every code both listeners answer, with its status and
// remediation. Its codes are program data, so a refusal to add one is a bug
// in this file and stops the process at start.
var catalog = func() *httpwire.Catalog {
	c := httpwire.NewCatalog()
	for code, spec := range codeSpecs {
		if err := c.Add(string(code), spec); err != nil {
			panic(err)
		}
	}
	if err := c.SetRemediation(string(hostNotAllowed), "Add the host name you sent to ALLOWED_HOSTS, or send one it lists."); err != nil {
		panic(err)
	}
	return c
}()

var respond = catalog.Responder()

// Errors writes every refusal of both listeners through the catalog, which
// sets each code's status and remediation. A refused Host is also counted by
// reason.
func Errors(w http.ResponseWriter, r *http.Request, status int, code httpserver.ErrorCode, msg string) {
	if code == hostNotAllowed {
		obs.RecordPreRouteRefusal(obs.RefusalHostNotAllowed)
	}
	respond(w, r, status, code, msg)
}
