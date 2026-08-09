package notify

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/cplieger/httpx/v4"
)

func TestSafeTransportErrorFailsClosedWhenURLReductionDepthIsExceeded(t *testing.T) {
	t.Parallel()

	const secret = "credential-bearing-location-text"
	cause := errors.New("failed to parse Location header " + secret)
	for hop := range 10 {
		cause = &url.Error{
			Op:  "Post",
			URL: fmt.Sprintf("https://hop-%d.example/%s", hop, secret),
			Err: cause,
		}
	}

	got := safeTransportError(cause)
	if got.Error() != "webhook transport failed" {
		t.Errorf("safeTransportError(deep URL chain).Error() = %q, want the contentless fail-closed phrase", got.Error())
	}
	if errors.Unwrap(got) != nil {
		t.Errorf("safeTransportError(deep URL chain) retained a cause, want fail-closed removal after the reduction cap: %v", errors.Unwrap(got))
	}
	if reduced := httpx.LogSafeError(got); reduced == nil || strings.Contains(reduced.Error(), secret) {
		t.Errorf("httpx.LogSafeError(safeTransportError(deep URL chain)) = %v, want a non-nil error with no remote text", reduced)
	}
}
