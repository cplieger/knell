package notify

import (
	"context"
	"fmt"
	"testing"
)

func TestReadFailureClassifiesCancellation(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("reading rejected response body: %w", context.Canceled)
	if got, want := readFailure(err), "delivery was canceled before the body was complete"; got != want {
		t.Errorf("readFailure(context.Canceled) = %q, want %q", got, want)
	}
}
