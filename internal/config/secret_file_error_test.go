package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cplieger/envx"
)

// TestSecretFileErrorNamesEveryFailureClass pins the per-class remedy
// secretFileError exists to give, on the four classes no other test in this
// package reaches. envx classifies every secret-file failure with a sentinel and
// embeds the KEY_FILE VALUE in its own message; that value is the credential
// itself whenever the operator pasted the secret into the file variable instead
// of a path to it, so each branch has to name the operator's next move (fix the
// variable, shrink the file, stop rewriting it, fix the mount) while dropping the
// value. Both halves fail silently: a branch wired to the wrong sentinel sends an
// operator whose mount picked up an archive off to check the path, and a branch
// that wraps envx's error re-copies a live credential into the startup ERROR line
// and from there into the log store, where it outlives the rotation.
//
// The grew-mid-read class is also a race no test can stage deterministically, so
// the sentinel is the only route to it.
func TestSecretFileErrorNamesEveryFailureClass(t *testing.T) {
	t.Parallel()

	// Stands in for the KEY_FILE value envx embeds: a whole webhook URL, which
	// is what this misconfiguration puts there.
	const canary = "https://discord.example/api/webhooks/1/canary-secret"

	tests := map[string]struct {
		err      error
		wantText string
	}{
		// The class an operator actually meets: envx refuses a path that is not
		// already clean or that contains "..", and the credential pasted into
		// the file variable instead of a path to it lands here (an "https://"
		// doubles a separator, so it never survives the clean check). Folding it
		// into the catch-all sends that operator off to check a mount instead of
		// telling them to unset the _FILE variable, and no other assertion in
		// this package reads the message for this class.
		"a path envx refuses": {
			err:      fmt.Errorf("%w: %s", envx.ErrSecretFilePathRejected, canary),
			wantText: "does not name a usable path",
		},
		"already over the size limit": {
			err:      fmt.Errorf("%w (1048577 bytes): %s", envx.ErrSecretFileTooLarge, canary),
			wantText: "larger than the 1 MiB secret-file limit",
		},
		"grew past the limit mid-read": {
			err:      fmt.Errorf("%w: %s", envx.ErrSecretFileGrew, canary),
			wantText: "grew past the 1 MiB secret-file limit",
		},
		"unreadable with no reachable PathError": {
			err:      fmt.Errorf("%w: %s", envx.ErrSecretFileUnreadable, canary),
			wantText: "could not be read: check that the path the variable names exists",
		},
		"a class this envx version does not classify": {
			err:      errors.New("envx: secret file refused for a new reason: " + canary),
			wantText: "could not be read or validated",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := secretFileError("DISCORD_WEBHOOK_URL", tt.err)
			if got == nil {
				t.Fatal("secretFileError returned nil: every envx secret-file failure must fail startup with a message of its own, or the operator is left with no diagnosis at all")
			}
			if !strings.Contains(got.Error(), tt.wantText) {
				t.Errorf("secretFileError = %q, want it to name this class by saying %q: a branch wired to the wrong sentinel sends the operator to the wrong remedy, and nothing else in the process contradicts it", got, tt.wantText)
			}
			if !strings.Contains(got.Error(), "DISCORD_WEBHOOK_URL_FILE") {
				t.Errorf("secretFileError = %q, want it to name DISCORD_WEBHOOK_URL_FILE so the operator knows which variable is broken", got)
			}
			if strings.Contains(got.Error(), canary) {
				t.Errorf("secretFileError = %q embeds the KEY_FILE value; envx puts that value in its own message and this sanitizer exists to drop it, because the value is the credential itself whenever the operator pasted the secret into the file variable", got)
			}
		})
	}

	// The classified-unreadable branch names the failed SYSCALL and the OS
	// reason whenever envx keeps an *os.PathError reachable, and still never the
	// path: that pair is all the operator has left to tell a missing mount from
	// a permission problem.
	t.Run("unreadable with a reachable PathError names the operation", func(t *testing.T) {
		t.Parallel()

		pathErr := &os.PathError{Op: "open", Path: canary, Err: os.ErrPermission}
		got := secretFileError("BEAT_TOKEN", fmt.Errorf("%w: %w", envx.ErrSecretFileUnreadable, pathErr))
		if got == nil {
			t.Fatal("secretFileError returned nil for an unreadable secret file")
		}
		if !strings.Contains(got.Error(), "(open failed)") {
			t.Errorf("secretFileError = %q, want it to name the failed operation: without it a missing mount and a permission problem read identically", got)
		}
		if !strings.Contains(got.Error(), os.ErrPermission.Error()) {
			t.Errorf("secretFileError = %q, want it to carry the OS reason", got)
		}
		if strings.Contains(got.Error(), canary) {
			t.Errorf("secretFileError = %q embeds the path os.PathError carries; that value is the bearer token itself whenever the operator pasted the credential into BEAT_TOKEN_FILE", got)
		}
	})
}

// TestLoadRejectsAnOversizedSecretFile is the end-to-end half of the class
// mapping above for the SIZE class — the one sentinel with no other
// real-file pin (the unreadable and blank classes get theirs from
// TestLoadRejectsUnreadableBeatTokenFile and TestLoadRejectsEmptyBeatTokenFile):
// the table asserts knell's wording per class, but a dependency bump
// that reclassified an oversized file would leave every row green while the
// operator got the catch-all message and was sent to check the path instead of
// the mount. It is also the one size class the filesystem can produce, and the
// shape is a mount pointing at a bundle, an archive or a log rather than a
// secret.
func TestLoadRejectsAnOversizedSecretFile(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "beat-token")
	// One byte past envx's documented 1 MiB secret-file ceiling.
	if err := os.WriteFile(tokenFile, bytes.Repeat([]byte("x"), 1<<20+1), 0o600); err != nil {
		t.Fatal(err)
	}
	setValidLoadEnv(t)
	unsetEnv(t, "BEAT_TOKEN")
	t.Setenv("BEAT_TOKEN_FILE", tokenFile)

	_, err := Load(maxNodeNameBytes)
	if err == nil {
		t.Fatal("Load() with an oversized BEAT_TOKEN_FILE = nil, want error: an unread secret file must fail startup rather than leave the gate unarmed")
	}
	if !strings.Contains(err.Error(), "larger than the 1 MiB secret-file limit") {
		t.Errorf("error = %q, want the too-large diagnosis: it is the only one whose remedy is the mount rather than the file's content", err)
	}
	if !strings.Contains(err.Error(), "BEAT_TOKEN_FILE") {
		t.Errorf("error = %q, want BEAT_TOKEN_FILE named so the operator knows which mount to fix", err)
	}
	if strings.Contains(err.Error(), tokenFile) {
		t.Errorf("error = %q embeds the BEAT_TOKEN_FILE value; that value is the bearer token itself whenever the operator pasted the credential into the file variable, and startup errors are shipped to the log store", err)
	}
}
