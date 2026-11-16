package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cplieger/go/envx"
)

// TestSecretFileErrorNamesEveryFailureClass pins the remedy secretFileError
// gives per class, on the four classes no other test reaches. envx embeds the
// KEY_FILE value in its message, and that value is the credential when the
// operator pasted the secret instead of a path, so each branch names the next
// move and drops the value. The grew-mid-read class is a race no test stages,
// so its sentinel is the only route to it.
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
			wantText: "could not be read. Check that the path the variable names exists",
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
		got := secretFileError("BEAT_TOKENS", fmt.Errorf("%w: %w", envx.ErrSecretFileUnreadable, pathErr))
		if got == nil {
			t.Fatal("secretFileError returned nil for an unreadable secret file")
		}
		if !strings.Contains(got.Error(), "the open call failed") {
			t.Errorf("secretFileError = %q, want it to name the failed operation: without it a missing mount and a permission problem read identically", got)
		}
		if !strings.Contains(got.Error(), os.ErrPermission.Error()) {
			t.Errorf("secretFileError = %q, want it to carry the OS reason", got)
		}
		if strings.Contains(got.Error(), canary) {
			t.Errorf("secretFileError = %q embeds the path os.PathError carries; that value is the bearer token itself whenever the operator pasted the credential into BEAT_TOKENS_FILE", got)
		}
	})
}

// TestLoadRejectsAnOversizedSecretFile is the real-file pin for the size class,
// the one sentinel no other test reaches through the filesystem: a dependency
// bump that reclassified it would leave the class table green while the
// operator was sent to check the path instead of a mount that picked up an
// archive or a log.
func TestLoadRejectsAnOversizedSecretFile(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "beat-token")
	// One byte past envx's documented 1 MiB secret-file ceiling.
	if err := os.WriteFile(tokenFile, bytes.Repeat([]byte("x"), 1<<20+1), 0o600); err != nil {
		t.Fatal(err)
	}
	setValidLoadEnv(t)
	unsetEnv(t, "BEAT_TOKENS")
	t.Setenv("BEAT_TOKENS_FILE", tokenFile)

	_, err := Load(maxNodeNameBytes)
	if err == nil {
		t.Fatal("Load() with an oversized BEAT_TOKENS_FILE = nil, want error: an unread secret file must fail startup rather than leave the gate unarmed")
	}
	if !strings.Contains(err.Error(), "larger than the 1 MiB secret-file limit") {
		t.Errorf("error = %q, want the too-large diagnosis: it is the only one whose remedy is the mount rather than the file's content", err)
	}
	if !strings.Contains(err.Error(), "BEAT_TOKENS_FILE") {
		t.Errorf("error = %q, want BEAT_TOKENS_FILE named so the operator knows which mount to fix", err)
	}
	if strings.Contains(err.Error(), tokenFile) {
		t.Errorf("error = %q embeds the BEAT_TOKENS_FILE value; that value is the bearer token itself whenever the operator pasted the credential into the file variable, and startup errors are shipped to the log store", err)
	}
}
