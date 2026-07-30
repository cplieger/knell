// Package config reads and validates knell's environment configuration.
// All environment reads live here; the rest of the app receives the parsed
// Config. Malformed required values fail startup with a clear error rather
// than falling back: a dead-man watcher silently running with the wrong
// beats or webhook is worse than one that refuses to start.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/cplieger/envx"
	"github.com/cplieger/slogx"
	"github.com/cplieger/webhttp"
)

// maxBeats caps how many beats one instance will watch. The cap keeps the
// metric label space and the notification fan-out operator-bounded; a config
// past it is almost certainly a generator bug.
const maxBeats = 64

// minDeadline is the smallest accepted beat deadline. Anything shorter turns
// transient sender hiccups into alert spam; a sender that beats more often
// than every 30 seconds still works with a longer deadline.
const minDeadline = 30 * time.Second

// minTokenLength is the shortest BEAT_TOKEN knell will start with. The token is
// the ONLY thing standing between a stranger who can reach the port and a
// forged ping, so a value short enough to be guessed is refused rather than
// warned about: a guessed token keeps the switch armed with no real heartbeat
// behind it, which is the one failure this app exists to prevent.
const minTokenLength = 16

// asciiWhitespace is the cutset of edge characters checkBeatToken REFUSES in a
// BEAT_TOKEN. Two different mechanisms reach that one verdict:
//   - SP and HTAB are legal field-value bytes the wire NORMALIZES. The verifier
//     compares "Bearer "+token (see internal/webapi), so a TRAILING run IS
//     stripped from the header value and the sender's value and the verifier's
//     then differ, while a LEADING run is INTERIOR to that value and survives.
//     The leading run is refused anyway, as the ASCII twin of the invisible edge
//     invisibleEdge only warns about: it is part of the credential while being
//     absent from the value the operator reads.
//   - CR, LF, VT and FF are illegal bytes in a field value and are not stripped
//     at all, so no sender can put them on the wire.
//
// Non-ASCII spaces (NBSP U+00A0, NEL U+0085, U+2000…) are deliberately NOT in
// the set: textproto keeps them, so a token made of them IS presented verbatim
// and DOES authenticate. strings.TrimSpace (unicode.IsSpace) would treat them
// as blank, so using it as the refusal test would fail startup on a working
// configuration.
const asciiWhitespace = " \t\r\n\v\f"

// defaultListenAddr is the listener address used when LISTEN_ADDR is unset or
// blank.
const defaultListenAddr = ":9190"

// MaxBeatIDLen is the longest beat id beatIDPattern admits: the pattern is
// built from it (1 leading character + MaxBeatIDLen-1), so the bound lives in
// one place. Exported because a notifier has to render an id of this length
// inside a bounded message.
const MaxBeatIDLen = 64

// beatIDPattern is the accepted beat-id grammar: URL-path and metric-label
// safe, human-readable, bounded by MaxBeatIDLen.
var beatIDPattern = regexp.MustCompile(
	fmt.Sprintf(`^[A-Za-z0-9][A-Za-z0-9_-]{0,%d}$`, MaxBeatIDLen-1))

// Beat is one watched heartbeat: an id senders ping and the silence deadline
// after which the beat is declared missing.
type Beat struct {
	ID       string
	Deadline time.Duration
}

// Config is the fully parsed runtime configuration.
type Config struct {
	// AllowedHosts is the exact-match Host allowlist webapi applies. A nil or
	// inactive policy accepts every Host (the documented default).
	AllowedHosts *webhttp.HostPolicy
	WebhookURL   string
	Node         string
	ListenAddr   string
	BeatToken    string
	Beats        []Beat
	LogLevel     slog.Level
}

// LogValue implements slog.LogValuer so a Config can never publish its own
// secrets. DISCORD_WEBHOOK_URL's path IS the Discord credential and BeatToken
// is the required /beat/{id} gate, so NEITHER is rendered: the webhook is
// reported by PRESENCE only and the token is not reported at all (it is
// required, so its presence is not state). Logging a Config (or a *Config,
// whose method set includes this value receiver) stays leak-free even from a
// call site that logs the whole struct rather than the attributes this method
// itself chooses -- which is what main.go's logConfig publishes today, by
// spreading this value's group.
//
// The VALUE receiver is the load-bearing part: a *Config method set would leave
// the bare Config that Load returns (and run() holds) outside slog.LogValuer, so
// a slog call that logged the whole struct would reflection-render both secrets.
//
//nolint:gocritic // hugeParam: slog.LogValuer must sit on the value receiver so a bare Config redacts too; the copy happens at most once per config log line.
func (c Config) LogValue() slog.Value {
	webhook := "unset"
	if c.WebhookURL != "" {
		webhook = "configured"
	}
	// The Host allowlist is knell's one OPTIONAL gate, and ABSENCE is the state
	// that needs publishing: a present-but-blank ALLOWED_HOSTS warns at parse
	// time, but a MISSPELLED variable name is indistinguishable from unset and
	// draws nothing at all, so without this attribute an operator believes the
	// DNS-rebinding guard is armed and no line contradicts them.
	//
	// The COUNT is state, not decoration: malformed entries are
	// warned-and-dropped, so an active policy can hold zero usable hosts and
	// then rejects every non-loopback request (fail closed) — a distinct and
	// dangerous configuration a bare on/off value would render identically to a
	// working allowlist. Active and Size are nil-safe, so a zero Config (and any
	// Config built without going through Load) renders "any" rather than panicking.
	allowedHosts := "any"
	if c.AllowedHosts.Active() {
		allowedHosts = fmt.Sprintf("allowlist(%d)", c.AllowedHosts.Size())
	}
	return slog.GroupValue(
		slog.Int("beats", len(c.Beats)),
		slog.String("node", c.Node),
		slog.String("listen_addr", c.ListenAddr),
		slog.String("webhook", webhook),
		slog.String("allowed_hosts", allowedHosts),
		slog.String("log_level", c.LogLevel.String()),
	)
}

// Load reads the environment and returns the validated configuration.
// BEATS, DISCORD_WEBHOOK_URL and BEAT_TOKEN are required; everything else has a
// default.
//
// maxNodeNameBytes is the NODE_NAME cap this package ENFORCES; the bound itself
// is owned by the package that renders the notices it is measured over
// (internal/notify's MaxNodeNameBytes), and the composition root passes it in —
// the same mediation main already performs when it translates a config.Beat
// into a watch.Beat. Taking it as a parameter keeps the environment boundary
// free of any dependency on the Discord transport.
func Load(maxNodeNameBytes int) (Config, error) {
	var cfg Config

	rawBeats, err := envx.Require("BEATS")
	if err != nil {
		return cfg, fmt.Errorf("BEATS is required (e.g. \"api:20m,backup:26h\"): %w", err)
	}
	beats, err := parseBeats(rawBeats)
	if err != nil {
		return cfg, fmt.Errorf("parsing BEATS: %w", err)
	}
	cfg.Beats = beats

	webhook, err := loadWebhook()
	if err != nil {
		return cfg, err
	}
	cfg.WebhookURL = webhook

	node, err := nodeName(maxNodeNameBytes)
	if err != nil {
		return cfg, err
	}
	cfg.Node = node

	cfg.ListenAddr = listenAddr()

	cfg.AllowedHosts = allowedHosts()

	beatToken, err := loadBeatToken()
	if err != nil {
		return cfg, err
	}
	cfg.BeatToken = beatToken

	cfg.LogLevel = logLevel()

	return cfg, nil
}

// nodeName resolves the observer name: NODE_NAME when set to a non-blank value
// (a blank one is warned about and ignored), else the hostname, else
// "unknown". A NODE_NAME past maxNodeNameBytes fails startup like any other
// malformed required value: the cap is what guarantees no name can push a
// notification past Discord's content limit, where the switch would arm and
// never ring. The hostname fallback is not length-checked because the kernel
// already bounds it far below the cap (HOST_NAME_MAX is 64 on Linux, 255 by
// POSIX), and refusing to start over the machine's own hostname would trade a
// deliverable notice for no notice at all. That reasoning holds only while
// maxNodeNameBytes stays at or above the OS bound: notify's budget test measures
// the templates at the CAP, so a cap lowered below 255 would leave the DEFAULT
// node name outside anything that was measured (TestNodeNameCapCoversTheHostnameFallback
// pins it).
func nodeName(maxNodeNameBytes int) (string, error) {
	raw, present := os.LookupEnv("NODE_NAME")
	node := strings.TrimSpace(raw)
	// TrimSpace's definition of blank is Unicode SPACES only, so a name built
	// entirely from runes the operator cannot see (a zero-width space, a soft
	// hyphen, a BOM) survives it and names nothing: every notice then reads
	// "[knell ]" and no startup line says the value was useless. invisibleInURL
	// is this package's own wider predicate for "the operator cannot see it"
	// (it refuses such a rune in the webhook URL and warns about one at a
	// token's edge), so both definitions of blank stay one definition and the
	// existing warn-and-fall-back-to-hostname path covers this shape too.
	if strings.TrimFunc(node, invisibleInURL) == "" {
		node = ""
	}
	if node == "" {
		if present {
			// Same rule as listenAddr: an unset NODE_NAME is the documented
			// hostname default, while a blank one is a value the operator set
			// and this process threw away.
			slog.Warn("NODE_NAME is set but blank and was ignored; the node name prefixes every Discord notice, so set it to name this observer, or unset the variable to use the hostname")
		}
		return hostnameNode(), nil
	}
	if len(node) > maxNodeNameBytes {
		return "", fmt.Errorf("NODE_NAME is %d bytes, maximum is %d: the node name prefixes every Discord notification, and the cap keeps every notice far inside Discord's 2000-character content limit (an unbounded name would make Discord reject them all)", len(node), maxNodeNameBytes)
	}
	return node, nil
}

// osHostname is the seam over the one OS call this package cannot reach
// through the environment: every other read is os.LookupEnv, which t.Setenv
// already controls, so without this var the two fallback branches below are
// unreachable from any test. Reassigned by tests in this package only.
var osHostname = os.Hostname

// hostnameNode is the NODE_NAME fallback: the hostname, else "unknown". A
// missing or blank hostname is a warning rather than a startup failure — the
// notices stay deliverable and attributable to something, which beats not
// arming the switch at all.
func hostnameNode() string {
	host, err := osHostname()
	if err != nil {
		slog.Warn("failed to determine hostname, using fallback node name", "node", "unknown", "error", err)
		return "unknown"
	}
	if host = strings.TrimSpace(host); host == "" {
		slog.Warn("hostname is blank, using fallback node name", "node", "unknown")
		return "unknown"
	}
	return host
}

// listenAddr resolves the listener address: LISTEN_ADDR when set to a non-blank
// value (a blank one is warned about and ignored), else defaultListenAddr. A
// padded LISTEN_ADDR copied from a deployment file is not
// a usable address (net.Listen resolves " :9190" as a hostname lookup and
// fails), and the padding is invisible in the resulting crash-loop log line, so
// the padding is trimmed here rather than refused: unlike a credential, an
// address has no verifier on the other side that a trim could disagree with
// (BEAT_TOKEN's padding is REFUSED for exactly that reason; see
// checkBeatToken). A value that is entirely whitespace falls back to the default
// rather than to "", which would bind an ephemeral port and hide the listener
// from scrapes.
func listenAddr() string {
	// LookupEnv, not envx.String: only the PRESENT-but-blank case is an
	// accident worth a line (compose interpolation of an undefined variable
	// produces exactly it), and envx.String collapses that with "unset",
	// which is the documented default case and must stay silent.
	raw, present := os.LookupEnv("LISTEN_ADDR")
	if addr := strings.TrimSpace(raw); addr != "" {
		warnEphemeralListenPort(addr)
		return addr
	}
	if present {
		slog.Warn("LISTEN_ADDR is set but blank and was ignored; the listener binds every interface at the default address, so unset the variable to accept that on purpose, or set a host:port to narrow it", "listen_addr", defaultListenAddr)
	}
	return defaultListenAddr
}

// warnEphemeralListenPort reports a LISTEN_ADDR that asks the kernel for an
// ephemeral port. Port 0 binds successfully and startup reports itself healthy,
// so net.Listen never refuses it and the address main logs is a different
// number on every boot: no sender's POST /beat/{id} URL and no scrape target
// can name it, so every configured beat goes missing one deadline after start
// while the observer looks up. This is the outcome listenAddr already avoids on
// the whitespace-only path ("which would bind an ephemeral port and hide the
// listener from scrapes"); an explicitly configured 0 reaches it unremarked.
// Warned rather than refused: the value is a working bind, so only the
// diagnostic is missing.
func warnEphemeralListenPort(addr string) {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		// Not a host:port at all (a bare "9190", a stray path). net.Listen
		// refuses it at bind time and main's classifyBindError names it, so
		// there is nothing to add here.
		return
	}
	// A service NAME ("http") is resolved by net.Listen, never zero.
	if n, convErr := strconv.Atoi(port); convErr != nil || n != 0 {
		return
	}
	slog.Warn("LISTEN_ADDR asks for port 0, so the kernel picks a fresh random port on every start; no sender can reach POST /beat/{id} and no scrape can reach /metrics at a port that changes each boot, so every configured beat goes missing one deadline after start",
		"hint", "set an explicit port, e.g. "+defaultListenAddr)
}

// ParseAllowedHosts builds the ALLOWED_HOSTS policy SHAPE knell ships:
// loopback-exempt, with the ALLOWED_HOSTS-naming 403 envelope. Exported so
// internal/webapi's tests exercise the shipped shape rather than a hand-copied
// twin that cannot fail when this one changes.
func ParseAllowedHosts(entries []string) (policy *webhttp.HostPolicy, invalid []string) {
	return webhttp.ParseHostList(entries,
		webhttp.WithLoopbackExempt(),
		webhttp.WithHostAllowlistError("host_not_allowed",
			"host not allowed; add it to ALLOWED_HOSTS to serve this hostname"))
}

// allowedHosts parses the ALLOWED_HOSTS exact-match Host allowlist. Unset (the
// documented default) yields an inactive policy that accepts every Host, so the
// guard ships permissive and removes no capability. An active allowlist is what
// breaks DNS rebinding (CWE-346) on the endpoints BEAT_TOKEN does not cover:
// the bearer gate guards /beat/{id} only, so a rebinding page is still free to
// read /metrics — which enumerates every beat and its freshness — under the
// ATTACKER's hostname, and the allowlist is the only check that looks at the
// Host that request carries. Matching is textual on the Host header,
// the one value the attacker cannot forge away, and no name is resolved.
//
// Malformed entries WARN rather than fail startup, unlike the required values
// this package refuses: an allowlist knell cannot use degrades browser access,
// while a startup refusal takes the whole observer down and a dead-man switch
// that does not run detects nothing. ParseHostList still fails CLOSED when every
// entry was unusable (any non-blank entry engages the gate), which the second
// warning names. WithLoopbackExempt keeps an in-container client (a `curl
// http://127.0.0.1:9190/healthz`) working under any allowlist: it admits a
// request only when BOTH the socket peer and the Host are loopback, so a
// rebinding request, which carries the attacker's hostname in Host, never
// qualifies. The baked `knell health` probe needs no exemption at all — it
// stats the marker file and sends no request (see main.go).
func allowedHosts() *webhttp.HostPolicy {
	const key = "ALLOWED_HOSTS"
	// LookupEnv, not Getenv: a PRESENT-but-blank value is the same compose
	// accident listenAddr and nodeName already report, and here it leaves the
	// rebinding guard OFF while the operator believes the allowlist is armed.
	// Unset is the documented default and must stay silent.
	raw, present := os.LookupEnv(key)
	policy, invalid := ParseAllowedHosts(strings.Split(raw, ","))
	if len(invalid) > 0 {
		slog.Warn("dropping malformed "+key+" entries; they cannot match any Host a sender or browser sends",
			"invalid", invalid,
			"hint", "use bare hostnames or IPs only (no scheme, path, or CIDR), e.g. localhost,10.0.0.5,knell.example.com; a lone port like :9190 belongs in LISTEN_ADDR")
	}
	if policy.Active() && policy.Size() == 0 {
		slog.Warn(key+" has no usable entries; rejecting every non-loopback request (fail closed), so no sender can record a beat",
			"hint", "fix the entries listed in the preceding warning, or unset the variable to accept every Host")
	}
	if present && !policy.Active() {
		slog.Warn(key+" is set but blank and was ignored; every Host is accepted, so the DNS-rebinding guard is off",
			"hint", "unset the variable to accept every Host on purpose, or list the hostnames knell is reached by, e.g. knell.internal,10.0.0.5")
	}
	return policy
}

// logLevel resolves the log level: LOG_LEVEL when it parses, else info. A
// present-but-blank value is warned about and ignored, the same accident
// listenAddr and nodeName already report (compose interpolation of an
// undefined variable produces exactly it). It matters most on this variable:
// slogx.ParseLevel returns ok=true for a blank value, so the one knob an
// operator turns while diagnosing a live outage is also the one whose
// silent fallback hides the diagnosis.
func logLevel() slog.Level {
	// LookupEnv, not envx.String, for the reason listenAddr gives: envx.String
	// collapses present-but-blank with unset, and only the former is an
	// accident worth a line.
	raw, present := os.LookupEnv("LOG_LEVEL")
	if present && strings.TrimSpace(raw) == "" {
		slog.Warn("LOG_LEVEL is set but blank and was ignored; logging stays at the default level, so unset the variable to accept that on purpose, or set debug, info, warn or error", "log_level", slog.LevelInfo.String())
		return slog.LevelInfo
	}
	level, ok := slogx.ParseLevel(raw, slog.LevelInfo)
	if !ok {
		slog.Warn("invalid LOG_LEVEL, using info", "value", raw)
	}
	return level
}

// rejectBlankFileVar fails startup when a `_FILE` variable is PRESENT but
// blank. envx gates its file channel on a non-empty value, so an empty
// `_FILE` is indistinguishable from unset and silently falls back to the
// plain variable — which is not the credential the operator pointed knell at,
// and for BEAT_TOKEN would arm the gate for a stale value while a rotated
// secret file sat unread, 401ing every sender. Compose interpolation of an
// unset variable produces exactly this shape. envx.IsBlankSecretFilePath
// reports the state (it owns the `_FILE` suffix and the blank rule, and
// deliberately leaves the verdict to the caller); the refusal and its wording
// are knell's policy and stay here.
func rejectBlankFileVar(key string) error {
	if envx.IsBlankSecretFilePath(key) {
		return fmt.Errorf("%s_FILE is set but empty: unset it to configure %s directly, or point it at a secret file", key, key)
	}
	return nil
}

// secretFileError rewrites an envx secret-file failure into a message that
// names the variable and the failure CLASS but never the operator-supplied
// path. envx embeds the KEY_FILE path in its blank-file, path-policy and
// os.PathError messages — correct when the value is a path, and a leak when it
// is not: the common misconfiguration `DISCORD_WEBHOOK_URL_FILE=https://…/<token>`
// (the credential pasted into the file variable) would otherwise copy that live
// webhook URL into the startup ERROR line and from there into Loki, and
// BEAT_TOKEN_FILE is structurally identical for the bearer token. The original
// error is deliberately NOT wrapped: %w would carry the path through
// Error() anyway.
//
// envx classifies every secret-file failure with a sentinel
// (ErrBlankSecretFile, ErrSecretFilePathRejected, ErrSecretFileTooLarge,
// ErrSecretFileGrew, ErrSecretFileUnreadable), so each class states its own
// remedy — fix the variable, shrink the file, stop rewriting it, fix the mount.
// Naming the class needs
// no error-text matching and no access to the path, which is what makes the
// per-class wording possible without leaking the value. The final branch stays
// as the default for a class a future envx adds.
func secretFileError(key string, err error) error {
	switch {
	case errors.Is(err, envx.ErrBlankSecretFile):
		return fmt.Errorf("%s_FILE points to a blank secret file: point it at a file containing the secret, or unset it to configure %s directly", key, key)
	case errors.Is(err, envx.ErrSecretFilePathRejected):
		// envx refuses a path that is not already clean or that contains "..",
		// and its own message embeds the value — which is exactly the leak this
		// function exists to prevent, because a variable holding the credential
		// instead of a path to it (a webhook URL's "https://" doubles a
		// separator, so it never survives the clean check) lands here.
		return fmt.Errorf("%s_FILE does not name a usable path: it must be an already-clean path with no \"..\" segment, no doubled separator and no trailing slash, e.g. /run/secrets/%s; if the variable holds the secret itself rather than a path to it, unset %s_FILE and configure %s directly", key, strings.ToLower(key), key, key)
	case errors.Is(err, envx.ErrSecretFileTooLarge):
		// 1 MiB is envx's documented secret-file ceiling, restated here because
		// it is not exported. The shape this catches is a mount pointing at the
		// wrong thing entirely — a bundle, an archive, a log — so the remedy is
		// the mount, not the file's content.
		return fmt.Errorf("%s_FILE points to a file larger than the 1 MiB secret-file limit, so it was refused instead of read: point it at a file holding only the secret (a few dozen bytes), not at a bundle, archive or log the mount picked up by mistake", key)
	case errors.Is(err, envx.ErrSecretFileGrew):
		// The file passed the size gate and then grew past it mid-read, so
		// reading on would have handed knell a silently truncated secret. That
		// is a writer problem, not a size problem: the remedy is an atomic
		// write, which also fixes the shorter race of reading a half-written
		// secret.
		return fmt.Errorf("%s_FILE grew past the 1 MiB secret-file limit while it was being read, so the secret would have been silently truncated and every request using it would be rejected: have the writer create the file atomically (write a temporary file, then rename it into place) rather than appending to the mounted one, then restart knell", key)
	case errors.Is(err, envx.ErrSecretFileUnreadable):
		// The operating system refused the open, stat or read. envx keeps the
		// *os.PathError reachable, so the syscall and its reason can be named:
		// pathErr.Err is the bare reason ("no such file or directory",
		// "permission denied"), while pathErr.Path is the operator-supplied
		// value and stays out of the message.
		var pathErr *os.PathError
		if errors.As(err, &pathErr) {
			return fmt.Errorf("%s_FILE could not be read (%s failed): %v: check that the path the variable names exists inside the container and is readable by knell's non-root user", key, pathErr.Op, pathErr.Err)
		}
		return fmt.Errorf("%s_FILE could not be read: check that the path the variable names exists inside the container and is readable by knell's non-root user", key)
	}
	// A failure class this envx version did not have when the branches above
	// were written. The wording names every requirement the classes cover, in
	// the operator's own terms rather than the dependency's, so an unclassified
	// failure still tells the operator what a usable secret file looks like.
	return fmt.Errorf("%s_FILE could not be read or validated: point it at a clean path (no \"..\") naming a readable secret file of at most 1 MiB; a secret file holds a few dozen bytes", key)
}

// warnPlainVarIgnored reports that KEY_FILE supplied the secret while the
// plain KEY was also set, so the plain variable was ignored. envx documents
// this composition as the caller's policy (SecretWithSource reports the
// source on its error paths too); subject names the credential in the
// operator's own vocabulary. Call it only once the secret read SUCCEEDED and
// the value it produced has PASSED validation — on an error path the file
// supplied nothing, so there is no winner to report and the plain variable was
// never consulted, and on a validation failure the advice would describe a
// configuration that never ran. The message is static and the
// variable names ride attributes, so one Loki query covers both credentials
// and can filter on which variable was ignored.
func warnPlainVarIgnored(key, subject string, src envx.SecretSource) {
	if src != envx.SourceFile || os.Getenv(key) == "" {
		return
	}
	slog.Warn("both the plain variable and its _FILE companion are set; the file wins and the plain variable is ignored, so unset it to keep the credential out of the process environment",
		"variable", key, "file_variable", key+"_FILE", "credential", subject)
}

// beatTokenFitsHeader reports whether value can be carried verbatim in an HTTP
// field value, ASSUMING edge ASCII whitespace has already been REFUSED (see
// checkBeatToken and asciiWhitespace). It answers the byte-legality question
// only: a value with a trailing SP or HTAB passes it, yet the wire would strip
// that byte and the exact-match verifier would then reject every ping — so it
// is not a substitute for the padding refusal, it runs after it. HTTP permits
// SP, HTAB, visible ASCII and obs-text (bytes >= 0x80, which is why a
// non-ASCII space token stays legal), but rejects every other ASCII control
// byte and DEL. Go's own HTTP client refuses to write such a value and its
// server rejects a handcrafted one before the handler runs, so a token
// containing one is unpresentable no matter what the sender does.
func beatTokenFitsHeader(value string) bool {
	for i := range len(value) {
		b := value[i]
		if (b < ' ' && b != '\t') || b == 0x7f {
			return false
		}
	}
	return true
}

// invisibleEdge reports whether value begins or ends with a rune the operator
// cannot see. It shares the webhook URL guard's predicate (invisibleInURL)
// because the misconfiguration is the same one: strings.TrimSpace alone covers
// only Unicode SPACES, so a token carrying a zero-width space, a soft hyphen or
// a BOM at an edge reads identical to its visible form and draws no warning at
// all, while arming the gate for a value one rune longer than the one the
// operator reads. The predicate's ASCII space is unreachable from
// checkBeatToken: ASCII edge padding is refused before this runs.
func invisibleEdge(value string) bool {
	return strings.TrimFunc(value, invisibleInURL) != value
}

// errBeatTokenSetButEmpty is the refusal for a BEAT_TOKEN that is present and
// carries no value. Two guards reach the same verdict — loadBeatToken's, because
// envx cannot tell present-but-empty from unset, and checkBeatToken's, which
// stops an empty token from reaching webapi, where it would leave the gate with
// no credential to verify against — so they share one message and cannot come to
// describe the misconfiguration differently.
var errBeatTokenSetButEmpty = fmt.Errorf("BEAT_TOKEN is set but empty: it is the only thing standing between a stranger who can reach this port and a forged ping, so there is no configuration in which knell serves /beat/{id} without it; set it to a random token of at least %d bytes (e.g. `openssl rand -hex 16`), or point BEAT_TOKEN_FILE at a file holding one", minTokenLength)

// checkBeatToken validates a configured BEAT_TOKEN as the exact credential
// senders must present, or refuses it. It never rewrites the value.
//
// Edge ASCII whitespace is REFUSED rather than trimmed; asciiWhitespace holds
// the two wire mechanisms behind that one verdict, and an interior forbidden
// byte is refused separately by beatTokenFitsHeader. In every case the
// configured token is either unsendable as written (a trailing SP or HTAB run,
// or a forbidden byte) or unreadable in the value the operator holds (a leading
// SP or HTAB run), and a dead-man switch should refuse to start rather than
// report itself gated while rejecting every sender: a 401'd ping is an
// undetectable ping, and one deadline later every configured beat goes falsely
// missing.
//
// A token shorter than minTokenLength is refused for the opposite reason: it IS
// presentable, and so is a guess at it. Since the token is the endpoint's only
// gate, a guessable one lets a stranger keep the switch armed with no heartbeat
// behind it.
func checkBeatToken(token string) error {
	if strings.Trim(token, asciiWhitespace) != token {
		// The value is never echoed: the message names the variable and the
		// shape of the problem only (the startup log ships to Loki), and it
		// names the whole CUTSET rather than one member's mechanism, because
		// both mechanisms above reach the same verdict.
		return errors.New("BEAT_TOKEN has leading or trailing ASCII whitespace: a trailing space or tab is stripped from the header value on the wire, and CR, LF, VT and FF cannot be sent in one at all, so such a token never reaches the verifier as configured and POST /beat/{id} would reject every ping while the endpoint reports itself gated; a leading space or tab is refused too, because it authenticates as part of the credential while being invisible in the value you read; knell will not silently rewrite a credential, so remove the surrounding whitespace")
	}
	if token == "" {
		// Nothing to present at all: fails startup like a present-but-empty
		// BEAT_TOKEN and a blank BEAT_TOKEN_FILE. Checked before the length
		// floor below so an empty value gets the message written for it rather
		// than being reported as merely too short.
		return errBeatTokenSetButEmpty
	}
	if !beatTokenFitsHeader(token) {
		// Free of edge padding, yet still unpresentable: a control byte (a
		// pasted newline, a \n that came through a compose value verbatim, a
		// stray second line in a secret file) is illegal in an HTTP field
		// value, so no sender can deliver this token. Refuse at startup rather
		// than arm a gate that 401s every ping and turns every configured beat
		// falsely missing one deadline later. The value is never echoed: the
		// message names the variable and the shape of the problem only.
		//
		// Checked BEFORE the armed-with-an-invisible-value warning below: a
		// value like "\u00a0\n\u00a0" passes the ASCII-edge refusal and reads
		// blank to invisibleInURL, so warning first would log "the gate is
		// armed" for a configuration this very check then refuses to start.
		return fmt.Errorf("BEAT_TOKEN contains a control character that HTTP forbids in a header value, so no sender can present it; use a token of at least %d printable characters", minTokenLength)
	}
	if len(token) < minTokenLength {
		// Presentable, and short enough to be guessed. The gate is the ONLY
		// thing protecting /beat/{id} now, so a guessed token keeps every beat
		// reading fresh while the thing it watches is dead — the one failure
		// this app exists to prevent — and the throttle on failed attempts
		// (see internal/webapi) slows a guessing run without ending it.
		//
		// The exact length is deliberately NOT logged, and neither is the
		// value or its character class: the startup log ships to Loki, whose
		// audience is far wider than the age-encrypted file the token lives in,
		// and each of those facts narrows a guess. The operator set the token,
		// so the minimum is the only number they need.
		return fmt.Errorf("BEAT_TOKEN is shorter than the %d-byte minimum: it is the only gate on /beat/{id}, so a token short enough to guess lets a stranger who can reach this port keep every beat reading fresh while the thing it watches is dead; set a random token of at least %d bytes (e.g. `openssl rand -hex 16`)", minTokenLength, minTokenLength)
	}
	if strings.TrimFunc(token, invisibleInURL) == "" {
		// Invisible end to end by invisibleInURL — all Unicode spaces (NBSP,
		// U+2000...) and every other unprintable rune (a zero-width space, a
		// soft hyphen, a BOM) — yet long enough, free of ASCII edge padding and
		// legal in a header, so every rune survives the header: the token IS
		// presentable, so it is kept verbatim and
		// the gate stays armed. Reachable past the length floor because those
		// runes are multi-byte — eight NBSPs are sixteen bytes — so the floor
		// does not stand in for this warning. Say so, because nothing else in
		// the startup log tells this token apart from one the operator can read,
		// and it is invisible in `docker inspect` output too. The wording
		// deliberately does NOT name the value's character class: the startup
		// log is shipped to Loki, where describing a live credential's alphabet
		// narrows a guess.
		slog.Warn("BEAT_TOKEN is armed with a value that is easy to mistake for absent; the /beat/{id} gate requires it and every sender must present it verbatim, so set a random token of visible characters instead")
	} else if invisibleEdge(token) {
		// Presentable, non-blank, long enough, and carrying an edge rune the
		// operator cannot see. ASCII edge padding was already REFUSED above, so
		// what reaches here is a non-ASCII space, a zero-width space, a soft
		// hyphen or a BOM (the shapes a token pasted out of a rendered page or a
		// word processor carries) that textproto carries verbatim:
		// the gate arms for a value one character longer than the one the
		// operator reads, every sender presenting the visible token gets 401,
		// and one deadline later every configured beat posts a false MISSING
		// notice. Warn rather than refuse, because the value IS presentable and
		// a token containing a non-ASCII space is documented as accepted. As
		// above, the wording never names the character class.
		slog.Warn("BEAT_TOKEN is armed with a value whose first or last character is invisible but part of the credential; every sender must present it verbatim, so retype the token from visible characters")
	}
	return nil
}

// loadBeatToken reads the REQUIRED BEAT_TOKEN bearer gate for POST /beat/{id}.
// It is required because it is the endpoint's only gate: nothing else
// distinguishes a real sender from anything else that can reach the port, so
// there is no configuration in which knell serves the endpoint without it. An
// unset variable, a present-but-empty one, and any value no sender could present
// as configured — one carrying leading or trailing ASCII whitespace, one
// carrying an HTTP-forbidden control byte, or one under the minTokenLength floor
// — all FAIL STARTUP, like an empty BEAT_TOKEN_FILE.
//
// The value is stored EXACTLY as configured, so
// the token knell verifies is the one the operator wrote; nothing is rewritten.
// A file-sourced token arrives as written apart from ONE trailing line ending:
// envx.SecretWithSource removes a single "\n" (or "\r\n") from the file's
// content and returns every other byte verbatim, including edge spaces and
// tabs. So the padding refusal covers the _FILE channel too — a BEAT_TOKEN_FILE
// whose content carries leading or trailing ASCII whitespace FAILS STARTUP,
// exactly as the plain variable does. That is the same principle this package
// applies everywhere else: knell will not silently rewrite a credential.
// The ordinary way of writing such a file is unaffected: `printf '%s\n' token >
// file` and `echo token > file` differ from the token only by that one trailing
// newline, which envx still removes. BEAT_TOKEN_FILE points at a
// mounted secret file instead (the same convention DISCORD_WEBHOOK_URL uses),
// keeping the credential out of `docker inspect` output.
func loadBeatToken() (string, error) {
	if err := rejectBlankFileVar("BEAT_TOKEN"); err != nil {
		return "", err
	}
	token, tokenSrc, err := envx.SecretWithSource("BEAT_TOKEN")
	switch {
	case err == nil:
		// The ignored-plain-variable advisory is deliberately NOT emitted
		// here: the checks below can still fail startup (a control byte in a
		// file-sourced token), and advising the operator to unset a variable
		// in the same breath as exiting on the winning one is advice about a
		// configuration that never ran. It is emitted once the token is fully
		// validated, below.
		if tokenErr := checkBeatToken(token); tokenErr != nil {
			return "", tokenErr
		}
	case errors.As(err, new(*envx.MissingError)):
		// Neither BEAT_TOKEN nor BEAT_TOKEN_FILE is set, or BEAT_TOKEN is
		// present and empty (envx Require cannot tell the two apart, and
		// compose interpolation of an undefined variable produces exactly the
		// empty shape). Both fail startup, and they are reported separately
		// because the operator's next move differs: one has to choose a token,
		// the other already tried to and supplied nothing. Serving the endpoint
		// without a credential is not one of the options: unauthenticated, any
		// client that can reach the port keeps every beat reading fresh, which
		// disarms the switch silently — and a startup failure is the one signal
		// that cannot be mistaken for a working observer.
		if v, ok := os.LookupEnv("BEAT_TOKEN"); ok && v == "" {
			return "", errBeatTokenSetButEmpty
		}
		return "", fmt.Errorf("BEAT_TOKEN is required: it is the only gate on /beat/{id}, so without it any client that can reach this port can keep every beat reading fresh while the thing it watches is dead; set it to a random token of at least %d bytes (e.g. `openssl rand -hex 16`), or point BEAT_TOKEN_FILE at a file holding one: %w", minTokenLength, err)
	default:
		// Any other error means the variable WAS provided and could not be
		// used (unreadable or blank _FILE): fail closed rather than starting
		// without the credential the operator meant to configure. The envx error
		// is sanitized, never wrapped: it embeds the BEAT_TOKEN_FILE value, which
		// is the bearer token itself whenever the operator pasted the
		// credential into the file variable.
		return "", secretFileError("BEAT_TOKEN", err)
	}
	// The token is now fully validated, so the winning-source advisory cannot
	// be followed by a startup failure about the credential it just described.
	warnPlainVarIgnored("BEAT_TOKEN", "token", tokenSrc)
	return token, nil
}

// loadWebhook reads and shape-checks DISCORD_WEBHOOK_URL. The URL is a
// secret: errors never embed it, and only https is accepted — the URL's own
// path carries the webhook credential, so a plain-http webhook would put the
// secret on the wire in cleartext on every notification.
func loadWebhook() (string, error) {
	if err := rejectBlankFileVar("DISCORD_WEBHOOK_URL"); err != nil {
		return "", err
	}
	webhook, src, err := envx.SecretWithSource("DISCORD_WEBHOOK_URL")
	if err != nil {
		if errors.As(err, new(*envx.MissingError)) {
			return "", fmt.Errorf("DISCORD_WEBHOOK_URL is required: %w", err)
		}
		// Provided via _FILE but unreadable/empty: not a missing-variable
		// case, and never a fallback to the plain variable — a webhook the
		// operator meant to configure must fail startup, not go unset. The
		// envx error is sanitized, never wrapped: it embeds the
		// DISCORD_WEBHOOK_URL_FILE value, which is the webhook credential
		// itself whenever the operator pasted the URL into the file variable.
		return "", secretFileError("DISCORD_WEBHOOK_URL", err)
	}

	// Neither channel trims: envx returns the plain variable verbatim and the
	// file's content minus at most one trailing line ending, so a value copied
	// from a deployment file can carry padding through either. A trailing space
	// survives url.Parse and is escaped as %20 on every POST, so Discord answers
	// 404 forever and the switch can never ring. The trim is therefore knell's
	// own, and it applies to both channels. The webhook is trimmed where
	// BEAT_TOKEN is refused because knell is this URL's only sender: there is no
	// second party that must reproduce the value byte for byte.
	webhook = strings.TrimSpace(webhook)
	if webhook == "" {
		// Whitespace-only: the variable WAS provided, so this is a broken
		// secret pipeline, not a missing setting. Reported as such instead
		// of falling through to the shape check, which would answer
		// "scheme must be https" for a value that carries no scheme.
		return "", errors.New("DISCORD_WEBHOOK_URL is set but empty (whitespace only): point it at the https webhook URL, or use DISCORD_WEBHOOK_URL_FILE")
	}
	if _, err := parseWebhookURL(webhook); err != nil {
		return "", fmt.Errorf("DISCORD_WEBHOOK_URL: %w", err)
	}
	// The URL is now fully validated, so the winning-source advisory cannot be
	// followed by a startup failure about the credential it just described --
	// the position loadBeatToken already uses, and the one warnPlainVarIgnored's
	// own contract requires.
	warnPlainVarIgnored("DISCORD_WEBHOOK_URL", "webhook URL", src)
	return webhook, nil
}

// parseBeats parses the BEATS spec list: comma-separated "id:deadline"
// entries, e.g. "watchdog-mimir:20m,watchdog-loki:20m". IDs must match
// [A-Za-z0-9][A-Za-z0-9_-]{0,63} and be unique; deadlines are Go durations
// of at least minDeadline.
func parseBeats(raw string) ([]Beat, error) {
	// Count first, allocate second, and never materialize the split. BEATS is
	// operator-supplied and unbounded, so sizing the containers from its ENTRY
	// count let the value's LENGTH decide the footprint rather than the 64 beats
	// this parser can keep: 1 MiB of separators allocated ~93 MiB, which a
	// memory-limited container OOM-kills before either refusal below can name the
	// cause -- and an OOM leaves the operator a crash loop with no message, the one
	// startup failure this package has no way to explain afterwards. SplitSeq walks
	// the same entries without the intermediate slice, so the count pass is free and
	// both caps are decided before a single allocation.
	configured := 0
	for entry := range strings.SplitSeq(raw, ",") {
		if strings.TrimSpace(entry) != "" {
			configured++
		}
	}
	if configured == 0 {
		return nil, errors.New("no beats configured")
	}
	if configured > maxBeats {
		return nil, fmt.Errorf("%d beats configured, maximum is %d", configured, maxBeats)
	}
	beats := make([]Beat, 0, configured)
	seen := make(map[string]struct{}, configured)
	for entry := range strings.SplitSeq(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		b, err := parseBeatEntry(entry, seen)
		if err != nil {
			return nil, err
		}
		beats = append(beats, b)
	}
	return beats, nil
}

// parseBeatEntry validates one trimmed "id:deadline" entry and records the
// id in seen. Checks run in the documented order: grammar, duplicate,
// duration syntax, minimum deadline.
func parseBeatEntry(entry string, seen map[string]struct{}) (Beat, error) {
	id, rawDeadline, found := strings.Cut(entry, ":")
	if !found {
		return Beat{}, fmt.Errorf("entry %q: expected \"id:deadline\"", entry)
	}
	id = strings.TrimSpace(id)
	if !beatIDPattern.MatchString(id) {
		return Beat{}, fmt.Errorf("entry %q: id must match %s", entry, beatIDPattern)
	}
	if _, dup := seen[id]; dup {
		return Beat{}, fmt.Errorf("entry %q: duplicate id %q", entry, id)
	}
	deadline, err := time.ParseDuration(strings.TrimSpace(rawDeadline))
	if err != nil {
		// The stdlib message names the offending text but not the remedy, and
		// this is the first refusal a new operator meets: BEATS is required, so
		// a unit-less "api:30" and a day-unit "cron-backup:1d" are the two
		// shapes a first BEATS carries. Every other refusal in this package
		// names what to do next; without this one the container crash-loops on
		// stdlib grammar text.
		return Beat{}, fmt.Errorf("entry %q: invalid deadline: %w: use a Go duration "+
			"with an explicit unit (s, m, h), e.g. 30s, 20m or 26h; there is no day "+
			"unit, so a daily job is 26h", entry, err)
	}
	if deadline < minDeadline {
		return Beat{}, fmt.Errorf("entry %q: deadline below minimum %s", entry, minDeadline)
	}
	seen[id] = struct{}{}
	return Beat{ID: id, Deadline: deadline}, nil
}

// invisibleInURL reports whether r is a rune an operator cannot see inside a
// configured webhook URL: the ASCII space, and every rune outside Unicode's
// printable set (the non-ASCII spaces NBSP/U+2000…/U+3000, the zero-width
// space, the soft hyphen, and the format and control categories). url.Parse
// accepts all of them and percent-encodes each one on every request, so the
// host and path that reach the other end are not the configured ones and no
// notification can ever be delivered — while startup succeeds and /healthz
// reports ready. Printable runes are excluded on purpose: they are escaped the
// same way, but the operator can read them, and refusing them would reject a
// working non-Discord relay URL.
func invisibleInURL(r rune) bool {
	return r == ' ' || !unicode.IsPrint(r)
}

// parseWebhookURL validates that raw is an absolute HTTPS URL with a host.
// Errors intentionally exclude operator-supplied text because the URL path
// contains the webhook credential. This is a configuration shape check, not
// an SSRF guard.
func parseWebhookURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		// Deliberately not wrapped: a url.Error embeds the raw URL, and the
		// webhook URL is a secret that must not reach the startup error log.
		return nil, errors.New("not a valid URL")
	}
	if u.Scheme != "https" {
		// Deliberately omits the provided scheme: a malformed secret value
		// like "credentialmaterial:rest" parses with the secret prefix as its
		// scheme, and this error reaches the startup log. Naming only the
		// required scheme keeps the message actionable and leak-free.
		return nil, errors.New("scheme must be https (the webhook URL's own path is the credential, so plain http would send it in cleartext)")
	}
	if u.Hostname() == "" {
		// Hostname(), not Host: an authority that carries only a port
		// ("https://:443/api/webhooks/x/y") has a non-empty Host and would
		// pass, so startup would succeed and the health marker go ready for a
		// webhook that has no destination at all — every notification would
		// then fail as a transport error exactly when the switch must ring.
		return nil, errors.New("missing host")
	}
	if !webhookPortValid(u.Port()) {
		// The number is the operator's own value, not part of the credential,
		// but the message still omits it for the same reason every refusal here
		// does: the startup error ships to Loki, and a fixed constant cannot
		// leak a mis-parsed secret.
		return nil, errors.New("port must be between 1 and 65535")
	}
	if u.Path == "" || u.Path == "/" {
		// The webhook's PATH is the credential, so a URL that carries none is
		// not a webhook: every POST would go to the origin's root and Discord
		// would refuse it forever while startup reported success.
		return nil, errors.New("missing path (the webhook URL's own path carries the credential, so a host-only URL cannot deliver a notification)")
	}
	if !utf8.ValidString(raw) || strings.IndexFunc(raw, invisibleInURL) >= 0 {
		// An interior space survives url.Parse and is percent-encoded on every
		// POST, so the path that reaches Discord is not the path the operator
		// pasted and the switch can never ring. Edge padding is already
		// trimmed by loadWebhook; what is left here is interior, which is the
		// shape a folded YAML scalar produces when it joins a wrapped URL.
		//
		// Every INVISIBLE rune is refused for the same reason, not just the
		// ASCII space: a non-breaking space, an ideographic space, a
		// zero-width space or a soft hyphen (the shapes a URL pasted out of a
		// rendered page or a word processor carries) all survive url.Parse and
		// are percent-encoded on every request exactly like a space, in the
		// host as well as the path. Visible runes are deliberately NOT refused
		// even though they are escaped too: the operator can see them, and
		// refusing them would reject a working non-Discord relay URL.
		//
		// utf8.ValidString carries the same rule down to the BYTE level, and the
		// rune predicate cannot: an invalid UTF-8 byte decodes to U+FFFD, which
		// unicode.IsPrint accepts, so a mis-encoded URL (a secret file written in
		// latin-1, a value mangled by a mis-decoding pipeline) would pass every
		// other gate and be percent-encoded byte by byte on every POST — in the
		// HOST as well as the path, so the notice would go to a name that does
		// not resolve.
		return nil, errors.New("contains a space or an invisible character (it is percent-encoded on every request, so the webhook host and path that reach the other end are not the configured ones; remove it, or percent-encode it yourself if it really belongs to the credential)")
	}
	return u, nil
}

// webhookPortValid reports whether an authority's optional port is in range.
// url.Parse validates a port's SYNTAX (digits only), never its RANGE, so
// "https://discord.example:99999/hook" parses, passes every other gate in
// parseWebhookURL, and starts the process healthy — while net/http refuses the
// address on every POST ("invalid port"), httpx retries the transport failure
// and the sweep retries forever. A permanently undeliverable webhook is the one
// failure a dead-man switch cannot afford, and startup is the only moment the
// operator is watching.
func webhookPortValid(port string) bool {
	if port == "" {
		return true
	}
	n, err := strconv.Atoi(port)
	return err == nil && n >= 1 && n <= 65535
}
