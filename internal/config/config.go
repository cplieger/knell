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

// maxBeats caps how many beats one instance will watch, keeping the metric
// label space and the notification fan-out operator-bounded.
const maxBeats = 64

// minDeadline is the smallest accepted beat deadline. Anything shorter turns
// transient sender hiccups into alert spam.
const minDeadline = 30 * time.Second

// minTokenLength is the shortest BEAT_TOKEN knell will start with. The token is
// the ONLY thing standing between a stranger who can reach the port and a
// forged ping, so a guessable value is refused rather than warned about.
const minTokenLength = 16

// maxTokenLength exists only because the token has to TRAVEL: every ping carries
// it in a header and MaxRequestHeaderBytes caps the block knell will read, so the
// two bounds are one decision made in one place. With no maximum, a token big
// enough to fill that block -- which a BEAT_TOKEN_FILE pointing at the wrong file
// supplies, envx reading up to 1 MiB -- passes startup and then makes every ping
// fail 431 while the endpoint reports itself gated. 512 bytes is 16x the token
// `openssl rand -hex 16` produces, so the refusal is a margin.
const maxTokenLength = 512

// headerOverheadAllowance is the room MaxRequestHeaderBytes reserves for
// everything that is NOT the beat token: the request line, Host, the "Bearer "
// prefix, and whatever sits in front of knell adds. 8 KiB matches the default
// PER-LINE ceiling of the servers likely to sit in front (nginx's 8k buffer,
// Apache's LimitRequestFieldSize 8190). It is headroom ON TOP of maxTokenLength
// because a ceiling EQUAL to the token maximum would 431 a maximum-length token
// the moment it is sent with anything else.
const headerOverheadAllowance = 8 << 10

// MaxRequestHeaderBytes is the request-header cap the composition root applies
// to the HTTP server, derived from the token bound so the two cannot drift: a
// token checkBeatToken accepted always fits. net/http's own default is 1 MiB,
// which lets one unauthenticated caller make this process read a megabyte of
// header per connection.
const MaxRequestHeaderBytes = maxTokenLength + headerOverheadAllowance

// asciiWhitespace is the cutset of edge characters checkBeatToken REFUSES in a
// BEAT_TOKEN. Two mechanisms reach that one verdict: SP and HTAB are legal
// field-value bytes the wire NORMALIZES, so a TRAILING run is stripped and the
// verifier's value then differs from the sender's (a LEADING run survives, and is
// refused for being part of the credential while absent from the value the
// operator reads); CR, LF, VT and FF are illegal in a field value. Non-ASCII
// spaces are NOT in the set: textproto keeps them, so such a token DOES
// authenticate and TrimSpace would refuse a working configuration.
const asciiWhitespace = " \t\r\n\v\f"

// defaultListenAddr is used when LISTEN_ADDR is unset or blank.
const defaultListenAddr = ":9190"

// MaxBeatIDLen is the longest beat id beatIDPattern admits; the pattern is
// built from it, so the bound lives in one place. Exported because a notifier
// has to render an id of this length inside a bounded message.
const MaxBeatIDLen = 64

// beatIDPattern is the accepted beat-id grammar: URL-path and metric-label
// safe, human-readable, bounded by MaxBeatIDLen.
var beatIDPattern = regexp.MustCompile(
	fmt.Sprintf(`^[A-Za-z0-9][A-Za-z0-9_-]{0,%d}$`, MaxBeatIDLen-1),
)

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
	// TrustedProxies is the reverse-proxy CIDR set X-Forwarded-For is honored
	// for. Empty (the default) keeps webhttp's spoof-proof behaviour: the
	// access line's client_ip is the socket peer.
	TrustedProxies []*net.IPNet
	WebhookURL     string
	// WebhookSource is the channel DISCORD_WEBHOOK_URL arrived through, as envx
	// names it: the one non-secret FACT about the credential worth publishing at
	// startup, since the plain variable sits in `docker inspect` output.
	WebhookSource envx.SecretSource
	Node          string
	ListenAddr    string
	BeatToken     string
	// BeatTokenSource is the channel BEAT_TOKEN arrived through, carried for the
	// same reason. warnPlainVarIgnored fires only when BOTH variables are set, so
	// without this the plain-only case -- the one with the exposure -- is silent.
	BeatTokenSource envx.SecretSource
	Beats           []Beat
	LogLevel        slog.Level
}

// LogValue implements slog.LogValuer so a Config can never publish its own
// secrets. DISCORD_WEBHOOK_URL's path IS the Discord credential and BeatToken is
// the required /beat/{id} gate, so NEITHER is rendered: each is reported by its
// SOURCE only. The VALUE receiver is load-bearing: a *Config method set would
// leave the bare Config that Load returns outside slog.LogValuer, so a call that
// logged the whole struct would reflection-render both secrets.
//
//nolint:gocritic // hugeParam: slog.LogValuer must sit on the value receiver so a bare Config redacts too; the copy happens at most once per config log line.
func (c Config) LogValue() slog.Value {
	// The Host allowlist is knell's one OPTIONAL gate, and ABSENCE is the state
	// that needs publishing: a MISSPELLED variable name is indistinguishable
	// from unset, so without this an operator believes the rebinding guard is
	// armed. Active and Size are nil-safe, so a zero Config renders "any".
	allowedHosts := "any"
	if c.AllowedHosts.Active() {
		allowedHosts = fmt.Sprintf("allowlist(%d)", c.AllowedHosts.Size())
	}
	return slog.GroupValue(
		slog.Int("beats", len(c.Beats)),
		slog.String("node", c.Node),
		slog.String("listen_addr", c.ListenAddr),
		slog.String("webhook", string(c.WebhookSource)),
		slog.String("beat_token", string(c.BeatTokenSource)),
		slog.String("allowed_hosts", allowedHosts),
		slog.Int("trusted_proxies", len(c.TrustedProxies)),
		slog.String("log_level", c.LogLevel.String()),
	)
}

// Load reads the environment and returns the validated configuration. BEATS,
// DISCORD_WEBHOOK_URL and BEAT_TOKEN are required; everything else has a
// default. maxNodeNameBytes is the NODE_NAME cap this package ENFORCES, owned by
// the package that renders the notices it is measured over; hostPolicyOpts are
// the ALLOWED_HOSTS options internal/webapi authors, the 403 envelope being
// response shape. Both are parameters so this boundary depends on neither.
func Load(maxNodeNameBytes int, hostPolicyOpts ...webhttp.HostAllowlistOption) (Config, error) {
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

	webhook, webhookSource, err := loadWebhook()
	if err != nil {
		return cfg, err
	}
	cfg.WebhookURL = webhook
	cfg.WebhookSource = webhookSource

	node, err := nodeName(maxNodeNameBytes)
	if err != nil {
		return cfg, err
	}
	cfg.Node = node

	cfg.ListenAddr = listenAddr()

	hosts, err := allowedHosts(hostPolicyOpts)
	if err != nil {
		return cfg, err
	}
	cfg.AllowedHosts = hosts

	cfg.TrustedProxies = trustedProxies()

	beatToken, beatTokenSource, err := loadBeatToken()
	if err != nil {
		return cfg, err
	}
	cfg.BeatToken = beatToken
	cfg.BeatTokenSource = beatTokenSource

	cfg.LogLevel = logLevel()

	// Both advisories run here rather than inside the loaders: each names a
	// variable to unset, so every refusal above has to be behind them or the
	// advice describes a configuration that never ran.
	warnPlainVarIgnored("DISCORD_WEBHOOK_URL", "webhook URL", cfg.WebhookSource)
	warnPlainVarIgnored("BEAT_TOKEN", "token", cfg.BeatTokenSource)

	return cfg, nil
}

// nodeName resolves the observer name: NODE_NAME when set to a non-empty value
// (an empty one is ignored), else the hostname, else "unknown".
// A NODE_NAME past maxNodeNameBytes fails startup like any other malformed
// required value: the cap is what guarantees no name can push a notification
// past Discord's content limit, where the switch would arm and never ring. The
// hostname fallback is not length-checked because the kernel bounds it far below
// the cap, which holds only while maxNodeNameBytes stays at or above 255.
func nodeName(maxNodeNameBytes int) (string, error) {
	node := os.Getenv("NODE_NAME")
	if node == "" {
		return hostnameNode(), nil
	}
	if len(node) > maxNodeNameBytes {
		return "", fmt.Errorf("NODE_NAME is %d bytes, maximum is %d: the node name prefixes every Discord notification, and the cap keeps every notice far inside Discord's 2000-character content limit (an unbounded name would make Discord reject them all)", len(node), maxNodeNameBytes)
	}
	return node, nil
}

// osHostname is the seam over the one OS call this package cannot reach through
// the environment, so the two fallback branches below stay testable.
// Reassigned by tests in this package only.
var osHostname = os.Hostname

// hostnameNode is the NODE_NAME fallback: the hostname, else "unknown". A
// missing or blank hostname is a warning rather than a startup failure -- the
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
// value (a blank one is ignored), else defaultListenAddr.
// Padding is trimmed rather than refused because net.Listen resolves " :9190" as
// a hostname lookup and fails with the padding invisible in the crash-loop log
// line; unlike a credential, an address has no verifier on the other side that a
// trim could disagree with. An entirely blank value falls back to the default
// rather than to "", which would bind an ephemeral port.
func listenAddr() string {
	// TrimFunc with invisibleInURL, not TrimSpace: a pasted value carries
	// invisible runes TrimSpace keeps and net.Listen then fails to resolve, in a
	// crash loop whose cause is invisible in the log line.
	if addr := strings.TrimFunc(os.Getenv("LISTEN_ADDR"), invisibleInURL); addr != "" {
		return addr
	}
	return defaultListenAddr
}

// allowedHosts parses the ALLOWED_HOSTS exact-match Host allowlist. Unset (the
// documented default) yields an inactive policy that accepts every Host. An
// active allowlist is what breaks DNS rebinding (CWE-346) on the endpoints
// BEAT_TOKEN does not cover: /metrics enumerates every beat and its freshness,
// and Host is the only value the attacker cannot forge away. Any malformed entry
// FAILS STARTUP, because dropping it would leave a PARTIAL allowlist whose
// unlisted pings surface as missing-beat alerts for healthy services.
func allowedHosts(opts []webhttp.HostAllowlistOption) (*webhttp.HostPolicy, error) {
	const key = "ALLOWED_HOSTS"
	raw := os.Getenv(key)
	policy, invalid := webhttp.ParseHostList(strings.Split(raw, ","), opts...)
	if len(invalid) > 0 {
		// %q, not %v: the one mistake an operator cannot SEE is an invisible
		// rune pasted in with the hostname, and %q escapes it. Count plus bounded
		// sample: a 1 MiB value rejects ~95k entries, and the record is knell's.
		return nil, fmt.Errorf("%s has %d entries no Host can ever match, so the allowlist knell would serve is not the one configured: %.64q; use bare hostnames or IPs only (no scheme, path, or CIDR), e.g. localhost,10.0.0.5,knell.example.com — a lone port like :9190 belongs in LISTEN_ADDR", key, len(invalid), invalid[:min(len(invalid), 4)])
	}
	return policy, nil
}

// trustedProxies parses TRUSTED_PROXIES into the CIDR set the access line
// resolves client_ip against. Entries degrade RESTRICTIVELY: a malformed entry
// is warned about and dropped, because dropping one NARROWS whose
// X-Forwarded-For is believed (env-validation.md, "Restriction LISTS").
// Contrast ALLOWED_HOSTS, where a dropped entry would refuse a caller the
// operator meant to admit. Unset or blank returns nil, which is what
// webhttp.WithClientIP takes when given no ranges: client_ip is the socket peer.
func trustedProxies() []*net.IPNet {
	// Getenv: ParseCIDRs trims and skips blank entries, so a present-but-blank
	// value needs nothing local.
	raw := os.Getenv("TRUSTED_PROXIES")
	nets, invalid := webhttp.ParseCIDRs(strings.Split(raw, ","))
	if len(invalid) > 0 {
		slog.Warn("TRUSTED_PROXIES entries ignored, X-Forwarded-For is not honored for them",
			"ignored", fmt.Sprintf("%.64q", invalid[:min(len(invalid), 4)]),
			"ignored_count", len(invalid), "trusted", len(nets))
	}
	return nets
}

// logLevel resolves the log level: LOG_LEVEL when it parses, else info.
func logLevel() slog.Level {
	raw := os.Getenv("LOG_LEVEL")
	level, ok := slogx.ParseLevel(raw, slog.LevelInfo)
	if !ok {
		// NOT pre-quoted: TextHandler already quotes an attr value that needs it,
		// and an invisible rune does, so quoting here would quote the quotes.
		slog.Warn("invalid LOG_LEVEL, using info", "value", fmt.Sprintf("%.64s", raw))
	}
	return level
}

// rejectBlankFileVar fails startup when a `_FILE` variable is PRESENT but blank.
// envx gates its file channel on a non-empty value, so an empty `_FILE` is
// indistinguishable from unset and silently falls back to the plain variable --
// which is not the credential the operator pointed knell at, and for BEAT_TOKEN
// would arm the gate for a stale value while a rotated secret file sat unread.
// envx.IsBlankSecretFilePath reports the state; the refusal is knell's policy.
func rejectBlankFileVar(key string) error {
	if envx.IsBlankSecretFilePath(key) {
		return fmt.Errorf("%s_FILE is set but empty: unset it to configure %s directly, or point it at a secret file", key, key)
	}
	return nil
}

// secretFileError rewrites an envx secret-file failure into a message that names
// the variable and the failure CLASS but never the operator-supplied path. envx
// embeds the KEY_FILE path in its messages -- correct when the value is a path,
// and a leak when it is not: the common misconfiguration
// `DISCORD_WEBHOOK_URL_FILE=https://…/<token>` would otherwise copy a live
// webhook URL into the startup ERROR line and from there into Loki. envx
// classifies every failure with a sentinel, so each class states its own remedy
// with no error-text matching.
func secretFileError(key string, err error) error {
	switch {
	case errors.Is(err, envx.ErrBlankSecretFile):
		return fmt.Errorf("%s_FILE points to a blank secret file: point it at a file containing the secret, or unset it to configure %s directly", key, key)
	case errors.Is(err, envx.ErrSecretFilePathRejected):
		// envx refuses a path that is not already clean or that contains "..",
		// and its own message embeds the value -- the leak this function exists
		// to prevent, since a variable holding the credential lands here.
		return fmt.Errorf("%s_FILE does not name a usable path: it must be an already-clean path with no \"..\" segment, no doubled separator and no trailing slash, e.g. /run/secrets/%s; if the variable holds the secret itself rather than a path to it, unset %s_FILE and configure %s directly", key, strings.ToLower(key), key, key)
	case errors.Is(err, envx.ErrSecretFileTooLarge):
		// 1 MiB is envx's documented ceiling, restated because it is not
		// exported. The shape this catches is a mount pointing at the wrong
		// thing entirely, so the remedy is the mount.
		return fmt.Errorf("%s_FILE points to a file larger than the 1 MiB secret-file limit, so it was refused instead of read: point it at a file holding only the secret (a few dozen bytes), not at a bundle, archive or log the mount picked up by mistake", key)
	case errors.Is(err, envx.ErrSecretFileGrew):
		// The file passed the size gate and then grew past it mid-read, so
		// reading on would have handed knell a silently truncated secret. A
		// writer problem, not a size problem: the remedy is an atomic write.
		return fmt.Errorf("%s_FILE grew past the 1 MiB secret-file limit while it was being read, so the secret would have been silently truncated and every request using it would be rejected: have the writer create the file atomically (write a temporary file, then rename it into place) rather than appending to the mounted one, then restart knell", key)
	case errors.Is(err, envx.ErrSecretFileUnreadable):
		// The OS refused the open, stat or read. envx keeps the *os.PathError
		// reachable, so the syscall and its bare reason can be named while
		// pathErr.Path, the operator-supplied value, stays out.
		if pathErr, ok := errors.AsType[*os.PathError](err); ok {
			return fmt.Errorf("%s_FILE could not be read (%s failed): %v: check that the path the variable names exists inside the container and is readable by knell's non-root user", key, pathErr.Op, pathErr.Err)
		}
		return fmt.Errorf("%s_FILE could not be read: check that the path the variable names exists inside the container and is readable by knell's non-root user", key)
	}
	// A failure class this envx version did not have when the branches above
	// were written. The wording names every requirement the classes cover, in
	// the operator's terms rather than the dependency's.
	return fmt.Errorf("%s_FILE could not be read or validated: point it at a clean path (no \"..\") naming a readable secret file of at most 1 MiB; a secret file holds a few dozen bytes", key)
}

// warnPlainVarIgnored reports that KEY_FILE supplied the secret while the plain
// KEY was also set, so the plain variable was ignored. Call it only once the
// secret read SUCCEEDED and its value PASSED validation: on either failure path
// the advice would describe a configuration that never ran. The message is
// static and the variable names ride attributes, so one Loki query covers both
// credentials.
func warnPlainVarIgnored(key, subject string, src envx.SecretSource) {
	if src != envx.SourceFile || os.Getenv(key) == "" {
		return
	}
	slog.Warn("both the plain variable and its _FILE companion are set; the file wins and the plain variable is ignored, so unset it to keep the credential out of the process environment",
		"variable", key, "file_variable", key+"_FILE", "credential", subject)
}

// fileSourcedValueError names the CHANNEL that supplied a credential whose
// VALUE this package refused. The READ-failure refusals already name the `_FILE`
// variable, while the value-validation ones named only the plain variable, so a
// `_FILE` pointing at the wrong file crash-looped the observer with a message
// about a variable the operator may never have set. The wrapped error is
// unchanged and the value is still never echoed.
func fileSourcedValueError(key string, src envx.SecretSource, err error) error {
	if src != envx.SourceFile {
		return err
	}
	return fmt.Errorf("%w (this value came from %s_FILE, not %s: fix the file's content, or the mount it points at)", err, key, key)
}

// resolveSecret reads one required credential through envx's KEY/KEY_FILE pair
// and applies this package's fail-closed policy. Both credentials go through it
// so the policy has ONE spelling. A blank KEY_FILE is refused before envx
// resolves anything, since envx treats it as unset and falls back to the plain
// variable. A secret file that cannot be used FAILS STARTUP and never falls
// back; its error is sanitized by secretFileError and never wrapped, because it
// embeds the KEY_FILE value. An absent credential is reported as setButEmpty or
// missing, which envx cannot tell apart and whose remedies differ.
func resolveSecret(key string, setButEmpty, missing error) (string, envx.SecretSource, error) {
	if err := rejectBlankFileVar(key); err != nil {
		return "", envx.SourceNone, err
	}
	value, src, err := envx.SecretWithSource(key)
	switch {
	case err == nil:
		return value, src, nil
	case errors.As(err, new(*envx.MissingError)):
		if v, ok := os.LookupEnv(key); ok && v == "" {
			return "", src, setButEmpty
		}
		return "", src, fmt.Errorf("%w: %w", missing, err)
	default:
		return "", src, secretFileError(key, err)
	}
}

// beatTokenFitsHeader reports whether value can be carried verbatim in an HTTP
// field value, ASSUMING edge ASCII whitespace has already been REFUSED. It
// answers the byte-legality question only, so it is not a substitute for the
// padding refusal but runs after it. HTTP permits SP, HTAB, visible ASCII and
// obs-text (which is why a non-ASCII space token stays legal) and rejects every
// other ASCII control byte and DEL; Go's own client refuses to write such a
// value, so a token containing one is unpresentable whatever the sender does.
func beatTokenFitsHeader(value string) bool {
	for i := range len(value) {
		b := value[i]
		if (b < ' ' && b != '\t') || b == 0x7f {
			return false
		}
	}
	return true
}

// errWebhookSetButEmpty is the refusal for a DISCORD_WEBHOOK_URL that is present
// and carries no value: resolveSecret returns it for the present-but-empty
// variable, and loadWebhook for a value with nothing visible left after the
// trim, so the two cannot describe one misconfiguration differently.
var errWebhookSetButEmpty = errors.New("DISCORD_WEBHOOK_URL is set but empty: point it at the https webhook URL, or use DISCORD_WEBHOOK_URL_FILE")

// errWebhookRequired is the refusal for a DISCORD_WEBHOOK_URL that was never
// set; its present-but-empty twin is errWebhookSetButEmpty.
var errWebhookRequired = errors.New("DISCORD_WEBHOOK_URL is required")

// errBeatTokenSetButEmpty and errBeatTokenRequired are the two diagnoses an
// absent BEAT_TOKEN gets, kept apart because the operator's next move differs.
// Serving /beat/{id} with no credential is not one of the options: any client
// that can reach the port would keep every beat reading fresh.
var (
	errBeatTokenSetButEmpty = fmt.Errorf("BEAT_TOKEN is set but empty: it is the only thing standing between a stranger who can reach this port and a forged ping, so there is no configuration in which knell serves /beat/{id} without it; set it to a random token of at least %d bytes (e.g. `openssl rand -hex 16`), or point BEAT_TOKEN_FILE at a file holding one", minTokenLength)
	errBeatTokenRequired    = fmt.Errorf("BEAT_TOKEN is required: it is the only gate on /beat/{id}, so without it any client that can reach this port can keep every beat reading fresh while the thing it watches is dead; set it to a random token of at least %d bytes (e.g. `openssl rand -hex 16`), or point BEAT_TOKEN_FILE at a file holding one", minTokenLength)
)

// checkBeatToken validates a configured BEAT_TOKEN as the exact credential
// senders must present, or refuses it. It never rewrites the value. Edge ASCII
// whitespace is REFUSED rather than trimmed (asciiWhitespace holds the two wire
// mechanisms) and an interior forbidden byte is refused by beatTokenFitsHeader:
// such a token is unsendable as written or unreadable in the value the operator
// holds, and the switch should refuse to start rather than report itself gated
// while 401ing every sender. Below minTokenLength the reason inverts (it IS
// presentable, and so is a guess at it); above it, the header reserve pays.
func checkBeatToken(token string) error {
	if strings.Trim(token, asciiWhitespace) != token {
		// The value is never echoed (the startup log ships to Loki), and the
		// message names the whole CUTSET rather than one member's mechanism.
		return errors.New("BEAT_TOKEN has leading or trailing ASCII whitespace: a trailing space or tab is stripped from the header value on the wire, and CR, LF, VT and FF cannot be sent in one at all, so such a token never reaches the verifier as configured and POST /beat/{id} would reject every ping while the endpoint reports itself gated; a leading space or tab is refused too, because it authenticates as part of the credential while being invisible in the value you read; knell will not silently rewrite a credential, so remove the surrounding whitespace")
	}
	if !beatTokenFitsHeader(token) {
		// Free of edge padding, yet still unpresentable: a control byte (a
		// pasted newline, a stray second line in a secret file) is illegal in
		// an HTTP field value, so no sender can deliver this token.
		return fmt.Errorf("BEAT_TOKEN contains a control character that HTTP forbids in a header value, so no sender can present it; use a token of at least %d printable characters", minTokenLength)
	}
	if len(token) < minTokenLength {
		// Presentable, and short enough to be guessed; the throttle on failed
		// attempts slows a guessing run without ending it. The exact length,
		// the value and its character class are all kept out of the log.
		return fmt.Errorf("BEAT_TOKEN is shorter than the %d-byte minimum: it is the only gate on /beat/{id}, so a token short enough to guess lets a stranger who can reach this port keep every beat reading fresh while the thing it watches is dead; set a random token of at least %d bytes (e.g. `openssl rand -hex 16`)", minTokenLength, minTokenLength)
	}
	if len(token) > maxTokenLength {
		// Presentable byte for byte, refused for what it would cost the header
		// budget: once the WHOLE block crosses MaxRequestHeaderBytes, POST
		// /beat/{id} answers 431 while reporting itself gated. A multi-line file
		// never reaches here; a long single-line value from a mount does.
		return fmt.Errorf("BEAT_TOKEN is longer than the %d-byte maximum: the maximum is the token's share of the %d-byte header block knell reads, so an accepted token always travels with %d bytes left over for the request line, the Host header and the \"Bearer \" prefix, and a longer one is refused at startup rather than left to eat that reserve until POST /beat/{id} answers 431 to every ping while reporting itself gated; a real credential is far shorter than the maximum, so set a random token of at least %d bytes (e.g. `openssl rand -hex 16`), or check that BEAT_TOKEN_FILE names the secret file itself rather than a bundle the mount picked up", maxTokenLength, MaxRequestHeaderBytes, headerOverheadAllowance, minTokenLength)
	}
	return nil
}

// loadBeatToken reads the REQUIRED BEAT_TOKEN bearer gate for POST /beat/{id},
// required because it is the endpoint's only gate. An unset variable, a
// present-but-empty one, and any value no sender could present as configured all
// FAIL STARTUP, like an empty BEAT_TOKEN_FILE. The value is stored EXACTLY as
// configured: a file-sourced token arrives as written apart from ONE trailing
// line ending, which envx removes, so `printf '%s\n' token > file` is unaffected
// while a file carrying edge whitespace fails startup like the plain variable.
func loadBeatToken() (string, envx.SecretSource, error) {
	token, src, err := resolveSecret("BEAT_TOKEN", errBeatTokenSetButEmpty, errBeatTokenRequired)
	if err != nil {
		return "", src, err
	}
	if tokenErr := checkBeatToken(token); tokenErr != nil {
		return "", src, fileSourcedValueError("BEAT_TOKEN", src, tokenErr)
	}
	return token, src, nil
}

// loadWebhook reads and shape-checks DISCORD_WEBHOOK_URL, returning the URL and
// the channel it arrived through. The URL is a secret: errors never embed it, and
// only https is accepted -- the URL's own path carries the webhook credential, so
// plain http would put the secret on the wire in cleartext. The SOURCE is not a
// secret and is returned for the startup summary.
func loadWebhook() (string, envx.SecretSource, error) {
	webhook, src, err := resolveSecret("DISCORD_WEBHOOK_URL", errWebhookSetButEmpty, errWebhookRequired)
	if err != nil {
		return "", src, err
	}

	// Neither channel trims, so padding copied out of a deployment file reaches
	// here through either one; trimmed because knell is this URL's only sender.
	webhook = strings.TrimFunc(webhook, invisibleInURL)
	if webhook == "" {
		// Nothing visible at all: the variable WAS provided, so this is a broken
		// secret pipeline, not a missing setting. Falling through to the shape
		// check would answer "scheme must be https" for a value with no scheme.
		return "", src, fileSourcedValueError("DISCORD_WEBHOOK_URL", src, errWebhookSetButEmpty)
	}
	if _, err := parseWebhookURL(webhook); err != nil {
		return "", src, fileSourcedValueError("DISCORD_WEBHOOK_URL",
			src, fmt.Errorf("DISCORD_WEBHOOK_URL: %w", err))
	}
	return webhook, src, nil
}

// parseBeats parses the BEATS spec list: comma-separated "id:deadline"
// entries, e.g. "watchdog-mimir:20m,watchdog-loki:20m". IDs must match
// [A-Za-z0-9][A-Za-z0-9_-]{0,63} and be unique; deadlines are Go durations
// of at least minDeadline.
func parseBeats(raw string) ([]Beat, error) {
	// Count first, allocate second, and never materialize the split: sizing from
	// the ENTRY count keeps BEATS' LENGTH out of the footprint. execve caps one
	// env value at 32*PAGE_SIZE (128 KiB at 4 KiB pages, 512 KiB at 16 KiB), so
	// the split this avoids is single-digit MiB of headers, not an unbounded one.
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
		return Beat{}, fmt.Errorf("entry %.64q: expected \"id:deadline\"", entry)
	}
	id = strings.TrimSpace(id)
	if !beatIDPattern.MatchString(id) {
		return Beat{}, fmt.Errorf("entry %.64q: id must match %s", entry, beatIDPattern)
	}
	if _, dup := seen[id]; dup {
		return Beat{}, fmt.Errorf("entry %.64q: duplicate id %.64q", entry, id)
	}
	rawDeadline = strings.TrimSpace(rawDeadline)
	deadline, err := time.ParseDuration(rawDeadline)
	if err != nil {
		// The operand is stated here rather than through the stdlib message,
		// which quotes it a second time unbounded: a unit-less "api:30" and a
		// day-unit "cron-backup:1d" are the two shapes a first BEATS carries.
		return Beat{}, fmt.Errorf("entry %.64q: invalid deadline %.64q: use a Go duration "+
			"with an explicit unit (s, m, h), e.g. 30s, 20m or 26h; there is no day "+
			"unit, so a daily job is 26h", entry, rawDeadline)
	}
	if deadline < minDeadline {
		return Beat{}, fmt.Errorf("entry %.64q: deadline below minimum %s", entry, minDeadline)
	}
	seen[id] = struct{}{}
	return Beat{ID: id, Deadline: deadline}, nil
}

// invisibleInURL reports whether r is a rune an operator cannot see inside a
// configured webhook URL: the ASCII space, and every rune outside Unicode's
// printable set. Runes in this set that survive url.Parse are percent-encoded
// on every request, so the host and path that reach the other end are not the
// configured ones and no notification can ever be delivered -- while startup
// succeeds and /healthz reports ready. Printable runes are excluded on purpose:
// refusing them would reject a working non-Discord relay URL.
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
		// Deliberately omits the provided scheme: a malformed secret value like
		// "credentialmaterial:rest" parses with the secret prefix as its scheme,
		// and this error reaches the startup log.
		return nil, errors.New("scheme must be https (the webhook URL's own path is the credential, so plain http would send it in cleartext)")
	}
	if u.Hostname() == "" {
		// Hostname(), not Host: an authority carrying only a port
		// ("https://:443/api/webhooks/x/y") has a non-empty Host and would pass,
		// so startup would go ready for a webhook with no destination at all.
		return nil, errors.New("missing host")
	}
	// url.Parse validates a port's SYNTAX, never its RANGE, so
	// "https://discord.example:99999/hook" starts the process healthy and then
	// fails on every POST while httpx and the sweep retry forever. A permanently
	// undeliverable webhook is the one failure a dead-man switch cannot afford.
	if port := u.Port(); port != "" {
		n, convErr := strconv.Atoi(port)
		if convErr != nil || n < 1 || n > 65535 {
			// The number is the operator's own value, not part of the credential,
			// but the message omits it anyway: a fixed constant cannot leak a
			// mis-parsed secret into Loki.
			return nil, errors.New("port must be between 1 and 65535")
		}
	}
	if u.Path == "" || u.Path == "/" {
		// The webhook's PATH is the credential, so a URL that carries none is
		// not a webhook: every POST would go to the origin's root and Discord
		// would refuse it forever while startup reported success.
		return nil, errors.New("missing path (the webhook URL's own path carries the credential, so a host-only URL cannot deliver a notification)")
	}
	if !utf8.ValidString(raw) || strings.ContainsFunc(raw, invisibleInURL) {
		// Visible runes are NOT refused: that would reject a working non-Discord
		// relay URL. utf8.ValidString carries the rule to the BYTE level, which the
		// rune predicate cannot: an invalid byte decodes to U+FFFD, which IsPrint accepts.
		return nil, errors.New("contains a space or an invisible character (it is percent-encoded on every request, so the webhook host and path that reach the other end are not the configured ones; remove it, or percent-encode it yourself if it really belongs to the credential)")
	}
	return u, nil
}
