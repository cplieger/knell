// Package notify delivers knell's transition notifications to a Discord
// webhook. It is the app's only outbound-network package and retries transient
// delivery failures via httpx. internal/watch decides which transition
// happened; this package decides how an operator reads it.
package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cplieger/httpx/v5"
	"github.com/cplieger/knell/internal/watch"
)

// attemptTimeout bounds each delivery attempt inside httpx's retry loop, so
// its expiry is retryable rather than extending the caller's own budget.
const attemptTimeout = 10 * time.Second

// MaxNodeNameBytes is the maximum UTF-8 byte length of NODE_NAME, enforced by
// internal/config. The name is interpolated into every notice, so an
// unbounded value would make Discord reject all of them.
const MaxNodeNameBytes = 256

// maxAttempts is the total delivery attempts per notification (httpx
// semantics: total, including the first).
const maxAttempts = 3

// sendBudget is the total wall time one delivery may spend, derived from the
// per-attempt knobs so changing either cannot leave it stale.
const sendBudget = maxAttempts*attemptTimeout + rateLimitMaxWait

// rateLimitMaxWait caps one rate-limited attempt's wait, and is the wait used
// whenever a 429 carries no positive Retry-After header.
const rateLimitMaxWait = 30 * time.Second

// maxErrorBodyBytes caps how much of a rejected response's body is read. Two
// things are published from it and nothing else: the numeric "code" field, and
// which of knell's OWN payload field names Discord blamed, each with a
// class-gated machine code. The object's "message" strings are remote-authored
// and can echo the webhook URL, which is the credential, so they never reach a
// variable this package formats.
const maxErrorBodyBytes = 512

// maxErrorFieldDepth bounds the walk of Discord's nested "errors" object.
// Its deepest real shape, embeds.0.fields.0.name plus _errors, is six levels.
const maxErrorFieldDepth = 8

// maxErrorFields bounds how many blamed paths one detail names.
const maxErrorFields = 4

// maxErrorCodeBytes bounds one published machine code. The class below admits
// only single-byte ASCII, so a byte count is the rune count too.
const maxErrorCodeBytes = 48

// maxErrorCodeDigitRun bounds a run of digits inside a published machine code.
// A Discord snowflake is 17 digits or more, so refusing a longer run keeps the
// webhook's own path id out of a log line even when the endpoint answering the
// POST is the one reporting the error. Discord's form-body codes are compound
// words and carry no digit run at all.
const maxErrorCodeDigitRun = 2

// maxErrorDetailRunes bounds the whole bracketed list, the truncation marker
// included.
const maxErrorDetailRunes = 240

// userAgent identifies this client to Discord's edge; an unset User-Agent
// (Go's default) is commonly refused by an edge or WAF in front of a webhook.
const userAgent = "knell (https://github.com/cplieger/knell)"

// Discord posts a one-line message plus one rich embed to one
// Discord-compatible webhook.
type Discord struct {
	client *http.Client
	url    string
	// node is already escaped for Discord markdown, so it may occupy only a
	// slot that RENDERS markdown (the content line, an embed field value).
	// Escaping it again at a render site would publish the backslashes.
	node string
	// Fields only so a test can shorten them.
	attemptTimeout   time.Duration
	rateLimitMaxWait time.Duration
	sendBudget       time.Duration
}

var _ watch.Notifier = (*Discord)(nil)

// New builds a Discord notifier for the given webhook URL. node names this
// observer instance in every message so multi-node deployments read as
// distinct reports.
func New(webhookURL, node string) *Discord {
	client := httpx.NewClient(attemptTimeout + 5*time.Second)
	// Follow only a same-host hop, never one net/http would rewrite to another
	// method: the webhook POST must not be replayed as a bodyless GET. Referer
	// is deleted separately because net/http writes the previous request's full
	// URL there, and for a webhook the path is the credential.
	policy := httpx.RedirectPolicyFunc(httpx.WithSameHost(true), httpx.WithPreserveMethod(true))
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		req.Header.Del("Referer")
		return policy(req, via)
	}
	return &Discord{
		client:           client,
		url:              webhookURL,
		node:             escapeMarkdown(node),
		attemptTimeout:   attemptTimeout,
		rateLimitMaxWait: rateLimitMaxWait,
		sendBudget:       sendBudget,
	}
}

// Close releases idle connections. Call once on shutdown.
func (d *Discord) Close() {
	d.client.CloseIdleConnections()
}

// --- The Discord payload ---

// webhookMessage is one Execute Webhook payload: a terse content line, so a
// notification preview has a plain string to show, plus one embed carrying the
// detail.
type webhookMessage struct {
	Content         string          `json:"content"`
	Embeds          []embed         `json:"embeds"`
	AllowedMentions allowedMentions `json:"allowed_mentions"`
}

// allowedMentions is the only mention control in this app: escapeMarkdown
// deliberately leaves "@" alone, since a backslash before it is not a Discord
// escape, so an empty parse list is the only structural way to keep a notice
// from pinging anyone.
type allowedMentions struct {
	// Parse must be a non-nil empty slice: a nil one marshals to null rather
	// than to [], which is a different request.
	Parse []string `json:"parse"`
}

// embed is the card one notice renders as. The footer, author, image,
// thumbnail and url slots are absent on purpose: the title and footer text do
// not render Discord markdown, so no configured value may occupy them, and
// Discord shows only the first of several embeds sharing a url. "type" is
// absent because it is always "rich" for a webhook embed and is ignored.
// https://discord.com/developers/docs/resources/message#embed-object
type embed struct {
	Timestamp   time.Time    `json:"timestamp"`
	Title       string       `json:"title"`
	Description string       `json:"description"`
	Fields      []embedField `json:"fields,omitempty"`
	Color       int          `json:"color"`
}

// embedField is one labelled value in the card.
type embedField struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline"`
}

// --- Notice kinds and how each presents ---

// noticeKind is which of watch.Notifier's three methods produced a notice.
// obs.Kind names the same three concepts for the metrics labels and is
// deliberately not reused: its values are Prometheus label strings an
// operator's queries and alert rules read, and internal/obs fills a
// package-level registry in init().
type noticeKind int

const (
	kindMissing noticeKind = iota
	kindRecovered
	kindHistory
)

// noticeStyle is how one kind presents: the emoji vocabulary an operator
// already reads in the channel, the word that names the transition, and
// Discord's own brand colour for the severity.
type noticeStyle struct {
	emoji string
	word  string
	color int
}

var noticeStyles = [...]noticeStyle{
	kindMissing:   {emoji: "🚨", word: "MISSING", color: 0xED4245},
	kindRecovered: {emoji: "✅", word: "recovered", color: 0x57F287},
	kindHistory:   {emoji: "🕓", word: "outage history", color: 0xFEE75C},
}

// notice is one notification before it becomes a Discord message. id is the
// RAW beat id: the embed title does not render markdown, so an escaped id
// there would publish the backslashes instead of the name.
type notice struct {
	at       time.Time
	id       string
	guidance string
	fields   []embedField
	kind     noticeKind
}

// noticeTimeFormat includes the date because a queued notice may arrive days
// after recovery; times are always converted to UTC first so they correlate
// with knell_beat_last_seen_timestamp_seconds.
const noticeTimeFormat = "2006-01-02 15:04 MST"

// noticeTime renders an instant for a human-readable field value.
func noticeTime(t time.Time) string { return t.UTC().Format(noticeTimeFormat) }

// message renders one notice as the payload post sends. It is the single place
// a configured value is placed in a slot, which is what keeps the escaped
// forms out of the title.
func (d *Discord) message(n *notice) webhookMessage {
	style := noticeStyles[n.kind]
	fields := make([]embedField, 0, len(n.fields)+1)
	fields = append(fields, n.fields...)
	fields = append(fields, embedField{Name: "Observer", Value: d.node, Inline: true})
	return webhookMessage{
		Content: fmt.Sprintf("%s [knell %s] beat **%s** %s", style.emoji, d.node, escapeMarkdown(n.id), style.word),
		Embeds: []embed{{
			Timestamp:   n.at.UTC(),
			Title:       fmt.Sprintf("%s beat %s %s", style.emoji, n.id, style.word),
			Description: n.guidance,
			Fields:      fields,
			Color:       style.color,
		}},
		AllowedMentions: allowedMentions{Parse: []string{}},
	}
}

// BeatMissing announces that a beat's deadline of silence has passed. The
// wording names what to check without presuming the beat ever pinged, since
// watch.Transition cannot distinguish "never wired up" from "pinged for weeks
// and stopped".
func (d *Discord) BeatMissing(ctx context.Context, id string, live watch.Transition) error {
	return d.post(ctx, "missing "+id, &notice{
		kind:     kindMissing,
		id:       id,
		guidance: "Nothing has pinged it in time: check the sender, its path to this observer, and that anything is pinging this beat id at all.",
		at:       live.Observed,
		fields:   liveFields(live),
	})
}

// BeatRecovered announces the first ping after a missing alert.
func (d *Discord) BeatRecovered(ctx context.Context, id string, live watch.Transition) error {
	return d.post(ctx, "recovered "+id, &notice{
		kind:     kindRecovered,
		id:       id,
		guidance: "Pings are arriving again. The outage is over.",
		at:       live.Observed,
		fields:   liveFields(live),
	})
}

// liveFields are the two facts both live notices carry.
func liveFields(live watch.Transition) []embedField {
	return []embedField{
		{Name: "Silent for", Value: live.DownFor().Truncate(time.Second).String(), Inline: true},
		{Name: "Silence began", Value: noticeTime(live.Started), Inline: true},
	}
}

// BeatOutageHistory announces outages already over by the time this observer
// could send anything, in one past-tense message per call. outages must be
// non-empty and ascend by recovery point (guaranteed by watch).
func (d *Discord) BeatOutageHistory(ctx context.Context, id string, outages []watch.Outage) error {
	return d.post(ctx, "history "+id, historyNotice(id, outages))
}

// historyNotice renders the history notice for id. The lateness clause comes
// from watch's delivery blame rather than being guessed here.
func historyNotice(id string, outages []watch.Outage) *notice {
	last := outages[len(outages)-1]
	n := &notice{kind: kindHistory, id: id, at: last.Recovered}
	if len(outages) == 1 {
		n.guidance = lateClause(last.Undelivered)
		n.fields = []embedField{
			{Name: "Was missing for", Value: last.DownFor().Truncate(time.Second).String(), Inline: true},
			{Name: "Silence began", Value: noticeTime(last.Started), Inline: true},
			{Name: "Recovered at", Value: noticeTime(last.Recovered), Inline: true},
			{Name: "Delivery", Value: deliveryBlame(outages), Inline: true},
		}
		return n
	}
	n.guidance = batchLateClause(outages)
	n.fields = []embedField{
		{Name: "Outages", Value: strconv.Itoa(len(outages)), Inline: true},
		{Name: "Longest outage", Value: watch.LongestOutage(outages).Truncate(time.Second).String(), Inline: true},
		{Name: "Last recovered at", Value: noticeTime(last.Recovered), Inline: true},
		{Name: "Delivery", Value: deliveryBlame(outages), Inline: true},
	}
	return n
}

// markdownEscaper neutralizes Discord's markup. Every escaped entry is a
// Discord formatting character; Discord strips a backslash only in front of
// one of its own, so escaping anything else would publish the backslash.
var markdownEscaper = strings.NewReplacer(
	// Line breaks are collapsed rather than escaped: Discord's heading,
	// blockquote and list markup is line-anchored, so only removing the break
	// suppresses it. A space is never wider than the break it replaces, so
	// MaxNodeNameBytes still holds.
	"\r\n", " ",
	"\r", " ",
	"\n", " ",
	`\`, `\\`,
	"*", `\*`,
	"_", `\_`,
	"~", `\~`,
	"`", "\\`",
	"|", `\|`,
	"[", `\[`,
	"]", `\]`,
)

// escapeMarkdown renders s literally in a Discord message. Discord's markdown
// eats characters rather than merely styling them: a pair of underscores
// italicizes and removes both, so "db_backup_nightly" would otherwise arrive
// as "dbbackupnightly", matching nothing in BEATS.
func escapeMarkdown(s string) string {
	return markdownEscaper.Replace(s)
}

// lateClause explains why one ended outage is reported after the fact.
func lateClause(undelivered bool) string {
	if undelivered {
		return "This notice is late because delivery was delayed - check the webhook."
	}
	return "This notice is late because no delivery was ever attempted for it - the webhook is not the place to look."
}

// batchLateClause explains why a whole run of ended outages is reported after
// the fact. A mixed batch names both counts rather than picking a majority and
// stating something false about the rest.
func batchLateClause(outages []watch.Outage) string {
	switch total, undelivered := len(outages), countUndelivered(outages); undelivered {
	case total:
		return "Delivery was delayed for every outage - check the webhook."
	case 0:
		return "No delivery was ever attempted for any of them - the webhook is not the place to look."
	default:
		return fmt.Sprintf(
			"Delivery was delayed for %d (check the webhook); %d had nothing attempted.",
			undelivered, total-undelivered,
		)
	}
}

// deliveryBlame is the same delivery split as a compact field value.
// lateClause and batchLateClause own the SENTENCE an operator acts on, so a
// change to what the split means belongs in all three.
func deliveryBlame(outages []watch.Outage) string {
	total, undelivered := len(outages), countUndelivered(outages)
	if total == 1 {
		if undelivered == 1 {
			return "refused"
		}
		return "never attempted"
	}
	switch undelivered {
	case total:
		return "all refused"
	case 0:
		return "none attempted"
	default:
		return fmt.Sprintf("%d refused, %d never attempted", undelivered, total-undelivered)
	}
}

// countUndelivered counts the outages whose own delivery was refused.
func countUndelivered(outages []watch.Outage) int {
	var undelivered int
	for _, o := range outages {
		if o.Undelivered {
			undelivered++
		}
	}
	return undelivered
}

// post delivers one notice, retrying transient failures. The webhook URL never
// appears in returned errors or logs: the two places remote text could enter
// are reduced structurally rather than filtered (safeTransportError renders
// none of its cause, statusDetail projects the blamed paths onto this
// package's own field names).
func (d *Discord) post(ctx context.Context, label string, n *notice) error {
	body, _ := json.Marshal(d.message(n))
	ctx, cancel := httpx.ContextWithDefaultTimeout(ctx, d.sendBudget)
	defer cancel()
	_, err := httpx.Do(ctx, func(ctx context.Context) (struct{}, error) {
		return d.postAttempt(ctx, body)
	}, httpx.WithLabel("discord webhook "+label), httpx.WithMaxAttempts(maxAttempts),
		// Retryable so httpx does not classify this bound's expiry as terminal.
		httpx.WithAttemptTimeout(d.attemptTimeout),
		httpx.WithRateLimitRetry(d.rateLimitMaxWait),
		// watch already logs the terminal verdict; keep httpx's own line at Debug.
		httpx.WithExhaustedLevel(slog.LevelDebug))
	if err != nil {
		return fmt.Errorf("delivering %s notification: %w", label, err)
	}
	return nil
}

// safeTransportError reports a failed transport call in knell's own words.
// httpx.LogSafeError alone is not enough: two of net/http's causes are written
// from the response's Location header, so a redirecting endpoint could put the
// webhook path into the logs. The cause is classified, never rendered, and
// reachable only through Unwrap.
func safeTransportError(err error) error {
	// Reduce until no *url.Error is left anywhere in the chain: LogSafeError
	// returns what errors.As finds, so a nested one would unwrap past this
	// wrapper otherwise. Bounded so a self-reducing value cannot spin forever.
	const maxURLErrorDepth = 8
	cause := httpx.LogSafeError(err)
	for range maxURLErrorDepth {
		if _, ok := errors.AsType[*url.Error](cause); !ok {
			break
		}
		cause = httpx.LogSafeError(cause)
	}
	// net.OpError's Op is one of net's own fixed verbs, so it is safe to print;
	// it distinguishes a stalled dial (egress/DNS) from a stalled read (host
	// went quiet after accepting).
	phrase := "webhook transport failed"
	if opErr, ok := errors.AsType[*net.OpError](cause); ok && opErr.Op != "" {
		phrase += " during " + opErr.Op
	}
	return transportError{phrase: phrase, cause: cause}
}

// transportError is a transport failure whose message is knell's alone.
// Error() renders the fixed phrase and nothing from the cause; Unwrap exposes
// the cause so errors.Is/As keep working.
type transportError struct {
	cause  error
	phrase string
}

func (e transportError) Error() string { return e.phrase }
func (e transportError) Unwrap() error { return e.cause }

// postAttempt performs one delivery attempt: request construction, transport
// call, and response cleanup, leaving the verdict to deliveryError. Every
// error it returns is URL-free by construction.
func (d *Discord) postAttempt(ctx context.Context, body []byte) (struct{}, error) {
	req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, d.url, bytes.NewReader(body))
	if reqErr != nil {
		// The raw error would embed the URL; report the cause only.
		return struct{}{}, fmt.Errorf("building webhook request: %w", httpx.LogSafeError(reqErr))
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", userAgent)
	resp, doErr := d.client.Do(req) //nolint:bodyclose // closed via the deferred httpx.DrainClose below
	if doErr != nil {
		return struct{}{}, safeTransportError(doErr)
	}
	// Drain up to 64 KiB and close so the connection can be reused.
	defer httpx.DrainClose(resp.Body)
	return struct{}{}, deliveryError(resp)
}

// deliveryError reports what a response says about delivery: nil for success
// (exactly 2xx), otherwise CheckHTTPStatus's typed error plus whatever knell
// can safely add, so httpx.Do can still classify 502/503/504 and the 429 wait.
func deliveryError(resp *http.Response) error {
	statusErr := httpx.CheckHTTPStatus(resp)
	if statusErr == nil {
		return nil
	}
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		// A 3xx reaches here only because the hop was not followed; neither the
		// Location nor the request URL is included, since for a webhook the
		// path is the credential.
		return fmt.Errorf(
			"%w: redirect or other 3xx response was not followed, nothing was delivered (point DISCORD_WEBHOOK_URL at an endpoint that accepts the POST with a 2xx response)",
			statusErr,
		)
	}
	detail := statusDetail(resp.Body)
	if detail != "" {
		return fmt.Errorf("%w%s", statusErr, detail)
	}
	return statusErr
}

// statusDetail renders what a rejected response adds to its status code:
// Discord's numeric "code" field, and which of knell's own payload fields it
// blamed. The object's text fields are remote-authored and never published.
// The empty string means the status is the whole verdict.
func statusDetail(body io.Reader) string {
	// One byte past the cap, so an over-cap body is detectable and dropped
	// instead of being decoded as a whole one.
	detail, readErr := io.ReadAll(io.LimitReader(body, maxErrorBodyBytes+1))
	if readErr != nil || len(detail) > maxErrorBodyBytes {
		return ""
	}
	code, blamed, ok := discordError(detail)
	if !ok {
		return ""
	}
	if fields := blamedFields(blamed); fields != "" {
		return fmt.Sprintf(": Discord error code %d [%s]", code, fields)
	}
	return fmt.Sprintf(": Discord error code %d", code)
}

// discordError reports the numeric error code of a rejected response body and
// its "errors" object, still undecoded. Only the code is bound to a value this
// package formats; every other text field stays in the raw bytes.
func discordError(body []byte) (code int, blamed json.RawMessage, ok bool) {
	var parsed struct {
		Code   *int            `json:"code"`
		Errors json.RawMessage `json:"errors"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil || parsed.Code == nil {
		return 0, nil, false
	}
	return *parsed.Code, parsed.Errors, true
}

// blamedFields names which of knell's own payload fields Discord rejected, as
// dotted paths carrying the machine code reported at each. A value is
// published only after it is proved equal to one of this package's own
// literals or to a short index, and a code only after it matches the machine
// code class WHOLLY; anything else is DROPPED rather than rewritten, because a
// sanitizer that substitutes runes in remote text can assemble a credential
// out of bytes a needle would have missed. Control characters and newlines are
// therefore impossible by construction rather than filtered out.
func blamedFields(blamed json.RawMessage) string {
	var root map[string]any
	if len(blamed) == 0 || json.Unmarshal(blamed, &root) != nil {
		return ""
	}
	var w blameList
	w.walk(root, "", 0)
	if w.dropped {
		w.entries = append(w.entries, truncatedEntry)
	}
	return strings.Join(w.entries, "; ")
}

// truncatedEntry is the last entry of a list either cap cut short.
const truncatedEntry = "truncated"

// blameList accumulates the published paths under the entry-count and
// total-length caps.
type blameList struct {
	entries []string
	runes   int
	dropped bool
}

// add records one entry, or marks the list truncated and accepts nothing more.
// The truncation marker's own cost is always reserved, so a list that ends up
// cut still fits maxErrorDetailRunes.
func (w *blameList) add(entry string) {
	const separator = 2 // "; "
	const reserved = len(truncatedEntry) + separator
	if w.dropped {
		return
	}
	cost := len([]rune(entry))
	if w.runes > 0 {
		cost += separator
	}
	if len(w.entries) >= maxErrorFields || w.runes+cost+reserved > maxErrorDetailRunes {
		w.dropped = true
		return
	}
	w.entries = append(w.entries, entry)
	w.runes += cost
}

// walk descends one level of Discord's recursive errors object. Keys are
// sorted so one body always renders one line: map order would otherwise make
// the detail nondeterministic and untestable.
func (w *blameList) walk(fields map[string]any, path string, depth int) {
	if depth >= maxErrorFieldDepth {
		return
	}
	for _, key := range slices.Sorted(maps.Keys(fields)) {
		if w.dropped {
			return
		}
		if key == "_errors" {
			w.addBlame(path, fields[key])
			continue
		}
		if child, ok := fields[key].(map[string]any); ok {
			w.walk(child, joinPath(path, pathToken(key)), depth+1)
		}
	}
}

// addBlame records the path an _errors array blames, plus the first entry's
// machine code when it passes the class gate. A dropped code costs the code
// only: the path is still published.
func (w *blameList) addBlame(path string, reported any) {
	list, ok := reported.([]any)
	if !ok || len(list) == 0 {
		return
	}
	entry := path
	if entry == "" {
		// Discord's whole-message form: the errors object blames no field.
		entry = "_errors"
	}
	first, _ := list[0].(map[string]any)
	if code, ok := machineCode(first["code"]); ok {
		entry += ": " + code
	}
	w.add(entry)
}

func joinPath(path, token string) string {
	if path == "" {
		return token
	}
	return path + "." + token
}

// blamedFieldNames are the field names knell's own payload can be blamed for,
// sorted for slices.Contains readability. Publishing a key only after proving
// it equals one of these leaks membership, not content.
var blamedFieldNames = []string{
	"allowed_mentions", "color", "content", "description", "embeds",
	"fields", "inline", "name", "parse", "timestamp", "title", "value",
}

// pathToken renders one JSON key of the errors object, or "?" for a key this
// package's payload cannot have produced.
func pathToken(key string) string {
	if slices.Contains(blamedFieldNames, key) || isShortIndex(key) {
		return key
	}
	return "?"
}

// isShortIndex reports whether key is an index into a payload with one embed
// and at most 25 fields.
func isShortIndex(key string) bool {
	if key == "" || len(key) > 2 {
		return false
	}
	for _, r := range key {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// machineCode gates one _errors[].code for publication: upper-case ASCII,
// digits and underscores, first rune a letter, at least one underscore, and no
// digit run past maxErrorCodeDigitRun. The code is the one value here published
// as the remote sent it, so the class is what stands between a hostile endpoint
// and a knell log line: the underscore requirement refuses a bare token, the
// digit-run limit refuses a path id, and Discord's own codes carry neither.
func machineCode(reported any) (string, bool) {
	code, ok := reported.(string)
	if !ok || code == "" || len(code) > maxErrorCodeBytes {
		return "", false
	}
	if code[0] < 'A' || code[0] > 'Z' {
		return "", false
	}
	var underscores, digits int
	for i := range len(code) {
		switch c := code[i]; {
		case c >= '0' && c <= '9':
			digits++
			if digits > maxErrorCodeDigitRun {
				return "", false
			}
		case c >= 'A' && c <= 'Z':
			digits = 0
		case c == '_':
			digits = 0
			underscores++
		default:
			return "", false
		}
	}
	return code, underscores > 0
}
