package webapi

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cplieger/go/httpserver"
	"github.com/cplieger/go/slogx/capture"
	"github.com/cplieger/knell/internal/watch"
)

// Two tokens: tokenA covers beat a, tokenB covers beat b.
const (
	gateTokenA = "gate-test-token-for-beat-a"
	gateTokenB = "gate-test-token-for-beat-b"
)

// gateClock is a settable clock for the limiter.
type gateClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *gateClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *gateClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// newGateHandler builds the ping listener with tokenA for beat a and tokenB
// for beat b, on a limiter driven by clock.
func newGateHandler(t *testing.T, b *fakeBeater, clock *gateClock) http.Handler {
	t.Helper()
	set, err := httpserver.NewTokenSet([]string{gateTokenA, gateTokenB})
	if err != nil {
		t.Fatal(err)
	}
	limiter, err := newLimiter(time.Hour, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	return surface(New(b, &Deps{Tokens: set, Covers: [][]string{{"a"}, {"b"}}, Beats: []string{"a", "b"}, Limiter: limiter}),
		NewOps(OpsDeps{Healthz: staticHealthz(http.StatusOK)}))
}

func newGateFixture(t *testing.T) (*fakeBeater, *gateClock, http.Handler) {
	t.Helper()
	b := &fakeBeater{known: map[string]bool{"a": true, "b": true}}
	clock := &gateClock{now: time.Unix(1_700_000_000, 0)}
	return b, clock, newGateHandler(t, b, clock)
}

// ping sends POST /beat/<id> with token from client.
func ping(h http.Handler, client, id, token string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/beat/"+id, strings.NewReader(""))
	req.RemoteAddr = client + ":40000"
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// throttle spends client's own failure window.
func throttle(t *testing.T, h http.Handler, client string) {
	t.Helper()
	for i := range failuresPerKey {
		if rec := ping(h, client, "a", "wrong-token-wrong-token"); rec.Code != http.StatusUnauthorized {
			t.Fatalf("failure %d from %s = %d, want 401", i+1, client, rec.Code)
		}
	}
}

// fillGlobal spends what is left of the global window with bad tokens from
// fresh clients, until a fresh client is answered 429.
func fillGlobal(t *testing.T, h http.Handler) {
	t.Helper()
	for i := range failuresPerGlobal + 1 {
		client := "198.51.100." + strconv.Itoa(i/failuresPerKey+1)
		if rec := ping(h, client, "a", "wrong-token-wrong-token"); rec.Code == http.StatusTooManyRequests {
			if i%failuresPerKey == 0 {
				return
			}
			// A client throttled by its own window: start the next one.
			continue
		}
	}
	t.Fatal("the global window never filled")
}

// requestID matches the request id, the one field two 429s may differ in.
var requestID = regexp.MustCompile(`"request_id":"[0-9a-f]+"`)

// assertSame429 pins that two throttled answers are the same response, byte
// for byte apart from the request id.
func assertSame429(t *testing.T, right, wrong *httptest.ResponseRecorder) {
	t.Helper()
	if right.Code != http.StatusTooManyRequests || wrong.Code != http.StatusTooManyRequests {
		t.Fatalf("statuses = %d and %d, want 429 for both", right.Code, wrong.Code)
	}
	for _, h := range []string{"Content-Type", "Content-Length", "Retry-After", "Cache-Control", "WWW-Authenticate", "Connection"} {
		if r, w := right.Header().Get(h), wrong.Header().Get(h); r != w {
			t.Errorf("%s = %q for the right token and %q for the wrong one", h, r, w)
		}
	}
	if len(right.Header()) != len(wrong.Header()) {
		t.Errorf("header sets differ: %v and %v", right.Header(), wrong.Header())
	}
	r := requestID.ReplaceAllString(right.Body.String(), "")
	w := requestID.ReplaceAllString(wrong.Body.String(), "")
	if r != w {
		t.Errorf("bodies differ: %q and %q", r, w)
	}
}

func TestPerKeyLimitThrottlesAfterTenFailures(t *testing.T) {
	b, _, h := newGateFixture(t)
	before := scrapeExposition(t, h)
	throttle(t, h, "192.0.2.1")
	rec := ping(h, "192.0.2.1", "a", "wrong-token-wrong-token")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("failure %d = %d, want 429", failuresPerKey+1, rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "too_many_auth_failures") {
		t.Errorf("429 body = %s, want the too_many_auth_failures code", rec.Body.String())
	}
	if got, err := strconv.Atoi(rec.Header().Get("Retry-After")); err != nil || got < 1 || got > int(failureWindow.Seconds()) {
		t.Errorf("Retry-After = %q, want whole seconds within the window", rec.Header().Get("Retry-After"))
	}
	after := scrapeExposition(t, h)
	for series, want := range map[string]float64{
		`knell_auth_failures_total{reason="invalid_token"}`: failuresPerKey,
		`knell_auth_failures_total{reason="throttled"}`:     1,
	} {
		if got := mustSeriesValue(t, after, series) - mustSeriesValue(t, before, series); got != want {
			t.Errorf("%s moved by %v, want %v", series, got, want)
		}
	}
	if rec := ping(h, "192.0.2.2", "a", "wrong-token-wrong-token"); rec.Code != http.StatusUnauthorized {
		t.Errorf("another client = %d, want 401: the limit is per client", rec.Code)
	}
	if len(b.seen) != 0 {
		t.Errorf("recorded %v, want nothing", b.seen)
	}
}

// TestThrottledRightTokenIsRecordedBehindTheSame429 pins the rule the limiter
// rests on: a throttled client gets one 429 whatever it sends, so a guess is
// never confirmed, and a covering ping is still recorded, so throttling never
// causes a missing notice.
func TestThrottledRightTokenIsRecordedBehindTheSame429(t *testing.T) {
	b, _, h := newGateFixture(t)
	throttle(t, h, "192.0.2.1")
	before := scrapeExposition(t, h)

	right := ping(h, "192.0.2.1", "a", gateTokenA)
	wrong := ping(h, "192.0.2.1", "a", "wrong-token-wrong-token")
	notCovered := ping(h, "192.0.2.1", "a", gateTokenB)
	const unknown = "gate-throttled-unknown-id"
	unknownID := ping(h, "192.0.2.1", unknown, gateTokenA)
	assertSame429(t, right, wrong)
	assertSame429(t, notCovered, wrong)
	assertSame429(t, unknownID, wrong)

	if len(b.seen) != 1 || b.seen[0] != "a" {
		t.Errorf("recorded %v, want exactly the one covering ping for a", b.seen)
	}
	after := scrapeExposition(t, h)
	// The fake beater mints no per-beat series, so an absent series is 0.
	const series = `knell_beats_recorded_while_throttled_total{beat="a"}`
	was, _ := seriesValue(t, before, series)
	if got := mustSeriesValue(t, after, series) - was; got != 1 {
		t.Errorf("recorded_while_throttled{a} moved by %v, want 1", got)
	}
	if strings.Contains(after, `beat="`+unknown+`"`) {
		t.Error("a throttled unknown id minted a series")
	}
}

// TestThrottledAnswerIsTheSameOnTheWire pins the parts of a throttled answer
// net/http decides from an unread body: a 300 KiB body and an Expect:
// 100-continue exchange answer alike for a right and a wrong token.
func TestThrottledAnswerIsTheSameOnTheWire(t *testing.T) {
	_, _, h := newGateFixture(t)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	for range failuresPerKey {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+"/beat/a", nil)
		req.Header.Set("Authorization", "Bearer wrong-token-wrong-token")
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}
	send := func(token string, expect bool) (firstLine string, closed bool) {
		conn, err := net.Dial("tcp", srv.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		const size = 300 << 10
		head := "POST /beat/a HTTP/1.1\r\nHost: knell\r\nAuthorization: Bearer " + token +
			"\r\nContent-Length: " + strconv.Itoa(size) + "\r\n"
		if expect {
			head += "Expect: 100-continue\r\n"
		}
		if _, err := io.WriteString(conn, head+"\r\n"); err != nil {
			t.Fatal(err)
		}
		if !expect {
			go func() { _, _ = conn.Write(make([]byte, size)) }()
		}
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.Status, resp.Close
	}
	for _, expect := range []bool{false, true} {
		rs, rc := send(gateTokenA, expect)
		ws, wc := send("wrong-token-wrong-token", expect)
		if rs != ws || rc != wc {
			t.Errorf("expect=%t: right token answered %q close=%t, wrong token %q close=%t", expect, rs, rc, ws, wc)
		}
		if !strings.HasPrefix(rs, "429") {
			t.Errorf("expect=%t: answered %q, want 429", expect, rs)
		}
	}
}

func TestVerdictsWhenNotThrottled(t *testing.T) {
	b, _, h := newGateFixture(t)
	before := scrapeExposition(t, h)
	for name, tc := range map[string]struct {
		id, token string
		want      int
	}{
		"a covering token":           {"a", gateTokenA, http.StatusOK},
		"a token for another beat":   {"b", gateTokenA, http.StatusForbidden},
		"a valid token, unknown id":  {"ghost", gateTokenA, http.StatusNotFound},
		"a wrong token, unknown id":  {"ghost", "wrong-token-wrong-token", http.StatusUnauthorized},
		"a wrong token, known id":    {"b", "wrong-token-wrong-token", http.StatusUnauthorized},
		"no token":                   {"a", "", http.StatusUnauthorized},
		"an oversize Authorization":  {"a", strings.Repeat("x", httpserver.MaxTokenBytes+1), http.StatusUnauthorized},
		"a token as a prefix of one": {"a", gateTokenA[:len(gateTokenA)-1], http.StatusUnauthorized},
	} {
		t.Run(name, func(t *testing.T) {
			if rec := ping(h, "192.0.2.50", tc.id, tc.token); rec.Code != tc.want {
				t.Errorf("= %d, want %d (body %s)", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
	if len(b.seen) != 1 || b.seen[0] != "a" {
		t.Errorf("recorded %v, want only the covering ping", b.seen)
	}
	after := scrapeExposition(t, h)
	if got := mustSeriesValue(t, after, `knell_auth_failures_total{reason="beat_not_allowed"}`) -
		mustSeriesValue(t, before, `knell_auth_failures_total{reason="beat_not_allowed"}`); got != 1 {
		t.Errorf("beat_not_allowed moved by %v, want 1", got)
	}
}

// TestConcurrentFailuresCannotPassAFullWindowTogether pins the one limiter
// decision per request: concurrent bad tokens from one client get exactly
// failuresPerKey answers and the rest 429.
func TestConcurrentFailuresCannotPassAFullWindowTogether(t *testing.T) {
	_, _, h := newGateFixture(t)
	const n = 50
	codes := make(chan int, n)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() { codes <- ping(h, "192.0.2.7", "a", "wrong-token-wrong-token").Code })
	}
	wg.Wait()
	close(codes)
	count := map[int]int{}
	for c := range codes {
		count[c]++
	}
	if count[http.StatusUnauthorized] != failuresPerKey || count[http.StatusTooManyRequests] != n-failuresPerKey {
		t.Errorf("answers = %v, want %d 401 and %d 429", count, failuresPerKey, n-failuresPerKey)
	}
}

// TestGlobalWindowSparesTrustedSenders pins the global window: once a spray
// spends it, a client that is not trusted is throttled, and its covering ping
// still recorded; a sender with a recent recorded ping keeps its 200 until it
// fails once.
func TestGlobalWindowSparesTrustedSenders(t *testing.T) {
	b, _, h := newGateFixture(t)
	if rec := ping(h, "203.0.113.1", "a", gateTokenA); rec.Code != http.StatusOK {
		t.Fatalf("trusted sender's first ping = %d, want 200", rec.Code)
	}
	if rec := ping(h, "203.0.113.3", "ghost", gateTokenA); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown id = %d, want 404", rec.Code)
	}
	fillGlobal(t, h)

	if rec := ping(h, "203.0.113.1", "a", gateTokenA); rec.Code != http.StatusOK {
		t.Errorf("trusted sender under a full global window = %d, want 200", rec.Code)
	}
	if rec := ping(h, "203.0.113.2", "a", gateTokenA); rec.Code != http.StatusTooManyRequests {
		t.Errorf("new sender under a full global window = %d, want 429", rec.Code)
	}
	if rec := ping(h, "203.0.113.3", "a", gateTokenA); rec.Code != http.StatusTooManyRequests {
		t.Errorf("a 404 for an unknown id earned trust: = %d, want 429", rec.Code)
	}
	if rec := ping(h, "203.0.113.4", "a", "wrong-token-wrong-token"); rec.Code != http.StatusTooManyRequests {
		t.Errorf("new client's bad token under a full global window = %d, want 429", rec.Code)
	}
	// An exempt sender's failure is checked and answered, and costs it the
	// exemption: its next ping is held to the full global window.
	if rec := ping(h, "203.0.113.1", "b", gateTokenA); rec.Code != http.StatusForbidden {
		t.Errorf("trusted sender's failure under a full global window = %d, want 403", rec.Code)
	}
	if rec := ping(h, "203.0.113.1", "a", gateTokenA); rec.Code != http.StatusTooManyRequests {
		t.Errorf("trusted sender after its failure = %d, want 429", rec.Code)
	}
	if got := len(b.seen); got != 5 {
		t.Errorf("recorded %d pings %v, want 5: every covering ping counts, 200 or 429", got, b.seen)
	}
}

// TestTrustedSenderLosesItsExemptionAtItsFirstFailure pins that an exempt key
// with a failure in its own window is held to the global window again.
func TestTrustedSenderLosesItsExemptionAtItsFirstFailure(t *testing.T) {
	_, _, h := newGateFixture(t)
	if rec := ping(h, "203.0.113.1", "a", gateTokenA); rec.Code != http.StatusOK {
		t.Fatalf("first ping = %d, want 200", rec.Code)
	}
	if rec := ping(h, "203.0.113.1", "b", gateTokenA); rec.Code != http.StatusForbidden {
		t.Fatalf("wrong beat = %d, want 403", rec.Code)
	}
	fillGlobal(t, h)
	if rec := ping(h, "203.0.113.1", "a", gateTokenA); rec.Code != http.StatusTooManyRequests {
		t.Errorf("sender with a failure in its window under a full global window = %d, want 429", rec.Code)
	}
}

// TestTrustExpiresAfterTheTrustWindow pins that trust lasts the trust window
// from the last recorded ping, and a ping inside it refreshes it.
func TestTrustExpiresAfterTheTrustWindow(t *testing.T) {
	_, clock, h := newGateFixture(t)
	if rec := ping(h, "203.0.113.1", "a", gateTokenA); rec.Code != http.StatusOK {
		t.Fatalf("first ping = %d", rec.Code)
	}
	clock.advance(50 * time.Minute)
	if rec := ping(h, "203.0.113.1", "a", gateTokenA); rec.Code != http.StatusOK {
		t.Fatalf("refreshing ping = %d", rec.Code)
	}
	clock.advance(50 * time.Minute)
	fillGlobal(t, h)
	if rec := ping(h, "203.0.113.1", "a", gateTokenA); rec.Code != http.StatusOK {
		t.Errorf("past the first mark plus the window, inside the refreshed one = %d, want 200", rec.Code)
	}
	clock.advance(61 * time.Minute)
	fillGlobal(t, h)
	if rec := ping(h, "203.0.113.1", "a", gateTokenA); rec.Code != http.StatusTooManyRequests {
		t.Errorf("past the refreshed window = %d, want 429", rec.Code)
	}
}

// TestUntrustedForwardingWarnsOnce pins the one unsafe setup knell can see:
// a forwarded ping from a peer outside TRUSTED_PROXIES.
func TestUntrustedForwardingWarnsOnce(t *testing.T) {
	logs := capture.Default(t)
	_, _, h := newGateFixture(t)
	for range 3 {
		req := httptest.NewRequest(http.MethodPost, "/beat/a", nil)
		req.RemoteAddr = "192.0.2.9:40000"
		req.Header.Set("X-Forwarded-For", "203.0.113.200")
		req.Header.Set("Authorization", "Bearer "+gateTokenA)
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
	if got := logs.CountExact("a ping came through a proxy knell does not trust; set TRUSTED_PROXIES to its address, or every client behind it shares one failure limit"); got != 1 {
		t.Errorf("untrusted-proxy warnings = %d, want exactly 1", got)
	}
}

// blockingBeater records a ping only once release is closed.
type blockingBeater struct {
	fakeBeater
	entered chan struct{}
	release chan struct{}
	enter   sync.Once
}

func (b *blockingBeater) Beat(id string) watch.BeatOutcome {
	b.enter.Do(func() { close(b.entered) })
	<-b.release
	return b.fakeBeater.Beat(id)
}

// TestThrottledAnswerReachesTheClientBeforeTheRecord pins the order that
// keeps a throttled answer verdict-independent: the whole 429, with a fixed
// length, is on the wire before Beat runs, so a right token's answer cannot
// arrive later than a wrong one's.
func TestThrottledAnswerReachesTheClientBeforeTheRecord(t *testing.T) {
	b := &blockingBeater{entered: make(chan struct{}), release: make(chan struct{})}
	b.known = map[string]bool{"a": true, "b": true}
	clock := &gateClock{now: time.Unix(1_700_000_000, 0)}
	set, err := httpserver.NewTokenSet([]string{gateTokenA, gateTokenB})
	if err != nil {
		t.Fatal(err)
	}
	limiter, err := newLimiter(time.Hour, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(b, &Deps{Tokens: set, Covers: [][]string{{"a"}, {"b"}}, Beats: []string{"a", "b"}, Limiter: limiter}))
	t.Cleanup(srv.Close)
	release := sync.OnceFunc(func() { close(b.release) })
	t.Cleanup(release)
	post := func(token string) (*http.Response, error) {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+"/beat/a", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		return srv.Client().Do(req)
	}
	for range failuresPerKey {
		resp, err := post("wrong-token-wrong-token")
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}
	got := make(chan error, 1)
	go func() {
		resp, err := post(gateTokenA)
		if err != nil {
			got <- err
			return
		}
		defer func() { _ = resp.Body.Close() }()
		if _, err := io.ReadAll(resp.Body); err != nil {
			got <- err
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests || resp.ContentLength <= 0 || len(resp.TransferEncoding) > 0 {
			got <- fmt.Errorf("answer = %d, length %d, transfer encoding %v; want a 429 with a fixed length", resp.StatusCode, resp.ContentLength, resp.TransferEncoding)
			return
		}
		got <- nil
	}()
	// Beat is held until release, so the answer must arrive with Beat either
	// not yet entered or still blocked; either order is fine, both must happen.
	answered, entered := false, false
	for !answered || !entered {
		select {
		case <-b.entered:
			entered, b.entered = true, nil
		case err := <-got:
			if err != nil {
				t.Fatal(err)
			}
			answered = true
		case <-time.After(5 * time.Second):
			t.Fatalf("answered=%t, Beat entered=%t: the whole 429 must reach the client while Beat is held, and Beat must run for a throttled covering ping", answered, entered)
		}
	}
	release()
}

// TestTrustIsHeldPerToken pins that one token's senders replace only that
// token's own trusted senders: a 17th key on token A drops A's oldest, never
// token B's sender.
func TestTrustIsHeldPerToken(t *testing.T) {
	_, clock, h := newGateFixture(t)
	if rec := ping(h, "203.0.113.200", "b", gateTokenB); rec.Code != http.StatusOK {
		t.Fatalf("token B's sender = %d, want 200", rec.Code)
	}
	for i := range trustedPerToken + 1 {
		clock.advance(time.Second)
		if rec := ping(h, "203.0.113."+strconv.Itoa(i+1), "a", gateTokenA); rec.Code != http.StatusOK {
			t.Fatalf("token A sender %d = %d, want 200", i+1, rec.Code)
		}
	}
	fillGlobal(t, h)
	if rec := ping(h, "203.0.113.200", "b", gateTokenB); rec.Code != http.StatusOK {
		t.Errorf("token B's sender after 17 senders of token A = %d, want 200: one token's senders may not evict another's", rec.Code)
	}
	if rec := ping(h, "203.0.113.17", "a", gateTokenA); rec.Code != http.StatusOK {
		t.Errorf("token A's newest sender = %d, want 200", rec.Code)
	}
	if rec := ping(h, "203.0.113.1", "a", gateTokenA); rec.Code != http.StatusTooManyRequests {
		t.Errorf("token A's oldest sender = %d, want 429: it was replaced by the 17th", rec.Code)
	}
}

// TestVerdictCountsAndCharges pins what each verdict charges and counts: a
// valid token on an unknown id charges nothing, a throttled covering ping is
// no throttled failure, a refused record counts nothing and earns no trust.
func TestVerdictCountsAndCharges(t *testing.T) {
	t.Run("an unknown id with a valid token charges nothing", func(t *testing.T) {
		_, _, h := newGateFixture(t)
		for i := range failuresPerKey + 2 {
			if rec := ping(h, "192.0.2.30", "ghost", gateTokenA); rec.Code != http.StatusNotFound {
				t.Fatalf("typo'd ping %d = %d, want 404", i+1, rec.Code)
			}
		}
		if rec := ping(h, "192.0.2.30", "a", gateTokenA); rec.Code != http.StatusOK {
			t.Errorf("the same client's covering ping = %d, want 200: a typo'd URL with a valid token must not throttle its sender", rec.Code)
		}
	})
	t.Run("a throttled covering ping is no throttled failure", func(t *testing.T) {
		_, _, h := newGateFixture(t)
		throttle(t, h, "192.0.2.31")
		const series = `knell_auth_failures_total{reason="throttled"}`
		before := mustSeriesValue(t, scrapeExposition(t, h), series)
		ping(h, "192.0.2.31", "a", gateTokenA)
		if got := mustSeriesValue(t, scrapeExposition(t, h), series); got != before {
			t.Errorf("%s moved by %v for a covering ping, want 0", series, got-before)
		}
	})
	t.Run("a refused record counts nothing and earns no trust", func(t *testing.T) {
		b, _, h := newGateFixture(t)
		b.closed = true
		const series = `knell_beats_recorded_while_throttled_total{beat="a"}`
		if rec := ping(h, "192.0.2.32", "a", gateTokenA); rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("ping during shutdown = %d, want 503", rec.Code)
		}
		throttle(t, h, "192.0.2.33")
		before, _ := seriesValue(t, scrapeExposition(t, h), series)
		ping(h, "192.0.2.33", "a", gateTokenA)
		if got, _ := seriesValue(t, scrapeExposition(t, h), series); got != before {
			t.Errorf("%s moved for a throttled ping knell did not record", series)
		}
		b.closed = false
		fillGlobal(t, h)
		if rec := ping(h, "192.0.2.32", "a", gateTokenA); rec.Code != http.StatusTooManyRequests {
			t.Errorf("client whose only covering ping was answered 503 = %d, want 429: a 503 earns no trust", rec.Code)
		}
	})
}

// TestUntrustedForwardingNeedsAToken pins that a client without a valid token
// can neither raise nor use up the untrusted-proxy warning.
func TestUntrustedForwardingNeedsAToken(t *testing.T) {
	logs := capture.Default(t)
	_, _, h := newGateFixture(t)
	const warning = "a ping came through a proxy knell does not trust; set TRUSTED_PROXIES to its address, or every client behind it shares one failure limit"
	forwarded := func(token string) {
		req := httptest.NewRequest(http.MethodPost, "/beat/a", nil)
		req.RemoteAddr = "192.0.2.9:40000"
		req.Header.Set("X-Forwarded-For", "8.8.8.8")
		req.Header.Set("Authorization", "Bearer "+token)
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
	forwarded("wrong-token-wrong-token")
	if got := logs.CountExact(warning); got != 0 {
		t.Fatalf("a forged header with a wrong token raised %d warnings, want 0", got)
	}
	forwarded(gateTokenA)
	if got := logs.CountExact(warning); got != 1 {
		t.Errorf("a relayed ping with a valid token raised %d warnings, want 1", got)
	}
}
