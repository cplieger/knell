package obs

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

const (
	dashboardFile = "../../grafana-dashboard.json"
	promqlRules   = "../../alerts/promql.yaml"
	promqlTests   = "../../alerts/promql_test.yaml"
)

type dashQuery struct {
	Spec struct {
		Query struct {
			Spec struct {
				Expr         string `json:"expr"`
				LegendFormat string `json:"legendFormat"`
			} `json:"spec"`
		} `json:"query"`
	} `json:"spec"`
}

type dashPanel struct {
	Spec struct {
		Title       string `json:"title"`
		Description string `json:"description"`
		Data        struct {
			Spec struct {
				Queries         []dashQuery      `json:"queries"`
				Transformations []transformation `json:"transformations"`
				QueryOptions    struct {
					MaxDataPoints int `json:"maxDataPoints"`
				} `json:"queryOptions"`
			} `json:"spec"`
		} `json:"data"`
		VizConfig struct {
			Group string `json:"group"`
			Spec  struct {
				FieldConfig struct {
					Defaults struct {
						NoValue string `json:"noValue"`
					} `json:"defaults"`
					Overrides []struct {
						Properties []struct {
							ID    string `json:"id"`
							Value any    `json:"value"`
						} `json:"properties"`
					} `json:"overrides"`
				} `json:"fieldConfig"`
			} `json:"spec"`
		} `json:"vizConfig"`
	} `json:"spec"`
}

type transformation struct {
	Group string `json:"group"`
}

type conditionRule struct {
	Kind string `json:"kind"`
	Spec struct {
		Value    any    `json:"value"`
		Variable string `json:"variable"`
		Operator string `json:"operator"`
	} `json:"spec"`
}

type conditionGroup struct {
	Spec struct {
		Visibility string          `json:"visibility"`
		Condition  string          `json:"condition"`
		Items      []conditionRule `json:"items"`
	} `json:"spec"`
}

type gridItem struct {
	Spec struct {
		ConditionalRendering *conditionGroup `json:"conditionalRendering"`
		Element              struct {
			Name string `json:"name"`
		} `json:"element"`
	} `json:"spec"`
}

type layoutNode struct {
	Spec struct {
		Layout               *layoutNode     `json:"layout"`
		ConditionalRendering *conditionGroup `json:"conditionalRendering"`
		Title                string          `json:"title"`
		Tabs                 []layoutNode    `json:"tabs"`
		Rows                 []layoutNode    `json:"rows"`
		Items                []gridItem      `json:"items"`
		HideHeader           bool            `json:"hideHeader"`
	} `json:"spec"`
}

type dashVariable struct {
	Kind string `json:"kind"`
	Spec struct {
		Name        string `json:"name"`
		Hide        string `json:"hide"`
		Refresh     string `json:"refresh"`
		Regex       string `json:"regex"`
		Definition  string `json:"definition"`
		SkipURLSync bool   `json:"skipUrlSync"`
		Current     struct {
			Text  any `json:"text"`
			Value any `json:"value"`
		} `json:"current"`
		// Query is a data query on a QueryVariable and a string on a
		// CustomVariable.
		Query json.RawMessage `json:"query"`
	} `json:"spec"`
}

type dashboard struct {
	Spec struct {
		Elements  map[string]dashPanel `json:"elements"`
		Layout    layoutNode           `json:"layout"`
		Variables []dashVariable       `json:"variables"`
		Preload   bool                 `json:"preload"`
	} `json:"spec"`
}

func loadDashboard(t *testing.T) dashboard {
	t.Helper()
	raw, err := os.ReadFile(dashboardFile)
	if err != nil {
		t.Fatalf("reading %s: %v", dashboardFile, err)
	}
	var d dashboard
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatalf("decoding %s: %v", dashboardFile, err)
	}
	if len(d.Spec.Elements) == 0 {
		t.Fatalf("%s holds no panel", dashboardFile)
	}
	return d
}

func panelExprs(p *dashPanel) []string {
	var out []string
	for _, q := range p.Spec.Data.Spec.Queries {
		out = append(out, q.Spec.Query.Spec.Expr)
	}
	return out
}

func (d *dashboard) variables() map[string]dashVariable {
	out := map[string]dashVariable{}
	for _, v := range d.Spec.Variables {
		out[v.Spec.Name] = v
	}
	return out
}

// variableQuery is a query variable's PromQL, or "" for any other variable.
func variableQuery(v *dashVariable) string {
	var q struct {
		Spec struct {
			Query string `json:"query"`
		} `json:"spec"`
	}
	if v.Kind != "QueryVariable" || json.Unmarshal(v.Spec.Query, &q) != nil {
		return ""
	}
	return q.Spec.Query
}

// dashboardQueries is every variable and panel query, keyed by where it sits.
func dashboardQueries(d *dashboard) map[string]string {
	queries := map[string]string{}
	for name, v := range d.variables() {
		queries["variable "+name] = variableQuery(&v)
	}
	for name, p := range d.Spec.Elements {
		for i, e := range panelExprs(&p) {
			queries[fmt.Sprintf("%s %s query %d", name, p.Spec.Title, i)] = e
		}
	}
	return queries
}

func tab(t *testing.T, d *dashboard, title string) *layoutNode {
	t.Helper()
	for i := range d.Spec.Layout.Spec.Tabs {
		if tb := &d.Spec.Layout.Spec.Tabs[i]; tb.Spec.Title == title {
			return tb
		}
	}
	t.Fatalf("no tab titled %q", title)
	return nil
}

func row(t *testing.T, tb *layoutNode, title string) *layoutNode {
	t.Helper()
	for i := range tb.Spec.Layout.Spec.Rows {
		if r := &tb.Spec.Layout.Spec.Rows[i]; r.Spec.Title == title {
			return r
		}
	}
	t.Fatalf("tab %q has no row titled %q", tb.Spec.Title, title)
	return nil
}

// servedSeries scrapes Handler with one beat configured and returns each
// series name it serves with the label names that series carries.
func servedSeries(t *testing.T) map[string][]string {
	t.Helper()
	InitBeat("dashboard-beat", time.Minute, time.Unix(1700000000, 0))
	SetBuildInfo("v0.0.0-dashboard")
	t.Cleanup(func() { buildInfo.Delete("v0.0.0-dashboard") })
	SetBeatFresh("dashboard-beat", true)
	RecordHTTP(http.MethodGet, "/beat/{id}", http.StatusOK, time.Millisecond)
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	served := map[string][]string{}
	sample := regexp.MustCompile(`^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})? `)
	for line := range strings.SplitSeq(rec.Body.String(), "\n") {
		m := sample.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		labels := served[m[1]]
		for _, pair := range regexp.MustCompile(`(\w+)="`).FindAllStringSubmatch(m[2], -1) {
			if !slices.Contains(labels, pair[1]) {
				labels = append(labels, pair[1])
			}
		}
		served[m[1]] = labels
	}
	if _, ok := served["knell_beat_fresh"]; !ok {
		t.Fatalf("Setup: the scrape serves no knell_beat_fresh: %s", rec.Body.String())
	}
	return served
}

var (
	quotedRe      = regexp.MustCompile(`"(?:[^"\\]|\\.)*"`)
	matchersRe    = regexp.MustCompile(`\{[^}]*\}`)
	groupingRe    = regexp.MustCompile(`\b(by|without|on|ignoring|group_left|group_right)\s*\(([^)]*)\)`)
	rangeRe       = regexp.MustCompile(`\[[^\]]*\]`)
	variableRe    = regexp.MustCompile(`\$\{?\w+(?::\w+)?\}?`)
	labelValuesRe = regexp.MustCompile(`label_values\(([^,]+),\s*\w+\)`)
	functionRe    = regexp.MustCompile(`\b[a-z_]+\s*\(`)
	identRe       = regexp.MustCompile(`\b[a-zA-Z_:][a-zA-Z0-9_:]*\b`)
	alertsRe      = regexp.MustCompile(`ALERTS\{alertname="([A-Za-z]+)"([^}]*)\}`)
	ruleNameRe    = regexp.MustCompile(`(?m)^\s*- alert: (\w+)$`)
	ruleRe        = regexp.MustCompile(`(?ms)^\s*- alert: (\w+)\n\s*expr: (.*?)\n\s*for:`)
)

var promqlKeywords = []string{"and", "or", "unless", "bool", "offset", "group_left", "group_right"}

// seriesIn returns the series names a panel or variable query reads;
// label_values names a label after its series, which is no series.
func seriesIn(expr string) []string {
	expr = labelValuesRe.ReplaceAllString(expr, "$1")
	for _, re := range []*regexp.Regexp{matchersRe, quotedRe, groupingRe, rangeRe, variableRe, functionRe} {
		expr = re.ReplaceAllString(expr, " ")
	}
	var names []string
	for _, id := range identRe.FindAllString(expr, -1) {
		if !slices.Contains(promqlKeywords, id) && !slices.Contains(names, id) {
			names = append(names, id)
		}
	}
	return names
}

// TestDashboardReadsOnlySeriesKnellServes fails when a panel or a variable
// reads a series or a label knell does not serve, so a renamed series fails
// the build instead of blanking a panel. up is the scrape's own series and
// ALERTS the ruler's; job and instance are the scrape's labels.
func TestDashboardReadsOnlySeriesKnellServes(t *testing.T) {
	served := servedSeries(t)
	d := loadDashboard(t)
	queries := dashboardQueries(&d)
	scrape := []string{"job", "instance", "le"}
	for where, expr := range queries {
		if hasLineComment(expr) {
			t.Errorf("%s holds a # comment, which the series checks cannot read: %s", where, expr)
		}
		var labels []string
		for _, name := range seriesIn(expr) {
			switch have, ok := served[name]; {
			case name == "up" || name == "ALERTS":
			case !ok:
				t.Errorf("%s reads %s, which knell does not serve", where, name)
			default:
				labels = append(labels, have...)
			}
		}
		for _, sel := range selectors(bracedVariableRe.ReplaceAllString(expr, "$$v")) {
			if sel.name == "" {
				t.Errorf("%s has a selector with no series name before its braces, which the series checks cannot read: %s", where, expr)
			}
			have, ok := served[sel.name]
			if !ok {
				continue
			}
			for _, part := range sel.parts {
				m := matcherPartRe.FindStringSubmatch(part)
				if m == nil {
					continue
				}
				label, err := labelName(m[1])
				if err != nil {
					t.Errorf("%s quotes the label name %s on %s in a form PromQL does not decode", where, m[1], sel.name)
					continue
				}
				if !slices.Contains(have, label) && !slices.Contains(scrape, label) {
					t.Errorf("%s matches %s on %s, which carries no such label", where, label, sel.name)
				}
			}
		}
		for _, g := range groupingRe.FindAllStringSubmatch(expr, -1) {
			for l := range strings.SplitSeq(g[2], ",") {
				l = strings.TrimSpace(l)
				// A label the query names as a string is one label_replace makes.
				computed := strings.Contains(expr, `"`+l+`"`)
				if l != "" && !computed && !slices.Contains(labels, l) && !slices.Contains(scrape, l) {
					t.Errorf("%s groups by %s, which no series it reads carries", where, l)
				}
			}
		}
	}
}

var (
	// bracedVariableRe matches ${name}, whose braces are no selector.
	bracedVariableRe = regexp.MustCompile(`\$\{\w+(?::\w+)?\}`)
	// matcherPartRe matches one label matcher. PromQL lets the label name be
	// quoted and takes a string in any of its three quotings.
	matcherPartRe = regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_]*|` + promqlString + `)\s*(=~|!~|!=|=)\s*(` + promqlString + `)$`)
)

const promqlString = `"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|` + "`[^`]*`"

// skipString returns the index just past the string literal opening at s[i].
func skipString(s string, i int) int {
	q := s[i]
	for i++; i < len(s); i++ {
		switch {
		case s[i] == '\\' && q != '`':
			i++
		case s[i] == q:
			return i + 1
		}
	}
	return len(s)
}

// labelName decodes a quoted label name as PromQL does, so "inst\x61nce"
// names instance:
// https://prometheus.io/docs/prometheus/latest/querying/basics/#string-literals
func labelName(s string) (string, error) {
	switch s[0] {
	case '"':
		return strconv.Unquote(s)
	case '`':
		return s[1 : len(s)-1], nil
	case '\'':
		var b strings.Builder
		for body := s[1 : len(s)-1]; body != ""; {
			r, multibyte, tail, err := strconv.UnquoteChar(body, '\'')
			if err != nil {
				return "", err
			}
			// strconv.Unquote's rule: \x80 to \xff and octal escapes are one byte.
			if r < utf8.RuneSelf || !multibyte {
				b.WriteByte(byte(r))
			} else {
				b.WriteRune(r)
			}
			body = tail
		}
		return b.String(), nil
	}
	return s, nil
}

// hasLineComment reports a # outside a string literal, which PromQL reads as
// a comment to the end of the line; selectors would read a quote in it as a
// string opening.
func hasLineComment(expr string) bool {
	for i := 0; i < len(expr); i++ {
		switch expr[i] {
		case '"', '\'', '`':
			i = skipString(expr, i) - 1
		case '#':
			return true
		}
	}
	return false
}

type selectorBlock struct {
	name  string
	parts []string
}

// selectors returns each {...} block of expr with the series name before it,
// "" when none, a keyword (x or {...}) or a variable ($v{...}) is, split at
// its commas. Quoted strings are skipped, so a brace or comma inside a label
// value neither ends a block nor splits a matcher.
func selectors(expr string) []selectorBlock {
	var out []selectorBlock
	start, part := -1, 0
	var parts []string
	for i := 0; i < len(expr); i++ {
		switch c := expr[i]; {
		case c == '"' || c == '\'' || c == '`':
			i = skipString(expr, i) - 1
		case c == '{':
			start, part, parts = i, i+1, nil
		case (c == ',' || c == '}') && start >= 0:
			if p := strings.TrimSpace(expr[part:i]); p != "" {
				parts = append(parts, p)
			}
			part = i + 1
			if c == '}' {
				before := strings.TrimRight(expr[:start], " ")
				name := before[len(strings.TrimRightFunc(before, func(r rune) bool {
					return r == '_' || r == ':' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
				})):]
				if slices.Contains(promqlKeywords, name) || strings.HasSuffix(before[:len(before)-len(name)], "$") {
					name = ""
				}
				out = append(out, selectorBlock{name: name, parts: parts})
				start = -1
			}
		}
	}
	return out
}

// TestDashboardQueriesAreTheTestedOnes fails when a panel query differs from
// the one alerts/promql_test.yaml evaluates under its anchor, with $instance
// as knell:9191 and $__interval as the anchor's slot: 1m, the scrape interval
// where a slot holds one sample, or 5m, where a counter reset can recover
// inside the slot. A change to either runs through promtool in CI.
func TestDashboardQueriesAreTheTestedOnes(t *testing.T) {
	d := loadDashboard(t)
	raw, err := os.ReadFile(promqlTests)
	if err != nil {
		t.Fatalf("reading %s: %v", promqlTests, err)
	}
	for _, tc := range []struct{ title, anchor, slot string }{
		{"Requests by status", "requests_by_status", "1m"},
		{"Requests by status", "requests_by_status_5m", "5m"},
		{"Notices sent", "notices_sent", "1m"},
	} {
		t.Run(tc.anchor, func(t *testing.T) {
			var exprs []string
			for _, p := range d.Spec.Elements {
				if p.Spec.Title == tc.title {
					exprs = append(exprs, panelExprs(&p)...)
				}
			}
			if len(exprs) != 1 {
				t.Fatalf("found %d %s queries, want 1", len(exprs), tc.title)
			}
			m := regexp.MustCompile(`(?m)expr: &` + tc.anchor + ` '([^']*)'$`).FindSubmatch(raw)
			if m == nil {
				t.Fatalf("%s has no expr anchored as %s", promqlTests, tc.anchor)
			}
			fill := strings.NewReplacer(`$instance`, "knell:9191", `$__interval`, tc.slot)
			if want := fill.Replace(exprs[0]); string(m[1]) != want {
				t.Errorf("%s tests\n%s\nthe dashboard reads, with its variables filled in,\n%s", promqlTests, m[1], want)
			}
		})
	}
}

// knellJoinRe is the join that keeps only knell's series: and on (job,
// instance) to knell_build_info, the shape KnellRestartChurn uses.
var knellJoinRe = regexp.MustCompile(`\band\s+on\s*\(\s*(?:job\s*,\s*instance|instance\s*,\s*job)\s*\)\s*(?:last_over_time\s*\(\s*)?knell_build_info\b`)

// TestDashboardScopesSharedFamiliesToKnell fails when a query reads up or a
// process_ or go_ family without and on (job, instance) knell_build_info.
// Every target on a shared instance writes those families, so
// instance="$instance" alone also shows the other jobs there.
func TestDashboardScopesSharedFamiliesToKnell(t *testing.T) {
	d := loadDashboard(t)
	checked := 0
	for where, expr := range dashboardQueries(&d) {
		series := seriesIn(expr)
		shared := slices.ContainsFunc(series, func(name string) bool {
			return name == "up" || strings.HasPrefix(name, "process_") || strings.HasPrefix(name, "go_")
		})
		if !shared {
			continue
		}
		checked++
		if !knellJoinRe.MatchString(expr) {
			t.Errorf("%s reads a family every target writes without and on (job, instance) knell_build_info: %s", where, expr)
		}
	}
	if checked == 0 {
		t.Error("no query reads up, process_ or go_; the check no longer reads the dashboard")
	}
}

// shippedRules maps each rule in alerts/promql.yaml to its expression.
func shippedRules(t *testing.T) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(promqlRules)
	if err != nil {
		t.Fatalf("reading %s: %v", promqlRules, err)
	}
	rules := map[string]string{}
	for _, m := range ruleRe.FindAllStringSubmatch(string(raw), -1) {
		rules[m[1]] = m[2]
	}
	if n := len(ruleNameRe.FindAllString(string(raw), -1)); n == 0 || len(rules) != n {
		t.Fatalf("%s has %d - alert: lines but %d distinct names read with expr: on the next line and a for: after it", promqlRules, n, len(rules))
	}
	return rules
}

// attentionTile is the query of the Needs attention tile for the rule name
// with expression expr. absent() and absent_over_time() label their alert
// with only the selector's equality matchers, and a shipped rule's selector
// names no instance, so only such a rule's tile also reads instance="":
// https://prometheus.io/docs/prometheus/latest/querying/functions/#absent
func attentionTile(name, expr string) string {
	read := `ALERTS{alertname="` + name + `", alertstate="firing", instance=`
	if strings.Contains(expr, "absent(") || strings.Contains(expr, "absent_over_time(") {
		return `0 * max((` + read + `"$instance"} or ` + read + `""}))`
	}
	return `0 * max(` + read + `"$instance"})`
}

// TestDashboardOverviewMirrorsEveryShippedRule fails when a rule in
// alerts/promql.yaml has no Overview tile, a tile names a rule the file does
// not hold or does not name it in its description, a Needs attention tile's
// query is not exactly attentionTile's for its rule, the row's variable does
// not count exactly those tiles in layout order, or a tile is not hidden until
// its query answers.
func TestDashboardOverviewMirrorsEveryShippedRule(t *testing.T) {
	d := loadDashboard(t)
	rules := shippedRules(t)
	overview := tab(t, &d, "Overview")
	attention := row(t, overview, "Needs attention")
	gate := attention.Spec.ConditionalRendering
	if gate == nil || gate.Spec.Visibility != "show" || len(gate.Spec.Items) != 1 ||
		gate.Spec.Items[0].Kind != "ConditionalRenderingVariable" ||
		gate.Spec.Items[0].Spec.Operator != "matches" || gate.Spec.Items[0].Spec.Value != "^[1-9]" {
		t.Fatal("Needs attention is not shown by a variable matching ^[1-9]")
	}
	var counts []string
	for _, it := range attention.Spec.Layout.Spec.Items {
		p := d.Spec.Elements[it.Spec.Element.Name]
		cr := it.Spec.ConditionalRendering
		var conds []string
		if cr != nil {
			for _, c := range cr.Spec.Items {
				conds = append(conds, c.Kind)
			}
		}
		slices.Sort(conds)
		if cr == nil || cr.Spec.Visibility != "show" || cr.Spec.Condition != "or" ||
			!slices.Equal(conds, []string{"ConditionalRenderingData", "ConditionalRenderingTimeRangeSize"}) ||
			p.Spec.Data.Spec.QueryOptions.MaxDataPoints <= 0 {
			t.Errorf("%q does not show exactly while its query answers: want a shown group or'ing a has-data rule with a 0s range rule, and a maxDataPoints pin", p.Spec.Title)
		}
		exprs := panelExprs(&p)
		if len(exprs) != 1 {
			t.Errorf("%q holds %d queries, want 1", p.Spec.Title, len(exprs))
			continue
		}
		// The tile label keeps each count its own series: or drops a
		// right-hand series whose labels match one on its left.
		counts = append(counts, fmt.Sprintf(`label_replace(count(%s), "tile", %q, "", "")`, exprs[0], strings.TrimPrefix(it.Spec.Element.Name, "panel-")))
		m := alertsRe.FindStringSubmatch(exprs[0])
		if m == nil {
			t.Errorf("%q reads no ALERTS series by alertname: %s", p.Spec.Title, exprs[0])
			continue
		}
		expr, ok := rules[m[1]]
		if !ok {
			t.Errorf("%q reads %s, which %s does not hold", p.Spec.Title, m[1], promqlRules)
			continue
		}
		if exprs[0] != attentionTile(m[1], expr) {
			t.Errorf("%q must read exactly\n%s\nso it shows only the picked instance's firing %s alerts; it reads\n%s",
				p.Spec.Title, attentionTile(m[1], expr), m[1], exprs[0])
		}
	}
	gateVar := d.variables()[gate.Spec.Items[0].Spec.Variable]
	want := "query_result(count(" + strings.Join(counts, " or ") + ") or vector(0))"
	if got := variableQuery(&gateVar); got != want {
		t.Errorf("the row's variable query must count exactly its tiles in layout order:\n%s\nit reads\n%s", want, got)
	}
	if got := gateVar.Spec.Definition; got != want {
		t.Errorf("the row's variable definition must equal its query:\n%s\nit reads\n%s", want, got)
	}
	var mirrored []string
	for _, r := range overview.Spec.Layout.Spec.Rows {
		for _, it := range r.Spec.Layout.Spec.Items {
			p := d.Spec.Elements[it.Spec.Element.Name]
			for _, e := range panelExprs(&p) {
				for _, m := range alertsRe.FindAllStringSubmatch(e, -1) {
					if !strings.Contains(p.Spec.Description, m[1]) {
						t.Errorf("%q reads %s, which its description does not name", p.Spec.Title, m[1])
					}
					if strings.Contains(m[2], `alertstate="firing"`) && !slices.Contains(mirrored, m[1]) {
						mirrored = append(mirrored, m[1])
					}
				}
			}
		}
	}
	for name := range rules {
		if !slices.Contains(mirrored, name) {
			t.Errorf("%s has no Overview tile reading its firing alerts", name)
		}
	}
	for _, name := range mirrored {
		if _, ok := rules[name]; !ok {
			t.Errorf("a tile mirrors %s, which %s does not hold", name, promqlRules)
		}
	}
}

// TestDashboardBeatsTableShowsEachBeat fails when the Beats table stops
// reading a beat's state, last ping, deadline or outages.
func TestDashboardBeatsTableShowsEachBeat(t *testing.T) {
	d := loadDashboard(t)
	var beats *dashPanel
	for _, it := range row(t, tab(t, &d, "Overview"), "Beats").Spec.Layout.Spec.Items {
		if p := d.Spec.Elements[it.Spec.Element.Name]; p.Spec.Title == "Beats" {
			beats = &p
		}
	}
	if beats == nil || beats.Spec.VizConfig.Group != "table" {
		t.Fatal("Overview has no Beats table")
	}
	read := strings.Join(panelExprs(beats), "\n")
	for _, series := range []string{"knell_beat_fresh", "knell_beat_last_seen_timestamp_seconds", "knell_beat_deadline_seconds", "knell_beat_outages_total"} {
		if !strings.Contains(read, series) {
			t.Errorf("Beats reads no %s", series)
		}
	}
}

var aggregationRe = regexp.MustCompile(`^(?:sum|min|max|avg|count|group)\b\s*`)

var byClauseRe = regexp.MustCompile(`^by\s*\(([^)]*)\)\s*`)

// closingParen is the index of the parenthesis closing the one s starts with,
// or -1. Parentheses inside quoted strings are skipped.
func closingParen(s string) int {
	if !strings.HasPrefix(s, "(") {
		return -1
	}
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '"', '\'', '`':
			i = skipString(s, i) - 1
		case '(':
			depth++
		case ')':
			if depth--; depth == 0 {
				return i
			}
		}
	}
	return -1
}

// outerGrouping is the sorted by-labels of expr's outermost aggregation, or
// false when the outermost operation is not an aggregation with a by clause.
// A round() around the whole expression is looked through, since it keeps
// every label.
func outerGrouping(expr string) ([]string, bool) {
	rest := strings.TrimSpace(expr)
	if inner, ok := strings.CutPrefix(rest, "round"); ok {
		inner = strings.TrimSpace(inner)
		if end := closingParen(inner); end >= 0 && strings.TrimSpace(inner[end+1:]) == "" {
			return outerGrouping(inner[1:end])
		}
	}
	agg := aggregationRe.FindString(rest)
	if agg == "" {
		return nil, false
	}
	rest = rest[len(agg):]
	var by []string
	readBy := func() {
		if g := byClauseRe.FindStringSubmatch(rest); g != nil {
			for l := range strings.SplitSeq(g[1], ",") {
				if l = strings.TrimSpace(l); l != "" {
					by = append(by, l)
				}
			}
			rest = rest[len(g[0]):]
		}
	}
	readBy()
	end := closingParen(rest)
	if end < 0 {
		return nil, false
	}
	rest = strings.TrimSpace(rest[end+1:])
	if by == nil {
		readBy()
	}
	if by == nil || rest != "" {
		return nil, false
	}
	slices.Sort(by)
	return by, true
}

// TestDashboardMergedTablesJoinEachRowOnItsKey fails when a table that merges
// its queries has a query that is not an aggregation by the same labels as the
// others. Grafana's merge joins rows only on the fields every frame shares and
// splits a row whose other shared fields differ, so a bare selector's __name__
// gives one beat several rows:
// https://github.com/grafana/grafana/blob/main/packages/grafana-data/src/transformations/transformers/merge.ts
func TestDashboardMergedTablesJoinEachRowOnItsKey(t *testing.T) {
	d := loadDashboard(t)
	merged := 0
	for name, p := range d.Spec.Elements {
		if !slices.ContainsFunc(p.Spec.Data.Spec.Transformations, func(tr transformation) bool { return tr.Group == "merge" }) {
			continue
		}
		merged++
		var key []string
		for i, e := range panelExprs(&p) {
			by, ok := outerGrouping(e)
			switch {
			case !ok:
				t.Errorf("%s %q query %d is not an aggregation by the table's key: %s", name, p.Spec.Title, i, e)
			case key == nil:
				key = by
			case !slices.Equal(by, key):
				t.Errorf("%s %q query %d aggregates by %v, an earlier query by %v", name, p.Spec.Title, i, by, key)
			}
		}
	}
	if merged == 0 {
		t.Error("no table merges its queries; the check no longer reads the dashboard")
	}
}

// TestDashboardRowHeadersSeparateTheirPanels fails when a row's header costs
// a click and hides nothing, or when a row's panels read as part of the row
// above. A titled row folds two panels of one kind (always shown, or shown
// only with data), unless a titled row is above it: a headerless row there
// reads as that row's panels, so only a tab's opening rows may hide theirs.
func TestDashboardRowHeadersSeparateTheirPanels(t *testing.T) {
	d := loadDashboard(t)
	for _, tb := range d.Spec.Layout.Spec.Tabs {
		titledAbove := ""
		for _, r := range tb.Spec.Layout.Spec.Rows {
			if r.Spec.HideHeader {
				if titledAbove != "" {
					t.Errorf("tab %q row %q hides its header below row %q, so its panels read as that row's", tb.Spec.Title, r.Spec.Title, titledAbove)
				}
				continue
			}
			shown, gated := 0, 0
			for _, it := range r.Spec.Layout.Spec.Items {
				if it.Spec.ConditionalRendering == nil {
					shown++
				} else {
					gated++
				}
			}
			if titledAbove == "" && shown < 2 && (shown > 0 || gated < 2) {
				t.Errorf("tab %q row %q shows a header over %d always-shown panels and %d shown only with data, want two of one kind or hideHeader",
					tb.Spec.Title, r.Spec.Title, shown, gated)
			}
			titledAbove = r.Spec.Title
		}
	}
}

// aggregated reports whether expr's outermost operation, looking through
// round(), is an aggregation, which leaves one series per group.
func aggregated(expr string) bool {
	rest := strings.TrimSpace(expr)
	for {
		inner, ok := strings.CutPrefix(rest, "round")
		if !ok {
			break
		}
		inner = strings.TrimSpace(inner)
		end := closingParen(inner)
		if end < 0 || strings.TrimSpace(inner[end+1:]) != "" {
			break
		}
		rest = strings.TrimSpace(inner[1:end])
	}
	return aggregationRe.MatchString(rest)
}

// TestDashboardCountsEachBeatOnce fails when a query about the beats reads a
// per-beat series without aggregating it by beat. Several knells can share one
// scrape instance, each holding its own copy of every beat, so a bare selector
// shows or counts a beat once per knell. The Notices row is left out: each
// knell sends and loses its own notices, so their sum is the real count.
func TestDashboardCountsEachBeatOnce(t *testing.T) {
	served := servedSeries(t)
	d := loadDashboard(t)
	checked := 0
	for _, title := range []string{"Overview", "History"} {
		for _, r := range tab(t, &d, title).Spec.Layout.Spec.Rows {
			if r.Spec.Title == "Notices" {
				continue
			}
			for _, it := range r.Spec.Layout.Spec.Items {
				p := d.Spec.Elements[it.Spec.Element.Name]
				for i, e := range panelExprs(&p) {
					perBeat := slices.ContainsFunc(seriesIn(e), func(name string) bool {
						return slices.Contains(served[name], "beat")
					})
					if !perBeat {
						continue
					}
					checked++
					if !regexp.MustCompile(`\bby\s*\(\s*beat\s*\)`).MatchString(e) {
						t.Errorf("tab %q %q query %d reads a per-beat series without aggregating by beat: %s", title, p.Spec.Title, i, e)
					}
				}
			}
		}
	}
	if checked == 0 {
		t.Error("no query reads a per-beat series; the check no longer reads the dashboard")
	}
}

// TestDashboardHealthTilesNameEachKnell fails when a knell health stat panel
// that shows one value per knell has no legend, since Grafana then names each
// tile by its whole label set.
func TestDashboardHealthTilesNameEachKnell(t *testing.T) {
	d := loadDashboard(t)
	checked := 0
	for _, r := range tab(t, &d, "knell health").Spec.Layout.Spec.Rows {
		for _, it := range r.Spec.Layout.Spec.Items {
			p := d.Spec.Elements[it.Spec.Element.Name]
			if p.Spec.VizConfig.Group != "stat" {
				continue
			}
			for i, q := range p.Spec.Data.Spec.Queries {
				if aggregated(q.Spec.Query.Spec.Expr) {
					continue
				}
				checked++
				if strings.TrimSpace(q.Spec.Query.Spec.LegendFormat) == "" {
					t.Errorf("%q query %d shows one tile per knell with no legend", p.Spec.Title, i)
				}
			}
		}
	}
	if checked == 0 {
		t.Error("no knell health tile shows one value per knell; the check no longer reads the dashboard")
	}
}

func TestDashboardPanelsExplainThemselves(t *testing.T) {
	d := loadDashboard(t)
	if !d.Spec.Preload {
		t.Error("spec.preload = false, want true so a hidden tile below the fold still queries")
	}
	tabs := d.Spec.Layout.Spec.Tabs
	if len(tabs) < 2 || tabs[0].Spec.Title != "Overview" || tabs[len(tabs)-1].Spec.Title != "knell health" {
		t.Error("tabs must open on Overview and end on knell health")
	}
	for name, p := range d.Spec.Elements {
		if strings.TrimSpace(p.Spec.Description) == "" {
			t.Errorf("%s %q has no description", name, p.Spec.Title)
		}
		switch p.Spec.VizConfig.Group {
		case "stat", "gauge", "bargauge", "table":
			if strings.TrimSpace(p.Spec.VizConfig.Spec.FieldConfig.Defaults.NoValue) == "" {
				t.Errorf("%s %q has no no-value text", name, p.Spec.Title)
			}
		}
	}
}

const spanTitle = "${span} ${spanunit}"

// rangeWordVars are the variables a title names the range length with.
var rangeWordVars = []string{"span", "spanunit", "spanthe"}

// customText is the text Grafana gives a custom variable whose query is query,
// which titles render. The parse follows CustomVariable.transformCsvStringToOptions,
// https://github.com/grafana/scenes/blob/main/packages/scenes/src/variables/variants/CustomVariable.tsx.
// More than one option would let Grafana keep a stale choice, so it fails.
func customText(t *testing.T, query string) string {
	t.Helper()
	entries := regexp.MustCompile(`(?:\\,|[^,])+`).FindAllString(query, -1)
	if len(entries) != 1 {
		t.Errorf("custom query %q gives %d options, want 1", query, len(entries))
		return ""
	}
	if m := regexp.MustCompile(`^\s*(.+)\s:\s(.+)$`).FindStringSubmatch(entries[0]); m != nil {
		return strings.TrimSpace(m[1])
	}
	return strings.TrimSpace(entries[0])
}

// rangeWords is the text of each range word variable once spanread holds the
// option Grafana makes of row, or none at all (""), as on a failed first read.
func rangeWords(t *testing.T, vars map[string]dashVariable, row string) map[string]string {
	t.Helper()
	read := vars["spanread"]
	rx, err := regexp.Compile(strings.TrimSuffix(strings.TrimPrefix(read.Spec.Regex, "/"), "/"))
	if err != nil || rx.SubexpIndex("value") < 0 || rx.SubexpIndex("text") < 0 {
		t.Fatalf("spanread regex %q: %v, want value and text groups", read.Spec.Regex, err)
	}
	var value, text string
	if row != "" {
		m := rx.FindStringSubmatch(row)
		if m == nil {
			t.Fatalf("spanread regex %v does not match %s", rx, row)
		}
		value, text = m[rx.SubexpIndex("value")], m[rx.SubexpIndex("text")]
	}
	fill := strings.NewReplacer("${spanread:text}", text, "${spanread}", value)
	words := map[string]string{}
	for _, name := range rangeWordVars {
		var query string
		_ = json.Unmarshal(vars[name].Spec.Query, &query)
		words[name] = customText(t, fill.Replace(query))
	}
	return words
}

// panelNames are a panel's title and the display names its overrides give
// its fields, such as a table column's header.
func panelNames(p *dashPanel) []string {
	names := []string{p.Spec.Title}
	for _, o := range p.Spec.VizConfig.Spec.FieldConfig.Overrides {
		for _, prop := range o.Properties {
			if s, ok := prop.Value.(string); ok && prop.ID == "displayName" {
				names = append(names, s)
			}
		}
	}
	return names
}

// A figure over the time range reads only with the range's length, so every
// panel outside the charts whose query reads $__range names it through span
// and spanunit, in its title or a column header. Every range word is a custom
// variable over spanread's one answer, and reads "the range" until spanread
// has read a length.
func TestDashboardRangeFiguresNameTheRangeLength(t *testing.T) {
	d := loadDashboard(t)
	vars := d.variables()
	read, ok := vars["spanread"]
	if !ok {
		t.Fatal("variable spanread missing, want one read of the range length for every range word")
	}
	if read.Kind != "QueryVariable" || read.Spec.Hide != "hideVariable" || read.Spec.Refresh != "onTimeRangeChanged" || !read.Spec.SkipURLSync ||
		read.Spec.Current.Text != "" || read.Spec.Current.Value != "" {
		t.Errorf("spanread is %s hide %q refresh %q, want a hidden QueryVariable refreshed on time range change, saved empty, not in the URL",
			read.Kind, read.Spec.Hide, read.Spec.Refresh)
	}
	for _, name := range slices.Sorted(maps.Keys(vars)) {
		if name != "spanread" && strings.Contains(string(vars[name].Spec.Query), "$__range_s") {
			t.Errorf("variable %s reads $__range_s, want spanread alone to read the range length", name)
		}
	}
	for _, name := range rangeWordVars {
		if v := vars[name]; v.Kind != "CustomVariable" || v.Spec.Hide != "hideVariable" || !v.Spec.SkipURLSync {
			t.Errorf("variable %s is %q hide %q, want a hidden CustomVariable kept out of the URL", name, v.Kind, v.Spec.Hide)
		}
	}
	if got, want := rangeWords(t, vars, ""), map[string]string{"span": "the", "spanunit": "range", "spanthe": "the"}; !maps.Equal(got, want) {
		t.Errorf("range words before spanread reads a length are %v, want %v", got, want)
	}
	query := variableQuery(&read)
	for _, c := range []struct {
		rangeS     int
		span, unit string
	}{
		{45, "45", "seconds"},
		{120, "2", "minutes"},
		{7199, "120", "minutes"},
		{7200, "2", "hours"},
		{19980, "5.6", "hours"},
		{172799, "48", "hours"},
		{172800, "2", "days"},
		{2592000, "30", "days"},
	} {
		got := rangeWords(t, vars, queryResultRow(t, query, c.rangeS))
		if want := map[string]string{"span": c.span, "spanunit": c.unit, "spanthe": "the " + c.span}; !maps.Equal(got, want) {
			t.Errorf("range words at a %d s range are %v, want %v", c.rangeS, got, want)
		}
	}
	figures := 0
	for name, p := range d.Spec.Elements {
		if g := p.Spec.VizConfig.Group; g == "timeseries" || g == "state-timeline" {
			continue
		}
		if !slices.ContainsFunc(panelExprs(&p), func(e string) bool { return strings.Contains(e, "$__range") }) {
			continue
		}
		figures++
		if !slices.ContainsFunc(panelNames(&p), func(n string) bool { return strings.Contains(n, spanTitle) }) {
			t.Errorf("%s %q reads $__range, want its title or a column to name %s", name, p.Spec.Title, spanTitle)
		}
	}
	if figures == 0 {
		t.Error("no range figure found; the check no longer reads the dashboard")
	}
}
