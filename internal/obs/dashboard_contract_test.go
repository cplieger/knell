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
	"strings"
	"testing"
	"time"
)

const (
	dashboardFile = "../../grafana-dashboard.json"
	promqlRules   = "../../alerts/promql.yaml"
)

type dashQuery struct {
	Spec struct {
		Query struct {
			Spec struct {
				Expr  string `json:"expr"`
				Query string `json:"query"`
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
	} `json:"spec"`
}

type conditionGroup struct {
	Spec struct {
		Condition string          `json:"condition"`
		Items     []conditionRule `json:"items"`
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
	Kind string `json:"kind"`
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
	selectorRe    = regexp.MustCompile(`([a-zA-Z_:][a-zA-Z0-9_:]*)\{([^}]*)\}`)
	matcherNameRe = regexp.MustCompile(`([a-zA-Z_]\w*)\s*(?:=~|!~|!=|=)`)
	matchersRe    = regexp.MustCompile(`\{[^}]*\}`)
	groupingRe    = regexp.MustCompile(`\b(by|without|on|ignoring|group_left|group_right)\s*\(([^)]*)\)`)
	rangeRe       = regexp.MustCompile(`\[[^\]]*\]`)
	variableRe    = regexp.MustCompile(`\$\{?\w+(?::\w+)?\}?`)
	labelValuesRe = regexp.MustCompile(`label_values\(([^,]+),\s*\w+\)`)
	functionRe    = regexp.MustCompile(`\b[a-z_]+\s*\(`)
	identRe       = regexp.MustCompile(`\b[a-zA-Z_:][a-zA-Z0-9_:]*\b`)
	alertsRe      = regexp.MustCompile(`ALERTS\{alertname="([A-Za-z]+)"([^}]*)\}`)
	ruleNameRe    = regexp.MustCompile(`(?m)^\s*- alert: (\w+)$`)
)

var promqlKeywords = []string{"and", "or", "unless", "bool", "offset"}

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
	queries := map[string]string{}
	for name, v := range d.variables() {
		queries["variable "+name] = variableQuery(&v)
	}
	for name, p := range d.Spec.Elements {
		for i, e := range panelExprs(&p) {
			queries[fmt.Sprintf("%s %s query %d", name, p.Spec.Title, i)] = e
		}
	}
	scrape := []string{"job", "instance", "le"}
	for where, expr := range queries {
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
		for _, sel := range selectorRe.FindAllStringSubmatch(quotedRe.ReplaceAllString(expr, `""`), -1) {
			have, ok := served[sel[1]]
			if !ok {
				continue
			}
			for _, m := range matcherNameRe.FindAllStringSubmatch(sel[2], -1) {
				if !slices.Contains(have, m[1]) && !slices.Contains(scrape, m[1]) {
					t.Errorf("%s matches %s on %s, which carries no such label", where, m[1], sel[1])
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

func shippedRules(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile(promqlRules)
	if err != nil {
		t.Fatalf("reading %s: %v", promqlRules, err)
	}
	var names []string
	for _, m := range ruleNameRe.FindAllStringSubmatch(string(raw), -1) {
		names = append(names, m[1])
	}
	if len(names) == 0 {
		t.Fatalf("%s defines no rule", promqlRules)
	}
	return names
}

// TestDashboardOverviewMirrorsEveryShippedRule fails when a rule in
// alerts/promql.yaml has no Overview tile, or a tile names a rule the file
// does not hold. Each tile reads its rule's firing ALERTS series and names the
// rule, and a Needs attention tile starts hidden until its query answers.
func TestDashboardOverviewMirrorsEveryShippedRule(t *testing.T) {
	d := loadDashboard(t)
	rules := shippedRules(t)
	overview := tab(t, &d, "Overview")
	attention := row(t, overview, "Needs attention")
	gate := attention.Spec.ConditionalRendering
	if gate == nil || len(gate.Spec.Items) != 1 || gate.Spec.Items[0].Kind != "ConditionalRenderingVariable" ||
		gate.Spec.Items[0].Spec.Value != "^[1-9]" {
		t.Fatal("Needs attention is not shown by a variable matching ^[1-9]")
	}
	vars := d.variables()
	gateVar := vars[gate.Spec.Items[0].Spec.Variable]
	counted := variableQuery(&gateVar)
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
		if cr == nil || cr.Spec.Condition != "or" ||
			!slices.Equal(conds, []string{"ConditionalRenderingData", "ConditionalRenderingTimeRangeSize"}) ||
			p.Spec.Data.Spec.QueryOptions.MaxDataPoints <= 0 {
			t.Errorf("%q can show before its query answers: want a has-data rule or'ed with a 0s range rule and a maxDataPoints pin", p.Spec.Title)
		}
		for _, e := range panelExprs(&p) {
			if !strings.Contains(counted, "count("+e+")") {
				t.Errorf("%q query is not counted by the row's variable", p.Spec.Title)
			}
		}
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
	for _, name := range rules {
		if !slices.Contains(mirrored, name) {
			t.Errorf("%s has no Overview tile reading its firing alerts", name)
		}
	}
	for _, name := range mirrored {
		if !slices.Contains(rules, name) {
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
// or -1.
func closingParen(s string) int {
	if !strings.HasPrefix(s, "(") {
		return -1
	}
	depth, quoted := 0, false
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case quoted && c == '\\':
			i++
		case c == '"':
			quoted = !quoted
		case quoted:
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth == 0 {
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

// TestDashboardRowHeadersFoldTwoPanels fails when a row shows a header over
// fewer than two always-shown panels, which costs a click and hides nothing.
// A row of tiles shown only with data needs two of them, since it lights
// several.
func TestDashboardRowHeadersFoldTwoPanels(t *testing.T) {
	d := loadDashboard(t)
	for _, tb := range d.Spec.Layout.Spec.Tabs {
		for _, r := range tb.Spec.Layout.Spec.Rows {
			if r.Spec.HideHeader {
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
			if shown < 2 && (shown > 0 || gated < 2) {
				t.Errorf("tab %q row %q shows a header over %d always-shown panels and %d shown only with data, want two of one kind or hideHeader",
					tb.Spec.Title, r.Spec.Title, shown, gated)
			}
		}
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
