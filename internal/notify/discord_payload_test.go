package notify

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cplieger/knell/internal/watch"
)

// Discord's documented limits on one Execute Webhook payload. Every count is
// INCLUSIVE and edge whitespace is trimmed before counting, so the
// measurements below trim too.
// https://discord.com/developers/docs/resources/message#embed-object
const (
	limitContent     = 2000
	limitTitle       = 256
	limitDescription = 4096
	limitFieldName   = 256
	limitFieldValue  = 1024
	limitFooterText  = 2048
	limitAuthorName  = 256
	limitFields      = 25
	limitEmbeds      = 10
	// limitCombined is the sum across ALL embeds of title, description, every
	// field name and value, footer text and author name. Content is not part
	// of it.
	limitCombined = 6000
)

// discordMessage is the whole webhook payload as Discord would read it. The
// json tags are written out by hand rather than reusing the production types:
// a shared type would move with a renamed production tag instead of failing.
// The slots production never sets are decoded too, so adding one later is
// measured and searchable by construction, and Timestamp is a string so a test
// can assert the exact rendering.
type discordMessage struct {
	// raw is the body as it went over the wire, for the assertions that are
	// about a key's ABSENCE rather than its value.
	raw             []byte
	Content         string           `json:"content"`
	Embeds          []discordEmbed   `json:"embeds"`
	AllowedMentions *decodedMentions `json:"allowed_mentions"`
}

type decodedMentions struct {
	Parse *[]string `json:"parse"`
}

type discordEmbed struct {
	Type        string              `json:"type"`
	URL         string              `json:"url"`
	Title       string              `json:"title"`
	Description string              `json:"description"`
	Timestamp   string              `json:"timestamp"`
	Color       int                 `json:"color"`
	Fields      []discordEmbedField `json:"fields"`
	Footer      *decodedFooter      `json:"footer"`
	Author      *decodedAuthor      `json:"author"`
}

type discordEmbedField struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline"`
}

type decodedFooter struct {
	Text string `json:"text"`
}

type decodedAuthor struct {
	Name string `json:"name"`
}

// text flattens every slot a notice's words can occupy into one searchable
// string. The newline separator is deliberate: it stops a want string from
// matching across two slots, so a field assertion has to name the field.
func (m discordMessage) text() string {
	parts := []string{m.Content}
	for _, e := range m.Embeds {
		parts = append(parts, e.Title, e.Description)
		for _, f := range e.Fields {
			parts = append(parts, f.Name, f.Value)
		}
		parts = append(parts, e.footerText(), e.authorName())
	}
	return strings.Join(parts, "\n")
}

// field is the value of the one embed field named name, across all embeds.
// Absence or a duplicate is a setup failure, because the lines after the call
// read the value.
func (m discordMessage) field(t *testing.T, name string) string {
	t.Helper()

	var found, present []string
	for _, e := range m.Embeds {
		for _, f := range e.Fields {
			present = append(present, f.Name)
			if f.Name == name {
				found = append(found, f.Value)
			}
		}
	}
	if len(found) != 1 {
		t.Fatalf("payload carries %d fields named %q, want exactly 1; the names present are %q", len(found), name, present)
	}
	return found[0]
}

// onlyEmbed is the payload's single embed.
func (m discordMessage) onlyEmbed(t *testing.T) discordEmbed {
	t.Helper()

	if len(m.Embeds) != 1 {
		t.Fatalf("payload carries %d embeds, want exactly 1", len(m.Embeds))
	}
	return m.Embeds[0]
}

func (e discordEmbed) footerText() string {
	if e.Footer == nil {
		return ""
	}
	return e.Footer.Text
}

func (e discordEmbed) authorName() string {
	if e.Author == nil {
		return ""
	}
	return e.Author.Name
}

// slotMeasure is one limited slot's measured length. counted marks the slots
// inside Discord's combined budget.
type slotMeasure struct {
	slot    string
	runes   int
	limit   int
	counted bool
}

// measureSlots measures every slot Discord bounds, including the ones
// production never sets: a complete table is what makes adding one of those
// later impossible without a measurement.
func measureSlots(m discordMessage) []slotMeasure {
	slots := []slotMeasure{{slot: "content", runes: countRunes(m.Content), limit: limitContent}}
	for i, e := range m.Embeds {
		slots = append(slots,
			slotMeasure{slot: fmt.Sprintf("embeds.%d.title", i), runes: countRunes(e.Title), limit: limitTitle, counted: true},
			slotMeasure{slot: fmt.Sprintf("embeds.%d.description", i), runes: countRunes(e.Description), limit: limitDescription, counted: true},
		)
		for j, f := range e.Fields {
			slots = append(slots,
				slotMeasure{slot: fmt.Sprintf("embeds.%d.fields.%d.name", i, j), runes: countRunes(f.Name), limit: limitFieldName, counted: true},
				slotMeasure{slot: fmt.Sprintf("embeds.%d.fields.%d.value", i, j), runes: countRunes(f.Value), limit: limitFieldValue, counted: true},
			)
		}
		slots = append(slots,
			slotMeasure{slot: fmt.Sprintf("embeds.%d.footer.text", i), runes: countRunes(e.footerText()), limit: limitFooterText, counted: true},
			slotMeasure{slot: fmt.Sprintf("embeds.%d.author.name", i), runes: countRunes(e.authorName()), limit: limitAuthorName, counted: true},
		)
	}
	return slots
}

// countRunes counts a slot the way Discord does: runes, edge whitespace
// trimmed and not charged.
func countRunes(s string) int {
	return len([]rune(strings.TrimSpace(s)))
}

// deliverNotice posts one notice through the real render path and returns the
// payload the webhook received.
func deliverNotice(t *testing.T, node string, send func(*Discord) error) discordMessage {
	t.Helper()

	rec := newWebhookRecorder(http.StatusNoContent)
	srv := httptest.NewServer(rec.handler(t))
	t.Cleanup(srv.Close)

	d := newTestNotifier(t, srv.URL, node)
	t.Cleanup(d.Close)

	if err := send(d); err != nil {
		t.Fatalf("sending the notice: %v", err)
	}
	return <-rec.payloads
}

// noticeShapes are the four payload shapes the three render entry points
// produce: a batch history notice renders differently from a single one, so
// four shapes cover three methods.
func noticeShapes(t *testing.T, id string) map[string]func(*Discord) error {
	t.Helper()

	started := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	live := watch.Transition{Started: started, Observed: started.Add(37 * time.Minute)}
	one := watch.Outage{Started: started, Recovered: started.Add(12 * time.Minute), Undelivered: true}
	other := watch.Outage{Started: started, Recovered: started.Add(47 * time.Minute)}
	return map[string]func(*Discord) error{
		"missing":   func(d *Discord) error { return d.BeatMissing(t.Context(), id, live) },
		"recovered": func(d *Discord) error { return d.BeatRecovered(t.Context(), id, live) },
		"history one": func(d *Discord) error {
			return d.BeatOutageHistory(t.Context(), id, []watch.Outage{one})
		},
		"history several": func(d *Discord) error {
			return d.BeatOutageHistory(t.Context(), id, []watch.Outage{one, other})
		},
	}
}

// TestPayloadTextCoversEverySearchableSlot is the oracle for the oracle: a
// flattener that silently skipped a slot would make every forbid assertion in
// this package vacuous, and every want assertion pass for the wrong slot.
func TestPayloadTextCoversEverySearchableSlot(t *testing.T) {
	t.Parallel()

	m := discordMessage{
		Content: "marker-content",
		Embeds: []discordEmbed{{
			Title:       "marker-title",
			Description: "marker-description",
			Fields:      []discordEmbedField{{Name: "marker-field-name", Value: "marker-field-value"}},
			Footer:      &decodedFooter{Text: "marker-footer"},
			Author:      &decodedAuthor{Name: "marker-author"},
		}},
	}
	got := m.text()
	for _, want := range []string{
		"marker-content", "marker-title", "marker-description",
		"marker-field-name", "marker-field-value", "marker-footer", "marker-author",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("text() = %q, missing %q: a slot the flattener skips is a slot no assertion in this package can see", got, want)
		}
	}
}

// TestEveryNoticePostsAStructurallyValidEmbed pins the shape Discord accepts.
// An empty field name is its BASE_TYPE_REQUIRED 400, which is exactly the
// rejection statusDetail exists to explain, and a notice answered 400 is never
// delivered.
func TestEveryNoticePostsAStructurallyValidEmbed(t *testing.T) {
	t.Parallel()

	for name, send := range noticeShapes(t, "api") {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			m := deliverNotice(t, "node-1", send)
			if m.Content == "" {
				t.Error("content is empty: a notification preview shows the content line, and an all-empty payload is a 400")
			}
			if strings.ContainsAny(m.Content, "\r\n") {
				t.Errorf("content = %q, want one line: the detail belongs in the embed", m.Content)
			}
			e := m.onlyEmbed(t)
			if e.Title == "" || e.Description == "" {
				t.Errorf("embed title = %q, description = %q, want both non-empty", e.Title, e.Description)
			}
			if len(e.Fields) == 0 {
				t.Error("embed carries no fields, want the notice's figures")
			}
			for i, f := range e.Fields {
				if f.Name == "" || f.Value == "" {
					t.Errorf("embed field %d = {name: %q, value: %q}, want both non-empty: Discord answers 400 BASE_TYPE_REQUIRED and the notice is never delivered", i, f.Name, f.Value)
				}
			}
		})
	}
}

// TestEachNoticeKindCarriesItsOwnColor pins the palette through the real
// render path. A palette collapsed to one colour, or a kind wired to the wrong
// entry, changes nothing else in the payload.
func TestEachNoticeKindCarriesItsOwnColor(t *testing.T) {
	t.Parallel()

	if len(noticeStyles) != 3 {
		t.Fatalf("noticeStyles has %d entries, want one per notice kind", len(noticeStyles))
	}
	seen := make(map[int]string, len(noticeStyles))
	for _, style := range noticeStyles {
		if other, dup := seen[style.color]; dup {
			t.Errorf("colour %#06x is shared by %q and %q, want one colour per kind", style.color, other, style.word)
		}
		seen[style.color] = style.word
	}

	shapes := noticeShapes(t, "api")
	for name, want := range map[string]int{
		"missing":         0xED4245,
		"recovered":       0x57F287,
		"history one":     0xFEE75C,
		"history several": 0xFEE75C,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			m := deliverNotice(t, "node-1", shapes[name])
			if got := m.onlyEmbed(t).Color; got != want {
				t.Errorf("the %s notice renders colour %#06x, want %#06x", name, got, want)
			}
		})
	}
}

// TestNoticeEmbedOmitsTypeAndURL pins the two slots that must stay off the
// wire: Discord ignores a webhook embed's "type", and it shows only the first
// of several embeds sharing a "url".
func TestNoticeEmbedOmitsTypeAndURL(t *testing.T) {
	t.Parallel()

	m := deliverNotice(t, "node-1", noticeShapes(t, "api")["missing"])
	var wire struct {
		Embeds []map[string]json.RawMessage `json:"embeds"`
	}
	if err := json.Unmarshal(m.raw, &wire); err != nil {
		t.Fatalf("payload not JSON: %v", err)
	}
	if len(wire.Embeds) != 1 {
		t.Fatalf("payload carries %d embeds, want exactly 1", len(wire.Embeds))
	}
	for _, key := range []string{"type", "url"} {
		if _, present := wire.Embeds[0][key]; present {
			t.Errorf("embed carries %q, want it absent: %s", key, map[string]string{
				"type": `Discord always reads it as "rich" for a webhook embed`,
				"url":  "Discord shows only the first of several embeds sharing one",
			}[key])
		}
	}
}

// TestLiveNoticeTimestampsTheObservation pins WHICH of watch.Transition's two
// instants the card is stamped with. Both are present on the value, so a swap
// is invisible in every other assertion.
func TestLiveNoticeTimestampsTheObservation(t *testing.T) {
	t.Parallel()

	started := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	live := watch.Transition{Started: started, Observed: started.Add(37 * time.Minute)}
	for name, send := range map[string]func(*Discord) error{
		"missing":   func(d *Discord) error { return d.BeatMissing(t.Context(), "api", live) },
		"recovered": func(d *Discord) error { return d.BeatRecovered(t.Context(), "api", live) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := deliverNotice(t, "node-1", send).onlyEmbed(t).Timestamp
			if want := "2026-07-23T12:37:00Z"; got != want {
				t.Errorf("the %s notice is stamped %q, want %q: the card speaks for the observation, not for the start of the silence", name, got, want)
			}
		})
	}
}
