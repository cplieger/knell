package metrics

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// TestEveryKindConstantIsPreMinted enforces the sync rule notificationKinds'
// own doc states ("Add every new Kind constant here so those two exposition
// views stay aligned"). Nothing else can: the cold-start test iterates
// notificationKinds, so a Kind constant missing from that slice is invisible to
// it, and the watch/webapi counter tests all assert DELTAS against a baseline
// rather than the zero sample. A kind whose three counters are never pre-minted
// has no earlier sample for increase() to diff against, so KnellNotifyFailing
// stays silent through the first failure or drop of that kind - on the app whose
// entire job is being alertable.
//
// The source is parsed rather than reflected over because Go erases constant
// declarations at runtime: the declaration site is the only place the full set
// exists. Every .go file in the directory is parsed individually (parser.ParseDir
// is deprecated, and its build-tag-blind package grouping is exactly what the
// deprecation warns about); the scan is guarded below by a "found nothing"
// failure so a broken walk cannot pass silently.
func TestEveryKindConstantIsPreMinted(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("listing the package source: %v", err)
	}
	fset := token.NewFileSet()
	files := make([]*ast.File, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") {
			continue
		}
		file, parseErr := parser.ParseFile(fset, name, nil, 0)
		if parseErr != nil {
			t.Fatalf("parsing the package source %s: %v", name, parseErr)
		}
		files = append(files, file)
	}
	declared := map[string]string{}
	for _, file := range files {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				ident, ok := value.Type.(*ast.Ident)
				if !ok || ident.Name != "Kind" {
					continue
				}
				for i, name := range value.Names {
					if i >= len(value.Values) {
						t.Errorf("Kind constant %s has no explicit value, so this test cannot verify it is pre-minted", name.Name)
						continue
					}
					lit, ok := value.Values[i].(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						t.Errorf("Kind constant %s is not a string literal, so this test cannot verify it is pre-minted", name.Name)
						continue
					}
					unquoted, unquoteErr := strconv.Unquote(lit.Value)
					if unquoteErr != nil {
						t.Errorf("Kind constant %s value %s: %v", name.Name, lit.Value, unquoteErr)
						continue
					}
					declared[name.Name] = unquoted
				}
			}
		}
	}

	if len(declared) == 0 {
		t.Fatal("found no Kind constants in the package source: the scan is broken, so it would pass for any missing kind")
	}
	for name, value := range declared {
		if !slices.Contains(notificationKinds, Kind(value)) {
			t.Errorf("Kind constant %s (%q) is not in notificationKinds: its sent/failed/dropped series are never pre-minted at zero, so an increase() alert misses the first event of that kind", name, value)
		}
	}
	if len(notificationKinds) != len(declared) {
		t.Errorf("notificationKinds has %d entries for %d declared Kind constants %v: the pre-minting loop and the advertised HELP kind list must cover exactly the declared set", len(notificationKinds), len(declared), declared)
	}
}
