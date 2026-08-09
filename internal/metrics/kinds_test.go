package metrics

import (
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
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
// exists. Every production .go file in the directory is parsed individually
// (parser.ParseDir is deprecated, and its build-tag-blind package grouping is
// exactly what the deprecation warns about); _test.go files are excluded
// because the contract is the PRODUCTION kind set, so a test-only Kind fixture
// must not be counted as an advertised notification kind. The scan is guarded
// below by a "found nothing" failure so a broken walk cannot pass silently.
func TestEveryKindConstantIsPreMinted(t *testing.T) {
	t.Parallel()

	declared := declaredLabelConstants(t, parseProductionFiles(t), "Kind")

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

// TestEveryRefusalConstantIsPreMinted is the refusal half of the same sync rule
// refusalReasons' own doc states ("Add every new Refusal constant here so those
// two exposition views stay aligned"), and nothing else enforces it: the
// cold-start test iterates refusalReasons, so a Refusal constant missing from
// that slice is invisible to it, and its guard only fires when the SLICE grows.
// A reason whose series is never pre-minted has no earlier sample for
// increase() to diff against, so the first refusal of that reason is invisible
// to a windowed query over it - which is the exact flaw that retired this
// counter's predecessor, so losing it silently gives the reason label back the
// shape it was introduced to fix.
func TestEveryRefusalConstantIsPreMinted(t *testing.T) {
	t.Parallel()

	declared := declaredLabelConstants(t, parseProductionFiles(t), "Refusal")

	if len(declared) == 0 {
		t.Fatal("found no Refusal constants in the package source: the scan is broken, so it would pass for any missing reason")
	}
	for name, value := range declared {
		if !slices.Contains(refusalReasons, Refusal(value)) {
			t.Errorf("Refusal constant %s (%q) is not in refusalReasons: its series is never pre-minted at zero, so an increase() query misses the first refusal of that reason", name, value)
		}
	}
	if len(refusalReasons) != len(declared) {
		t.Errorf("refusalReasons has %d entries for %d declared Refusal constants %v: the pre-minting loop and the advertised HELP reason list must cover exactly the declared set", len(refusalReasons), len(declared), declared)
	}
}

// TestDeclaredKindConstantsSeesTheConversionShape is the oracle for the untyped
// `Name = Kind("x")` branch. Every kind the committed package declares is
// explicitly typed, so TestEveryKindConstantIsPreMinted's real-source scan never
// reaches collectLabelConversions: a regression that stops recognizing the
// conversion shape stays green until someone declares a kind that way - the
// exact day the guard is needed, when the kind would slip through un-minted and
// the pre-minting test would still pass. The source is synthetic so the real
// package files are never mutated.
func TestDeclaredKindConstantsSeesTheConversionShape(t *testing.T) {
	t.Parallel()

	const src = `package metrics

const (
	KindProbe = Kind("probe")
	KindTyped Kind = "typed"
	unrelatedTimeout = 5
)
`
	file, err := parser.ParseFile(token.NewFileSet(), "synthetic.go", src, 0)
	if err != nil {
		t.Fatalf("parsing the synthetic source: %v", err)
	}

	declared := declaredLabelConstants(t, []*ast.File{file}, "Kind")

	want := map[string]string{"KindProbe": "probe", "KindTyped": "typed"}
	if !maps.Equal(declared, want) {
		t.Errorf("declaredLabelConstants(..., %q) = %v, want %v: a kind this scan cannot see keeps its sent/failed/dropped series un-minted while the pre-minting contract reports itself satisfied", "Kind", declared, want)
	}
}

// parseProductionFiles parses every non-test .go file of the package, which is
// the scope of the pre-minting contract: a Kind declared only in a _test.go
// fixture is not an advertised notification kind.
func parseProductionFiles(t *testing.T) []*ast.File {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("listing the package source: %v", err)
	}
	fset := token.NewFileSet()
	files := make([]*ast.File, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(fset, name, nil, 0)
		if parseErr != nil {
			t.Fatalf("parsing the package source %s: %v", name, parseErr)
		}
		files = append(files, file)
	}
	return files
}

// declaredLabelConstants maps every declared constant of the named label type
// (Kind, Refusal) to its value. The type name is a parameter so the two
// string-kinded label vocabularies share one scan: a second copy of it would be
// the weaker of the two, and the shape it missed would be missed silently.
func declaredLabelConstants(t *testing.T, files []*ast.File, typeName string) map[string]string {
	t.Helper()
	declared := map[string]string{}
	for _, file := range files {
		collectLabelDecls(t, declared, file.Decls, typeName)
	}
	return declared
}

// collectLabelDecls narrows a file's declarations to its const blocks.
func collectLabelDecls(t *testing.T, declared map[string]string, decls []ast.Decl, typeName string) {
	t.Helper()
	for _, decl := range decls {
		gen, ok := decl.(*ast.GenDecl)
		if ok && gen.Tok == token.CONST {
			collectLabelSpecs(t, declared, gen.Specs, typeName)
		}
	}
}

// collectLabelSpecs narrows a const block's specs to those declaring the named
// label type, in either shape Go allows: an explicitly typed `Name Kind = "x"`,
// or an untyped `Name = Kind("x")` conversion. Both mint a constant this
// contract covers, so recognizing only the typed shape would let one declared
// the other way pass unnoticed with its series never pre-minted.
func collectLabelSpecs(t *testing.T, declared map[string]string, specs []ast.Spec, typeName string) {
	t.Helper()
	for _, spec := range specs {
		value, ok := spec.(*ast.ValueSpec)
		if !ok {
			continue
		}
		if ident, typed := value.Type.(*ast.Ident); typed && ident.Name == typeName {
			collectLabelValues(t, declared, value, typeName)
			continue
		}
		collectLabelConversions(t, declared, value, typeName)
	}
}

// collectLabelValues decodes one spec's string literals, failing the test for
// any declaration shape this contract cannot verify.
func collectLabelValues(t *testing.T, declared map[string]string, value *ast.ValueSpec, typeName string) {
	t.Helper()
	for i, name := range value.Names {
		if i >= len(value.Values) {
			t.Errorf("%s constant %s has no explicit value, so this test cannot verify it is pre-minted", typeName, name.Name)
			continue
		}
		if unquoted, ok := labelLiteralValue(t, typeName, name.Name, value.Values[i]); ok {
			declared[name.Name] = unquoted
		}
	}
}

// labelLiteralValue decodes a constant's string-literal value, failing the test
// for any value shape this contract cannot verify.
func labelLiteralValue(t *testing.T, typeName, name string, expr ast.Expr) (string, bool) {
	t.Helper()
	lit, ok := expr.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		t.Errorf("%s constant %s is not a string literal, so this test cannot verify it is pre-minted", typeName, name)
		return "", false
	}
	unquoted, err := strconv.Unquote(lit.Value)
	if err != nil {
		t.Errorf("%s constant %s value %s: %v", typeName, name, lit.Value, err)
		return "", false
	}
	return unquoted, true
}

// collectLabelConversions decodes the other legal shape of a declaration: an
// untyped const whose value is a Kind("...") / Refusal("...") conversion.
// Without it such a constant is invisible to this contract, so its series are
// never pre-minted and the test written to catch exactly that passes anyway.
func collectLabelConversions(t *testing.T, declared map[string]string, value *ast.ValueSpec, typeName string) {
	t.Helper()
	for i, name := range value.Names {
		if i >= len(value.Values) {
			continue
		}
		inner, ok := labelConversionArg(value.Values[i], typeName)
		if !ok {
			continue
		}
		if unquoted, ok := labelLiteralValue(t, typeName, name.Name, inner); ok {
			declared[name.Name] = unquoted
		}
	}
}

// labelConversionArg returns the argument of a Kind("...") / Refusal("...")
// conversion.
func labelConversionArg(expr ast.Expr, typeName string) (ast.Expr, bool) {
	call, ok := expr.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return nil, false
	}
	fn, ok := call.Fun.(*ast.Ident)
	if !ok || fn.Name != typeName {
		return nil, false
	}
	return call.Args[0], true
}
