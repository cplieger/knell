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
// exists. Every production .go file in the directory is parsed individually
// (parser.ParseDir is deprecated, and its build-tag-blind package grouping is
// exactly what the deprecation warns about); _test.go files are excluded
// because the contract is the PRODUCTION kind set, so a test-only Kind fixture
// must not be counted as an advertised notification kind. The scan is guarded
// below by a "found nothing" failure so a broken walk cannot pass silently.
func TestEveryKindConstantIsPreMinted(t *testing.T) {
	t.Parallel()

	declared := declaredKindConstants(t, parseProductionFiles(t))

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

// declaredKindConstants maps every declared Kind constant's name to its value.
func declaredKindConstants(t *testing.T, files []*ast.File) map[string]string {
	t.Helper()
	declared := map[string]string{}
	for _, file := range files {
		collectKindDecls(t, declared, file.Decls)
	}
	return declared
}

// collectKindDecls narrows a file's declarations to its const blocks.
func collectKindDecls(t *testing.T, declared map[string]string, decls []ast.Decl) {
	t.Helper()
	for _, decl := range decls {
		gen, ok := decl.(*ast.GenDecl)
		if ok && gen.Tok == token.CONST {
			collectKindSpecs(t, declared, gen.Specs)
		}
	}
}

// collectKindSpecs narrows a const block's specs to those declaring a Kind, in
// either shape Go allows: an explicitly typed `Name Kind = "x"`, or an untyped
// `Name = Kind("x")` conversion. Both mint a Kind constant this contract
// covers, so recognizing only the typed shape would let a kind declared the
// other way pass unnoticed with its three counters never pre-minted.
func collectKindSpecs(t *testing.T, declared map[string]string, specs []ast.Spec) {
	t.Helper()
	for _, spec := range specs {
		value, ok := spec.(*ast.ValueSpec)
		if !ok {
			continue
		}
		if ident, typed := value.Type.(*ast.Ident); typed && ident.Name == "Kind" {
			collectKindValues(t, declared, value)
			continue
		}
		collectKindConversions(t, declared, value)
	}
}

// collectKindValues decodes one Kind spec's string literals, failing the test
// for any declaration shape this contract cannot verify.
func collectKindValues(t *testing.T, declared map[string]string, value *ast.ValueSpec) {
	t.Helper()
	for i, name := range value.Names {
		if i >= len(value.Values) {
			t.Errorf("Kind constant %s has no explicit value, so this test cannot verify it is pre-minted", name.Name)
			continue
		}
		if unquoted, ok := kindLiteralValue(t, name.Name, value.Values[i]); ok {
			declared[name.Name] = unquoted
		}
	}
}

// kindLiteralValue decodes a kind's string-literal value, failing the test for
// any value shape this contract cannot verify.
func kindLiteralValue(t *testing.T, name string, expr ast.Expr) (string, bool) {
	t.Helper()
	lit, ok := expr.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		t.Errorf("Kind constant %s is not a string literal, so this test cannot verify it is pre-minted", name)
		return "", false
	}
	unquoted, err := strconv.Unquote(lit.Value)
	if err != nil {
		t.Errorf("Kind constant %s value %s: %v", name, lit.Value, err)
		return "", false
	}
	return unquoted, true
}

// collectKindConversions decodes the other legal shape of a kind declaration:
// an untyped const whose value is a Kind("...") conversion. Without it such a
// kind is invisible to this contract, so its sent/failed/dropped series are
// never pre-minted and the test written to catch exactly that passes anyway.
func collectKindConversions(t *testing.T, declared map[string]string, value *ast.ValueSpec) {
	t.Helper()
	for i, name := range value.Names {
		if i >= len(value.Values) {
			continue
		}
		inner, ok := kindConversionArg(value.Values[i])
		if !ok {
			continue
		}
		if unquoted, ok := kindLiteralValue(t, name.Name, inner); ok {
			declared[name.Name] = unquoted
		}
	}
}

// kindConversionArg returns the argument of a Kind("...") conversion.
func kindConversionArg(expr ast.Expr) (ast.Expr, bool) {
	call, ok := expr.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return nil, false
	}
	fn, ok := call.Fun.(*ast.Ident)
	if !ok || fn.Name != "Kind" {
		return nil, false
	}
	return call.Args[0], true
}
