package controller

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	kubernetesaccess "github.com/jensilin/KubeScaleSense/internal/kubernetes"
)

// This file is the machine-checked form of the phase's central claim: the
// Phase 1 controller has no path by which it could change the cluster.
//
// Three independent layers enforce it, and each is asserted separately, because
// any one of them could be removed by a plausible-looking future edit:
//
//	interface    the controller is handed a ClusterReader, which has no
//	             mutating method and no clientset behind it
//	transport    the REST client refuses every non-GET method
//	             (internal/kubernetes, TestReadOnlyClientset_HasNoWritePath)
//	RBAC         the ServiceAccount holds no write verb (deploy/)
//
// The transport and RBAC layers hold even if the code is wrong. The tests below
// cover the first layer and, more importantly, guard the boundary: they fail if
// a clientset is ever wired into a package that computes decisions.

// mutatingMethods are the client-go verbs that change cluster state. A name
// here is enough to fail the check, so the list is deliberately broad: it is
// better to force an explanatory comment on a false positive than to miss a
// write.
var mutatingMethods = []string{
	"Create",
	"Update",
	"UpdateStatus",
	"UpdateScale",
	"Patch",
	"Delete",
	"DeleteCollection",
	"Evict",
	"ApplyStatus",
}

// clientPackages are the import paths that can reach the Kubernetes API
// directly. Only internal/kubernetes may hold one, and only to build the
// read-only wrappers.
var clientPackages = []string{
	"k8s.io/client-go/kubernetes",
	"k8s.io/client-go/rest",
	"k8s.io/client-go/tools/clientcmd",
	"k8s.io/client-go/dynamic",
	"k8s.io/client-go/scale",
	"k8s.io/metrics/pkg/client/clientset/versioned",
	"sigs.k8s.io/controller-runtime",
}

// packagesAllowedAClient is the complete set of packages permitted to import a
// Kubernetes client. Keeping it to one package is what makes the read-only
// property reviewable: there is exactly one file to read.
var packagesAllowedAClient = map[string]bool{
	"internal/kubernetes":    true,
	"cmd/kubescalesense":     false, // main wires the reader but never a raw client
	"internal/controller":    false,
	"internal/scaling":       false,
	"internal/resources":     false,
	"internal/metrics":       false,
	"internal/observability": false,
	"internal/config":        false,

	// The workload and the demo tooling, added in P2. `false` here is a
	// stronger statement than it is for the controller's packages: those may
	// not hold a *client*, while these may not talk to Kubernetes at all. A
	// Normalizer that reads the API stops being a workload that happens to be
	// autoscaled and becomes part of the autoscaler
	// (WR-01), and a load generator that can scale a Deployment could quietly
	// produce the very effect P2 exists to demonstrate is absent.
	"cmd/normalizer":      false,
	"internal/normalizer": false,
	"tests/demo/loadgen":  false,
}

// The interface the controller is given must have no way to write. Asserted by
// reflection over the method set, so it holds against the compiled type rather
// than against the source it was declared in.
func TestClusterReader_ExposesNoMutatingMethod(t *testing.T) {
	t.Parallel()

	reader := reflect.TypeOf((*kubernetesaccess.ClusterReader)(nil)).Elem()

	if reader.NumMethod() == 0 {
		t.Fatal("ClusterReader has no methods; the reflection target is wrong")
	}

	for i := range reader.NumMethod() {
		name := reader.Method(i).Name
		for _, verb := range mutatingMethods {
			if strings.Contains(name, verb) {
				t.Errorf("ClusterReader.%s looks like a write; Phase 1's observation interface must be read-only", name)
			}
		}
	}
}

// The controller's dependencies must contain no Kubernetes client. Checked by
// reflection over the Options struct: a clientset smuggled in as a field would
// give every method on it to the reconcile loop.
func TestOptions_HoldNoKubernetesClient(t *testing.T) {
	t.Parallel()

	options := reflect.TypeOf(Options{})

	for i := range options.NumField() {
		field := options.Field(i)
		pkg := packagePathOf(field.Type)

		for _, client := range clientPackages {
			if pkg == client {
				t.Errorf("Options.%s is of type %s from %s; the controller must receive interfaces, not clients",
					field.Name, field.Type, pkg)
			}
		}
	}
}

// There is exactly one Actuator in the repository and it writes nothing. A
// second implementation appearing before P3 is the moment the phase's guarantee
// would be given up, so it must be a test failure rather than a code review
// someone might skim.
func TestActuator_HasExactlyOneImplementation(t *testing.T) {
	t.Parallel()

	root := repoRoot(t)
	var implementations []string

	forEachGoFile(t, root, func(path string, file *ast.File) {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Name.Name != "Apply" {
				continue
			}
			// An Apply method taking a scaling.Decision is an Actuator.
			if !mentionsDecision(fn.Type) {
				continue
			}
			implementations = append(implementations, receiverName(fn)+" in "+rel(root, path))
		}
	})

	if len(implementations) != 1 {
		t.Errorf("found %d Actuator implementations, want exactly 1 (DryRunActuator):\n  %s",
			len(implementations), strings.Join(implementations, "\n  "))
	}
	if len(implementations) == 1 && !strings.Contains(implementations[0], "DryRunActuator") {
		t.Errorf("the only Actuator is %q, want DryRunActuator", implementations[0])
	}
}

// The boundary check: no package that computes decisions may import a
// Kubernetes client. This is the assertion that fails if someone "just needs a
// clientset here for a moment".
func TestOnlyTheKubernetesPackageImportsAClient(t *testing.T) {
	t.Parallel()

	root := repoRoot(t)

	forEachGoFile(t, root, func(path string, file *ast.File) {
		pkg := filepath.ToSlash(filepath.Dir(rel(root, path)))

		allowed, known := packagesAllowedAClient[pkg]
		if !known {
			t.Errorf("package %q is not listed in packagesAllowedAClient; add it with an explicit decision", pkg)
			return
		}
		if allowed {
			return
		}

		for _, imported := range file.Imports {
			importPath, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				t.Fatalf("%s: unparsable import %s", path, imported.Path.Value)
			}
			for _, client := range clientPackages {
				if importPath == client || strings.HasPrefix(importPath, client+"/") {
					t.Errorf("%s imports %s; only internal/kubernetes may hold a Kubernetes client",
						rel(root, path), importPath)
				}
			}
		}
	})
}

// A source-level sweep for mutating calls, as a backstop to the import check: a
// write could in principle be issued through an already-imported type.
//
// internal/kubernetes is exempt because it legitimately names these verbs — in
// the transport guard and in its comments — and its own tests prove the
// transport refuses them.
func TestNoMutatingCallsOutsideTheKubernetesPackage(t *testing.T) {
	t.Parallel()

	root := repoRoot(t)

	// Calls that share a name with a client-go verb but are demonstrably not
	// Kubernetes writes. Each entry is a deliberate exception rather than a
	// blanket suppression.
	allowedReceivers := map[string]string{
		"pruneHistory": "local slice helper",
	}

	forEachGoFile(t, root, func(path string, file *ast.File) {
		pkg := filepath.ToSlash(filepath.Dir(rel(root, path)))
		if pkg == "internal/kubernetes" {
			return
		}

		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			for _, verb := range mutatingMethods {
				if selector.Sel.Name != verb {
					continue
				}
				if _, allowed := allowedReceivers[exprString(selector.X)]; allowed {
					continue
				}
				t.Errorf("%s calls .%s() on %s; Phase 1 must issue no mutating request",
					rel(root, path), verb, exprString(selector.X))
			}
			return true
		})
	})
}

// The RBAC manifests must grant no write verb. This layer holds even if every
// Go-level guarantee above is broken, which is why it is asserted against the
// shipped YAML rather than trusted.
func TestRBACManifests_GrantNoWriteVerb(t *testing.T) {
	t.Parallel()

	root := repoRoot(t)
	writeVerbs := []string{
		"create", "update", "patch", "delete", "deletecollection",
	}

	for _, name := range []string{"clusterrole.yaml", "role.yaml"} {
		path := filepath.Join(root, "deploy", "kubescalesense", name)

		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}

		for i, line := range strings.Split(string(raw), "\n") {
			trimmed := strings.TrimSpace(line)
			// Only rule lines matter. The deferred-permission tables in the
			// comments name every future verb on purpose.
			if !strings.HasPrefix(trimmed, "verbs:") {
				continue
			}
			for _, verb := range writeVerbs {
				if strings.Contains(trimmed, verb) {
					t.Errorf("%s:%d grants %q: %s", name, i+1, verb, trimmed)
				}
			}
		}
	}
}

// --- helpers ---------------------------------------------------------------

// repoRoot walks up from the package directory to the module root, so the test
// does not depend on where `go test` was invoked from.
func repoRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for range 6 {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatal("could not locate the module root from the test's working directory")
	return ""
}

// forEachGoFile visits every non-test Go file under cmd/ and internal/.
func forEachGoFile(t *testing.T, root string, visit func(path string, file *ast.File)) {
	t.Helper()

	fset := token.NewFileSet()
	visited := 0

	// tests/ is swept too, from P2 onwards: the demo tooling runs against a
	// real cluster, which makes it the most plausible place for a Kubernetes
	// client to appear "just for setup" — and a generator that can scale the
	// target would invalidate every observation made about it.
	for _, dir := range []string{"cmd", "internal", "tests"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}

			// Parsed in full rather than imports-only: the same walk feeds both
			// the import sweep and the call-site sweep.
			parsed, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}

			visited++
			visit(path, parsed)
			return nil
		})
		if err != nil {
			t.Fatalf("walking %s: %v", dir, err)
		}
	}

	// A silent zero would make every assertion in this file vacuously true.
	if visited < 10 {
		t.Fatalf("only %d Go files visited; the source sweep is not covering the repository", visited)
	}
}

func rel(root, path string) string {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return path
	}
	return filepath.ToSlash(relative)
}

func mentionsDecision(sig *ast.FuncType) bool {
	if sig.Params == nil {
		return false
	}
	for _, param := range sig.Params.List {
		if strings.Contains(exprString(param.Type), "Decision") {
			return true
		}
	}
	return false
}

func receiverName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return ""
	}
	return strings.TrimPrefix(exprString(fn.Recv.List[0].Type), "*")
}

// exprString renders the expressions this file needs to name: identifiers,
// selectors, and pointers to them.
func exprString(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return exprString(e.X) + "." + e.Sel.Name
	case *ast.StarExpr:
		return "*" + exprString(e.X)
	case *ast.IndexExpr:
		return exprString(e.X)
	case *ast.CallExpr:
		return exprString(e.Fun) + "(...)"
	default:
		return "?"
	}
}

// packagePathOf reports the import path of a type, following pointers and
// slices so a smuggled client is found wherever it is wrapped.
func packagePathOf(t reflect.Type) string {
	for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		t = t.Elem()
	}
	return t.PkgPath()
}
