package controller

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	kubernetesaccess "github.com/jensilin/KubeScaleSense/internal/kubernetes"
)

// This file is the machine-checked form of the project's central safety claim.
//
// Through P2 the claim was "there is no path by which this controller could
// change the cluster". P3 gives it paths, so the claim changes shape rather than
// weakening: the paths are enumerated, each is bounded to one kind of change
// regarding the one target, and every layer that used to say "nothing" now
// says "these and nothing else". Exactly one of them can change how many pods
// run — the scale subresource — and none can touch the pod template.
//
// Four independent layers enforce it, and each is asserted separately, because
// any one of them could be removed by a plausible-looking future edit:
//
//	interface    the controller's observation view is a ClusterReader, which
//	             has no mutating method and no clientset behind it
//	surface      exactly two Actuators exist, and every mutating client-go
//	             call in the repository is one of wantWriteSites, all in
//	             internal/kubernetes
//	transport    the observation client refuses every non-GET method, and
//	             each write client refuses everything but its one request
//	             shape (internal/kubernetes)
//	RBAC         the ServiceAccount's write rules are exactly wantWriteRules
//	             (deploy/)
//
// The transport and RBAC layers hold even if the code is wrong. The tests below
// cover the first two and, more importantly, guard the boundary: they fail if a
// clientset is ever wired into a package that computes decisions, or if a second
// way to mutate the cluster appears anywhere.

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

// wantActuators is the complete set of things in this repository that can be
// handed to the reconcile loop as its actuator.
//
// Two, named. A third appearing is the moment the safety surface grows, and
// that should be a failing test rather than a code review someone might skim —
// whether the newcomer writes anything or not, because the question "how many
// things here can change a cluster?" should have an answer that is checked.
var wantActuators = map[string]string{
	"DryRunActuator": "writes nothing and performs no API call at all",
	"ScaleActuator":  "writes spec.replicas through the scale subresource, and nothing else",
}

func TestActuator_ImplementationsAreTheExpectedTwo(t *testing.T) {
	t.Parallel()

	root := repoRoot(t)
	found := map[string]string{}

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
			found[receiverName(fn)] = rel(root, path)
		}
	})

	for name, why := range wantActuators {
		if _, ok := found[name]; !ok {
			t.Errorf("Actuator %s is missing; it should exist and %s", name, why)
		}
	}
	for name, where := range found {
		if _, expected := wantActuators[name]; !expected {
			t.Errorf("%s in %s is a third Actuator implementation; the set of things that can act on a "+
				"cluster is deliberately enumerated, so add it to wantActuators with an explicit decision",
				name, where)
		}
	}
}

// wantWriteSites is every mutating client-go call in the repository, as
// "file verb", with what it can change.
//
// Each entry has its own file, its own transport guard, and its own RBAC rule,
// so auditing a write path means reading one file. Adding an entry is the
// moment the safety surface grows, and it should be a deliberate edit here
// rather than something a sweep happens not to notice.
var wantWriteSites = map[string]string{
	"internal/kubernetes/scale.go UpdateScale": "spec.replicas of the target, through deployments/scale and nothing " +
		"else (FR-01)",
	"internal/kubernetes/events.go Create": "a new Event regarding the target; it creates announcements and cannot " +
		"edit or delete anything (FR-24)",
	"internal/kubernetes/deletioncost.go Patch": "the pod-deletion-cost annotation of a pod, and no other field — " +
		"the transport validates the body (FR-21)",
}

// TestNoMutatingCallsOutsideTheKubernetesPackage exempts internal/kubernetes
// because it legitimately names these verbs. This is the other half of that
// exemption: inside that package, the mutating calls must be exactly
// wantWriteSites, each appearing once. Without this pair, "only
// internal/kubernetes may write" would be an unbounded licence for the one
// package nobody else is checking.
func TestTheKubernetesPackageWritesOnlyAtTheEnumeratedSites(t *testing.T) {
	t.Parallel()

	root := repoRoot(t)
	found := map[string]int{}

	forEachGoFile(t, root, func(path string, file *ast.File) {
		if filepath.ToSlash(filepath.Dir(rel(root, path))) != "internal/kubernetes" {
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
				if selector.Sel.Name == verb {
					found[rel(root, path)+" "+verb]++
				}
			}
			return true
		})
	})

	for site, what := range wantWriteSites {
		switch found[site] {
		case 1:
		case 0:
			t.Errorf("expected write site %s is missing; it should exist and write %s", site, what)
		default:
			t.Errorf("write site %s appears %d times; each write path is a single call a reviewer can find",
				site, found[site])
		}
	}
	for site := range found {
		if _, expected := wantWriteSites[site]; !expected {
			t.Errorf("%s is a mutating client-go call outside the enumerated write sites; add it to "+
				"wantWriteSites with what it can change, or remove it (architecture § 7)", site)
		}
	}
}

// The write capability handed to the reconcile loop must stay two methods wide.
//
// The companion to TestClusterReader_ExposesNoMutatingMethod: that one bounds
// what the controller can read, this one bounds what it can write. Asserted by
// reflection so it holds against the compiled interface, and by exact method
// set rather than by a name filter — because the risk here is not a method
// called Delete, it is a fourth method that quietly does something else.
func TestScaleTarget_IsTwoMethodsWide(t *testing.T) {
	t.Parallel()

	target := reflect.TypeOf((*ScaleTarget)(nil)).Elem()

	want := map[string]bool{"Current": true, "Write": true}
	got := map[string]bool{}

	for i := range target.NumMethod() {
		got[target.Method(i).Name] = true
	}

	if !reflect.DeepEqual(want, got) {
		t.Errorf("ScaleTarget exposes %v, want exactly %v; the actuator's capability is deliberately "+
			"limited to reading and setting one replica count", keysOf(got), keysOf(want))
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

// wantWriteRules is every RBAC rule that carries a write verb, keyed by
// "file apiGroups resources", with its exact verbs line.
//
// All of them are in the namespaced Role, so every write is confined to the
// target's namespace. Each matches one entry of wantWriteSites.
var wantWriteRules = map[string]string{
	`role.yaml ["apps"] ["deployments/scale"]`: `verbs: ["get", "update", "patch"]`,
	`role.yaml ["events.k8s.io"] ["events"]`:   `verbs: ["create"]`,
	`role.yaml [""] ["pods"]`:                  `verbs: ["patch"]`,
}

// The RBAC manifests must grant exactly the enumerated write rules.
//
// This layer holds even if every Go-level guarantee above is broken, which is
// why it is asserted against the shipped YAML rather than trusted. The
// assertion is deliberately two-sided: each rule must be present, because
// without it its feature does not work, and no other rule may carry a write
// verb, because that is the bound on what a bug can do. In particular nothing
// may grant a write on `deployments` itself, which would reach the pod
// template (FS-21), or `delete` on pods (I-11).
func TestRBACManifests_GrantExactlyTheEnumeratedWriteRules(t *testing.T) {
	t.Parallel()

	root := repoRoot(t)
	writeVerbs := []string{"create", "update", "patch", "delete", "deletecollection", "escalate", "bind", "impersonate"}

	found := map[string][]string{}

	for _, name := range []string{"clusterrole.yaml", "role.yaml"} {
		path := filepath.Join(root, "deploy", "kubescalesense", name)

		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}

		// The rule a verbs: line belongs to is the apiGroups: and resources:
		// lines above it, which is enough structure for a rule list this small
		// and avoids pulling a YAML parser into a guard test.
		apiGroups, resources := "", ""
		for i, line := range strings.Split(string(raw), "\n") {
			trimmed := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "- "))

			// Comments are skipped: the deferred-permission tables name every
			// future verb on purpose.
			if strings.HasPrefix(trimmed, "#") {
				continue
			}
			if strings.HasPrefix(trimmed, "apiGroups:") {
				apiGroups = strings.TrimSpace(strings.TrimPrefix(trimmed, "apiGroups:"))
				continue
			}
			if strings.HasPrefix(trimmed, "resources:") {
				resources = strings.TrimSpace(strings.TrimPrefix(trimmed, "resources:"))
				continue
			}
			if !strings.HasPrefix(trimmed, "verbs:") {
				continue
			}

			for _, verb := range writeVerbs {
				if strings.Contains(trimmed, `"`+verb+`"`) || strings.Contains(trimmed, `"*"`) {
					rule := fmt.Sprintf("%s %s %s", name, apiGroups, resources)
					found[rule] = append(found[rule], fmt.Sprintf("line %d: %s", i+1, trimmed))
					break
				}
			}
		}
	}

	for rule, verbs := range wantWriteRules {
		lines, ok := found[rule]
		switch {
		case !ok:
			t.Errorf("no rule grants %s %s; the feature behind it cannot work without it", rule, verbs)
		case len(lines) != 1 || !strings.HasSuffix(lines[0], verbs):
			t.Errorf("%s grants %v, want exactly one rule with %s (architecture § 7)", rule, lines, verbs)
		}
	}
	for rule, lines := range found {
		if _, expected := wantWriteRules[rule]; !expected {
			t.Errorf("%s carries a write verb outside the enumerated write rules:\n  %s",
				rule, strings.Join(lines, "\n  "))
		}
	}
}

// Within the one write rule, the verbs must be exactly the documented three.
// `create` and `delete` on a subresource are meaningless, and their appearance
// would mean the rule was written by pattern rather than by intent.
func TestRBACManifests_ScaleRuleGrantsOnlyGetUpdatePatch(t *testing.T) {
	t.Parallel()

	path := filepath.Join(repoRoot(t), "deploy", "kubescalesense", "role.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading role.yaml: %v", err)
	}

	lines := strings.Split(string(raw), "\n")
	for i, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		if !strings.Contains(line, `["deployments/scale"]`) {
			continue
		}
		if i+1 >= len(lines) {
			t.Fatal("the deployments/scale rule has no verbs line after it")
		}

		got := strings.TrimSpace(lines[i+1])
		const want = `verbs: ["get", "update", "patch"]`
		if got != want {
			t.Errorf("the deployments/scale rule grants %q, want %q (architecture § 7)", got, want)
		}
		return
	}
	t.Error("role.yaml declares no deployments/scale rule")
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

// keysOf renders a set for an error message, sorted so the message is stable.
func keysOf(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
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
