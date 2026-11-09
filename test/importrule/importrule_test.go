// Package importrule enforces PRD-HPC-1461 AC-41: the controller module imports no KAOS internals,
// so it builds and runs in a cluster without KAOS. It runs in `make test` (the Argo
// kaos-hpc-operator-validate and -build workflows run that), so a violating import fails the PR.
package importrule

import (
	"os/exec"
	"strings"
	"testing"
)

// ownModule is this module; its packages are the only ones from the kaos-io organisation allowed.
const ownModule = "github.com/kaos-io/kaos-hpc-operator"

// internalPrefixes are import-path prefixes of KAOS-internal code. Everything not matching one of
// them (the standard library, k8s.io, sigs.k8s.io, other public modules) is allowed.
var internalPrefixes = []string{
	"novelcore/",            // novelcore/kubecore-operator and any module of that name space
	"github.com/novelcore/", // novelcore's GitHub organisation (kaos, kubecore-operator, ...)
	"github.com/kaos-io/",   // sibling KAOS modules (kaos-lib, ...); the module itself is exempted below
	"github.com/infra-novelcore/",
}

// internalImport returns the matching internal prefix of an import path, or "" when the path is allowed.
func internalImport(path string) string {
	if path == ownModule || strings.HasPrefix(path, ownModule+"/") {
		return ""
	}
	for _, p := range internalPrefixes {
		if strings.HasPrefix(path, p) {
			return p
		}
	}
	return ""
}

func TestShouldClassifyImportPaths(t *testing.T) {
	cases := []struct {
		name, path string
		internal   bool
	}{
		{"should_allow_the_standard_library", "net/http", false},
		{"should_allow_public_kubernetes_modules", "sigs.k8s.io/controller-runtime/pkg/client", false},
		{"should_allow_the_own_module", ownModule + "/internal/controller", false},
		{"should_allow_the_own_module_root", ownModule, false},
		{"should_refuse_kubecore_operator", "novelcore/kubecore-operator/internal/framework", true},
		{"should_refuse_the_novelcore_github_org", "github.com/novelcore/kaos/pkg/x", true},
		{"should_refuse_a_sibling_kaos_io_module", "github.com/kaos-io/kaos-lib/go/x", true},
		{"should_not_allow_a_module_that_only_starts_like_the_own_one", ownModule + "-fork/x", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := internalImport(c.path) != ""; got != c.internal {
				t.Fatalf("internalImport(%q) internal=%v, want %v", c.path, got, c.internal)
			}
		})
	}
}

// listDeps returns every package the module's code, tests included, depends on under the given build tags.
func listDeps(t *testing.T, tags string) []string {
	t.Helper()
	args := []string{"list", "-deps", "-test", "-f", "{{.ImportPath}}"}
	if tags != "" {
		args = append(args, "-tags", tags)
	}
	cmd := exec.Command("go", append(args, "./...")...)
	cmd.Dir = "../.."
	out, err := cmd.Output()
	if err != nil {
		stderr := ""
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = string(ee.Stderr)
		}
		t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, stderr)
	}
	return strings.Fields(string(out))
}

// TestShouldImportNoKAOSInternals fails when any package of the module, its tests or its tagged
// harnesses (scale, e2e), depends on a KAOS-internal module.
func TestShouldImportNoKAOSInternals(t *testing.T) {
	for _, tags := range []string{"", "scale"} {
		t.Run("tags="+tags, func(t *testing.T) {
			seen := map[string]bool{}
			for _, path := range listDeps(t, tags) {
				// "pkg [pkg.test]" and "pkg.test" are test variants of a package already listed.
				path, _, _ = strings.Cut(path, " ")
				if seen[path] {
					continue
				}
				seen[path] = true
				if p := internalImport(path); p != "" {
					t.Errorf("imports %s, a KAOS-internal path (prefix %s): the controller must run without KAOS (AC-41)", path, p)
				}
			}
			if len(seen) < 50 {
				t.Fatalf("only %d packages listed: go list did not see the module's dependencies", len(seen))
			}
		})
	}
}
