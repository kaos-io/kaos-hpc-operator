// Package chartsync checks that the Helm chart in dist/chart ships the CRDs and RBAC generated from
// the API types (config/), so an API change can never be released with a stale chart.
// CRDs: make manifests (hack/sync-chart-crds.sh copies them into the chart). RBAC rules: copy from config/rbac by hand;
// the chart has hand-written templates, so it is edited in place and not regenerated.
package chartsync

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// Template lines ({{ ... }}) and the lines they fill are dropped; what is left is plain YAML.
var templated = regexp.MustCompile(`(?m)^.*\{\{.*\}\}.*\n`)

func load(t *testing.T, path string, template bool) map[string]any {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if template {
		b = templated.ReplaceAll(b, nil)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(b, &doc); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return doc
}

func files(t *testing.T, dir string) []string {
	t.Helper()
	m, err := filepath.Glob(filepath.Join("..", "..", dir, "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestTheChartShipsTheGeneratedCRDs(t *testing.T) {
	generated := files(t, "config/crd/bases")
	if len(generated) == 0 {
		t.Fatal("no generated CRDs")
	}
	if got := len(files(t, "dist/chart/templates/crd")); got != len(generated) {
		t.Fatalf("the chart has %d CRDs, config/crd/bases %d: regenerate the chart", got, len(generated))
	}
	for _, g := range generated {
		chart := filepath.Join("..", "..", "dist/chart/templates/crd", filepath.Base(g))
		if !reflect.DeepEqual(load(t, g, false)["spec"], load(t, chart, true)["spec"]) {
			t.Errorf("%s differs from %s: regenerate the chart", chart, g)
		}
	}
}

func TestTheChartShipsTheGeneratedRBAC(t *testing.T) {
	for _, g := range files(t, "config/rbac") {
		doc := load(t, g, false)
		if doc["kind"] != "ClusterRole" {
			continue
		}
		name := filepath.Base(g)
		chart := filepath.Join("..", "..", "dist/chart/templates/rbac", name)
		if _, err := os.Stat(chart); err != nil {
			if strings.HasPrefix(name, "metrics_") {
				continue // the chart renders the metrics roles under metrics.enable
			}
			t.Errorf("%s is not in the chart: regenerate it", name)
			continue
		}
		if !reflect.DeepEqual(doc["rules"], load(t, chart, true)["rules"]) {
			t.Errorf("rules of %s differ from %s: regenerate the chart", chart, g)
		}
	}
}
