//go:build scale

package scale

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// writeReport renders the results of the scenarios that ran into REPORT.md (SCALE_REPORT overrides the path).
func writeReport() {
	reportMu.Lock()
	defer reportMu.Unlock()
	if len(results) == 0 {
		return
	}
	path := os.Getenv("SCALE_REPORT")
	if path == "" {
		path = "REPORT.md"
	}
	var b strings.Builder
	b.WriteString("# kaos-hpc-operator scale test report\n\n")
	fmt.Fprintf(&b, "Generated %s by `make scale-test` (build tag `scale`). Real controller binary (`cmd/main.go`) against envtest, "+
		"fake Slurm sites, fake runner pods. See [README.md](README.md) for the model, its limits and how to read the numbers.\n\n",
		time.Now().UTC().Format(time.RFC3339))
	b.WriteString("| scenario | result |\n|---|---|\n")
	for _, r := range results {
		status := "PASS"
		if len(r.failures) > 0 || r.t.Failed() {
			status = fmt.Sprintf("FAIL (%d assertions)", len(r.failures))
		}
		fmt.Fprintf(&b, "| %s | %s |\n", r.name, status)
	}
	for _, r := range results {
		fmt.Fprintf(&b, "\n## %s\n\n%s\n\n| measure | value |\n|---|---|\n", r.name, r.desc)
		for _, kv := range r.rows {
			fmt.Fprintf(&b, "| %s | %s |\n", kv[0], strings.ReplaceAll(kv[1], "|", "\\|"))
		}
		if len(r.failures) > 0 {
			b.WriteString("\nFailed assertions:\n\n")
			for _, f := range r.failures {
				fmt.Fprintf(&b, "- %s\n", f)
			}
		}
	}
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "writing the report:", err)
	}
}
