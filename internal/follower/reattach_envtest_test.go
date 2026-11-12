package follower

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

// TestShouldReattachAfterPodLossAgainstTheRealAPIServer is the live bug of 2026-11-08 (AC-02): the
// API server defaults spec.idleTimeoutMinutes to 20 (and nodes, resumable fields) on the job the first
// pod created, so a retried pod that rebuilt the same spec from the same environment compared it with
// the defaulted copy, saw a "different spec" and refused to follow a healthy job. A fake client does
// not default, so only the real API server shows it.
func TestShouldReattachAfterPodLossAgainstTheRealAPIServer(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true}
	rc, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = hpcv1alpha1.AddToScheme(s)
	c, err := client.NewWithWatch(rc, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		mod  func(*Config)
	}{
		{"should_reattach_when_idle_timeout_is_the_platform_default", func(*Config) {}},
		{"should_reattach_when_idle_timeout_is_off", func(c *Config) { c.IdleTimeout = ptr.To(int32(0)) }},
		{"should_reattach_when_idle_timeout_is_set", func(c *Config) { c.IdleTimeout = ptr.To(int32(45)) }},
		{"should_reattach_when_resumable", func(c *Config) { c.ResumableLead = 15; c.MaxContinuations = 5 }},
		{"should_reattach_when_resumable_opted_out", func(c *Config) { c.ResumableOptOut = true }},
		{"should_reattach_when_sharing_and_env_are_set", func(c *Config) {
			c.Scope = "abcdef12"
			c.Env = []hpcv1alpha1.NameValue{{Name: "KAOS_LIB_SUPPORT", Value: "{}"}}
		}},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.Namespace = "default"
			cfg.WorkflowUID = fmt.Sprintf("uid-%d", i)
			tc.mod(&cfg)
			ctx := context.Background()
			// The first pod creates the job; the API server stores it with its defaults.
			if err := c.Create(ctx, cfg.Job()); err != nil {
				t.Fatal(err)
			}
			var stored hpcv1alpha1.HPCArgoWorkflowJob
			key := client.ObjectKey{Namespace: cfg.Namespace, Name: JobName(cfg.WorkflowUID, cfg.NodeKey)}
			if err := c.Get(ctx, key, &stored); err != nil {
				t.Fatal(err)
			}
			stored.Status = hpcv1alpha1.HPCArgoWorkflowJobStatus{Phase: hpcv1alpha1.PhaseSucceeded, Reason: "Completed", Result: testResult}
			if err := c.Status().Update(ctx, &stored); err != nil {
				t.Fatal(err)
			}
			// The pod is lost; Argo's retry starts a new pod that builds the same config.
			out := &bytes.Buffer{}
			dir := t.TempDir()
			code := Run(ctx, c, cfg, Options{Poll: 10 * time.Millisecond, Recheck: 50 * time.Millisecond,
				ResultPath: filepath.Join(dir, "o", "kaos-result.json"), SlurmIDPath: filepath.Join(dir, "id"), Out: out})
			if code != ExitSucceeded || !strings.Contains(out.String(), "re-attached") {
				t.Fatalf("exit %d, want a re-attach to the existing job:\n%s", code, out)
			}
		})
	}
}
