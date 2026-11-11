package fakeslurm

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
)

// JobEnv is what a test reads to see which environment a submitted job carries.
func TestJobEnvReturnsTheEnvironmentOfTheNewestJobOfAName(t *testing.T) {
	site := New(Options{Name: "t", Token: "tok", QOS: []QOS{{Name: "default", MaxWallMinutes: 60}}})
	defer site.Close()
	submit := func(name, value string) {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"job": map[string]any{"name": name, "qos": "default", "time_limit": 10,
			"environment": []string{"A=" + value, "EMPTY="}}, "script": "#!/bin/bash"})
		req, _ := http.NewRequest(http.MethodPost, site.URL()+"/slurm/v0.0.44/job/submit", bytes.NewReader(body))
		req.Header.Set("X-SLURM-USER-NAME", "u1")
		req.Header.Set("X-SLURM-USER-TOKEN", "tok")
		resp, err := site.Srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("submit %s: %s", name, resp.Status)
		}
	}
	if site.JobEnv("job-a") != nil {
		t.Fatal("an unknown job has an environment")
	}
	submit("job-a", "first")
	submit("job-b", "other")
	submit("job-a", "second")
	if got := site.JobEnv("job-a"); got["A"] != "second" || len(got) != 2 {
		t.Fatalf("job-a: %v, want the environment of its newest submission", got)
	}
	if got := site.JobEnv("job-b")["A"]; got != "other" {
		t.Fatalf("job-b: A=%q", got)
	}
}
