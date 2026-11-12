package fakeslurm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// The simulated data plane: what the real runner finds for a job that ended well.
func TestTheDataPlaneServesWhatAJobThatEndedWellHandedBack(t *testing.T) {
	site := New(Options{Name: "t", Token: "tok", RunMin: 1, RunMax: 1, Dilation: 600, QOS: []QOS{{Name: "default", MaxWallMinutes: 60}}})
	defer site.Close()
	do := func(method, path, auth string, body []byte) (int, string) {
		t.Helper()
		req, _ := http.NewRequest(method, site.URL()+path, bytes.NewReader(body))
		req.Header.Set("X-SLURM-USER-NAME", "u1")
		req.Header.Set("X-SLURM-USER-TOKEN", "tok")
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := site.Srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		out, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(out)
	}
	submit := func(name string, env ...string) int {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"job": map[string]any{"name": name, "qos": "default", "time_limit": 10,
			"environment": env}, "script": "#!/bin/bash"})
		code, out := do(http.MethodPost, "/slurm/v0.0.44/job/submit", "", body)
		var reply struct {
			JobID int `json:"job_id"`
		}
		if code != http.StatusOK || json.Unmarshal([]byte(out), &reply) != nil {
			t.Fatalf("submit %s: %d %s", name, code, out)
		}
		return reply.JobID
	}
	object := func(id int, name string) (int, string) {
		return do(http.MethodGet, fmt.Sprintf("/lakefs/api/v1/repositories/outputs-a/branches/hpc-wf-s/objects?path=hpc-outputs/wf/s/%d/%s",
			id, name), "Bearer "+Wallet, nil)
	}
	context := func(handback string) string {
		return EnvContext + `={"version":1,"params":{"fake_site":{"handback":"` + handback + `"}},"ledger":{"version":2,` +
			`"reads":[{"name":"raw","repo":"dataset-raw","owner":null,"ref":"latest","commit":"c1"}],"writes":[]}}`
	}

	if code, out := do(http.MethodPost, "/oauth/v2/token", "", nil); code != http.StatusOK || !strings.Contains(out, Wallet) {
		t.Fatalf("the identity provider must mint the wallet: %d %s", code, out)
	}
	result := submit("with-a-result", EnvContext+`={"version":1,"params":{},"ledger":{"version":2,"reads":[],"writes":[]}}`)
	if code, _ := object(result, "kaos-result.json"); code != http.StatusNotFound {
		t.Fatalf("a job that has not ended hands nothing back, got %d", code)
	}
	ids := map[string]int{HandbackResult: result, HandbackNone: submit("none", context(HandbackNone)),
		HandbackLost: submit("lost", context(HandbackLost)), HandbackGarbage: submit("garbage", context(HandbackGarbage)),
		HandbackDown: submit("down", context(HandbackDown)), "no context": submit("bare")}
	deadline := time.Now().Add(20 * time.Second)
	for site.States()[Completed] < len(ids) {
		if time.Now().After(deadline) {
			t.Fatalf("the jobs did not end: %v", site.States())
		}
		time.Sleep(50 * time.Millisecond)
	}
	want := map[string][2]int{ // status of kaos-result.json, of kaos-result.absent
		HandbackResult: {200, 404}, HandbackNone: {404, 200}, HandbackLost: {404, 404}, HandbackGarbage: {200, 404},
		HandbackDown: {503, 503}, "no context": {404, 200},
	}
	for mode, id := range ids {
		a, body := object(id, "kaos-result.json")
		b, _ := object(id, "kaos-result.absent")
		if [2]int{a, b} != want[mode] {
			t.Errorf("%s: kaos-result.json %d, kaos-result.absent %d, want %v", mode, a, b, want[mode])
		}
		if mode == HandbackResult {
			var got map[string]any
			if json.Unmarshal([]byte(body), &got) != nil || got["status"] != "ok" || got["version"] != float64(1) {
				t.Errorf("the result is not a result: %s", body)
			}
		}
	}
	if code, _ := do(http.MethodGet, "/lakefs/api/v1/repositories/outputs-a/branches/hpc-wf-s/objects?path=hpc-outputs/wf/s/1/kaos-result.json",
		"Bearer other", nil); code != http.StatusUnauthorized {
		t.Errorf("a call without the wallet must be refused, got %d", code)
	}
	if code, _ := do(http.MethodDelete, "/lakefs/api/v1/repositories/outputs-a/branches/hpc-wf-s", "Bearer "+Wallet, nil); code != http.StatusNoContent {
		t.Errorf("the hand-off branch can be deleted, got %d", code)
	}
}
