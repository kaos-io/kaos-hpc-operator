package fakeslurm

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// The data plane a site job hands its step's result back through, simulated like the jobs themselves. The
// fake site never runs a job script, so nothing uploads a result; this stands in for what the script's
// hand-back program (runner/kaos_hpc_runner/payload/handback.py) would have left in lakeFS when the job
// ended, so that the REAL runner fetches, checks and reports it (PRD kaos#1911 F-04):
//
//	POST   /oauth/v2/token                                   the identity provider: mints the Wallet
//	GET    /lakefs/api/v1/repositories/<r>/branches/<b>/objects?path=hpc-outputs/<wf>/<step>/<job id>/<name>
//	DELETE /lakefs/api/v1/repositories/<r>/branches/<b>      the hand-off branch, when the runner is done
//	GET    /lakefs/api/v1/repositories/<r>/refs/<c>/objects/ls   an empty listing (no dataset is served)
//
// What a job "handed back" is decided by the run, in the step context the job was submitted with
// (KAOS_STEP_CONTEXT): params.fake_site.handback, one of the Handback* values. Without it: a result.
const (
	// Wallet is the bearer the fake identity provider mints; every lakeFS call must carry it.
	Wallet = "fake-site-wallet"
	// EnvContext is the step context in the submitted environment.
	EnvContext = "KAOS_STEP_CONTEXT"

	HandbackResult  = "result"      // the step wrote a result: served as kaos-result.json (plain JSON, not wire form)
	HandbackNone    = "none"        // the step ended well and wrote no result (the record kaos-result.absent): the job fails
	HandbackLost    = "lost"        // neither arrived: the upload never happened
	HandbackGarbage = "garbage"     // the result file is not JSON
	HandbackDown    = "unreachable" // lakeFS answers 503
)

// dataPlane answers a request to the simulated identity provider or lakeFS; false when the path is neither.
func (s *Site) dataPlane(w http.ResponseWriter, r *http.Request) bool {
	p := r.URL.Path
	if p == "/oauth/v2/token" && r.Method == http.MethodPost {
		writeJSON(w, http.StatusOK, map[string]any{"access_token": Wallet, "token_type": "Bearer", "expires_in": 43200})
		return true
	}
	rest, ok := strings.CutPrefix(p, "/lakefs/api/v1/repositories/")
	if !ok {
		return false
	}
	if r.Header.Get("Authorization") != "Bearer "+Wallet {
		writeErr(w, http.StatusUnauthorized, "no wallet")
		return true
	}
	parts := strings.Split(rest, "/")
	switch {
	case len(parts) == 3 && parts[1] == "branches" && r.Method == http.MethodDelete:
		w.WriteHeader(http.StatusNoContent)
	case len(parts) == 5 && parts[1] == "refs" && parts[3] == "objects" && parts[4] == "ls":
		writeJSON(w, http.StatusOK, map[string]any{"results": []any{}, "pagination": map[string]any{"has_more": false}})
	case len(parts) == 4 && parts[1] == "branches" && parts[3] == "objects" && r.Method == http.MethodGet:
		s.handedBack(w, r.URL.Query())
	default:
		writeErr(w, http.StatusNotFound, "no such endpoint")
	}
	return true
}

// handedBack serves what the job named in the object path left for the runner, if it ended well.
func (s *Site) handedBack(w http.ResponseWriter, q url.Values) {
	path := strings.Split(q.Get("path"), "/") // hpc-outputs/<wf>/<step>/<job id>/<name>
	if len(path) != 5 || path[0] != "hpc-outputs" {
		writeErr(w, http.StatusNotFound, "no such object")
		return
	}
	id, _ := strconv.Atoi(path[3])
	s.mu.Lock()
	j, known := s.jobs[id]
	var context string
	ended := known && j.State == Completed && j.Exit == 0
	if known {
		context = j.Env[EnvContext]
	}
	s.mu.Unlock()
	if !ended {
		writeErr(w, http.StatusNotFound, "no such object") // a job that did not end well hands nothing back
		return
	}
	var ctx struct {
		Params struct {
			FakeSite struct {
				Handback string `json:"handback"`
			} `json:"fake_site"`
		} `json:"params"`
		Ledger struct {
			Reads []json.RawMessage `json:"reads"`
		} `json:"ledger"`
	}
	mode := HandbackNone // a job submitted without a step context: its step wrote nothing
	if json.Unmarshal([]byte(context), &ctx) == nil && context != "" {
		if mode = ctx.Params.FakeSite.Handback; mode == "" {
			mode = HandbackResult
		}
	}
	name := path[4]
	switch {
	case mode == HandbackDown:
		writeErr(w, http.StatusServiceUnavailable, "lakeFS is not answering")
	case mode == HandbackGarbage && name == "kaos-result.json":
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("epoch 3 done, accuracy 0.93 {"))
	case mode == HandbackNone && name == "kaos-result.absent":
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("absent\n"))
	case mode == HandbackResult && name == "kaos-result.json":
		reads := ctx.Ledger.Reads
		if reads == nil {
			reads = []json.RawMessage{}
		}
		// As a step would write it with indentation: the runner writes it again in wire form.
		body, _ := json.MarshalIndent(map[string]any{"version": 1, "status": "ok",
			"values": map[string]any{"site_job": id, "simulated": true}, "reads": reads, "writes": []any{}}, "", "  ")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	default:
		writeErr(w, http.StatusNotFound, "no such object")
	}
}
