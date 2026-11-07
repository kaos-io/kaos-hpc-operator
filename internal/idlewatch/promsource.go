package idlewatch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// UtilSource reads GPU utilisation per pod over a window.
// Implementations return only pods that have at least one sample; a pod missing from the result is
// unknown, never idle.
type UtilSource interface {
	Window(ctx context.Context, window time.Duration, at time.Time) (map[PodKey]UtilStat, error)
}

// PromSource reads the utilisation gauge from a PromQL API (the pool's VictoriaMetrics).
type PromSource struct {
	HTTP *http.Client
	Cfg  Config
}

// Window returns, per pod, the highest utilisation of any of its GPUs over the window and the
// smallest sample count of its GPU series. Two instant queries cover every pod of the pool.
func (p *PromSource) Window(ctx context.Context, window time.Duration, at time.Time) (map[PodKey]UtilStat, error) {
	sel := fmt.Sprintf("%s[%ds]", p.Cfg.Metric, int(window.Seconds()))
	by := p.Cfg.NamespaceLabel + ", " + p.Cfg.PodLabel
	maxQ := fmt.Sprintf("max by (%s) (max_over_time(%s))", by, sel)
	cntQ := fmt.Sprintf("min by (%s) (count_over_time(%s))", by, sel)
	maxes, err := p.query(ctx, maxQ, at)
	if err != nil {
		return nil, fmt.Errorf("idlewatch: utilisation query: %w", err)
	}
	counts, err := p.query(ctx, cntQ, at)
	if err != nil {
		return nil, fmt.Errorf("idlewatch: sample count query: %w", err)
	}
	out := make(map[PodKey]UtilStat, len(maxes))
	for k, m := range maxes {
		out[k] = UtilStat{Max: m, Samples: int(counts[k])}
	}
	return out, nil
}

type promResponse struct {
	Status string `json:"status"`
	Error  string `json:"error"`
	Data   struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
			Value  [2]any            `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

func (p *PromSource) query(ctx context.Context, q string, at time.Time) (map[PodKey]float64, error) {
	form := url.Values{"query": {q}, "time": {strconv.FormatInt(at.Unix(), 10)}}
	u := strings.TrimRight(p.Cfg.MetricsURL, "/") + "/api/v1/query"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := p.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	var pr promResponse
	if err := json.Unmarshal(body, &pr); err != nil || resp.StatusCode != http.StatusOK || pr.Status != "success" {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, firstNonEmpty(pr.Error, truncate(string(body), 200)))
	}
	out := make(map[PodKey]float64, len(pr.Data.Result))
	for _, r := range pr.Data.Result {
		key := PodKey{Namespace: r.Metric[p.Cfg.NamespaceLabel], Name: r.Metric[p.Cfg.PodLabel]}
		raw, _ := r.Value[1].(string)
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || key.Name == "" {
			continue // NaN or an unlabelled series says nothing about a pod
		}
		out[key] = v
	}
	return out, nil
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
