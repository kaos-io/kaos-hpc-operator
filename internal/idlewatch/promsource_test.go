package idlewatch

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func fakeProm(t *testing.T, seen *[]string, maxBody, cntBody string, status int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		form, _ := url.ParseQuery(string(b))
		q := form.Get("query")
		*seen = append(*seen, q)
		w.WriteHeader(status)
		if strings.HasPrefix(q, "max by") {
			_, _ = w.Write([]byte(maxBody))
		} else {
			_, _ = w.Write([]byte(cntBody))
		}
	}))
}

const (
	maxBody = `{"status":"success","data":{"result":[
	 {"metric":{"namespace_name":"proj","pod_name":"a"},"value":[1,"0"]},
	 {"metric":{"namespace_name":"proj","pod_name":"b"},"value":[1,"42.5"]},
	 {"metric":{"namespace_name":"proj","pod_name":"c"},"value":[1,"NaN"]}]}}`
	cntBody = `{"status":"success","data":{"result":[
	 {"metric":{"namespace_name":"proj","pod_name":"a"},"value":[1,"20"]},
	 {"metric":{"namespace_name":"proj","pod_name":"b"},"value":[1,"19"]}]}}`
)

func TestPromSourceWindow(t *testing.T) {
	var seen []string
	srv := fakeProm(t, &seen, maxBody, cntBody, 200)
	defer srv.Close()
	src := &PromSource{HTTP: srv.Client(), Cfg: Config{MetricsURL: srv.URL + "/select/0/prometheus"}.WithDefaults()}
	got, err := src.Window(context.Background(), DefaultWindow, t0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("NaN series must be dropped, got %v", got)
	}
	if a := got[PodKey{Namespace: "proj", Name: "a"}]; a.Max != 0 || a.Samples != 20 {
		t.Fatalf("a = %+v", a)
	}
	if b := got[PodKey{Namespace: "proj", Name: "b"}]; b.Max != 42.5 {
		t.Fatalf("b = %+v", b)
	}
	if !strings.Contains(seen[0], "max_over_time("+DefaultMetric+"[1200s])") ||
		!strings.Contains(seen[1], "count_over_time(") {
		t.Fatalf("unexpected queries %v", seen)
	}
}

func TestPromSourceReportsAServerError(t *testing.T) {
	var seen []string
	srv := fakeProm(t, &seen, `{"status":"error","error":"boom"}`, "", 422)
	defer srv.Close()
	src := &PromSource{HTTP: srv.Client(), Cfg: Config{MetricsURL: srv.URL}.WithDefaults()}
	if _, err := src.Window(context.Background(), DefaultWindow, t0); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("want the server's error, got %v", err)
	}
}
