package idlewatch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
)

type fakePods struct {
	running, ended []corev1.Pod
}

func (f *fakePods) Running(context.Context) ([]corev1.Pod, error) { return f.running, nil }
func (f *fakePods) Ended(context.Context) ([]corev1.Pod, error)   { return f.ended, nil }

type fakeUtil struct {
	stats map[PodKey]UtilStat
	err   error
	calls []time.Duration
}

func (f *fakeUtil) Window(_ context.Context, w time.Duration, _ time.Time) (map[PodKey]UtilStat, error) {
	f.calls = append(f.calls, w)
	return f.stats, f.err
}

type fakeLogs struct {
	last  map[string]time.Time
	err   error
	calls int
}

func (f *fakeLogs) LastLine(_ context.Context, _, pod, _ string) (time.Time, error) {
	f.calls++
	return f.last[pod], f.err
}

type fakeEnder struct {
	ended, reaped []string
	err           error
}

func (f *fakeEnder) End(_ context.Context, p *corev1.Pod, _ string, _ time.Time) error {
	f.ended = append(f.ended, p.Name)
	return f.err
}
func (f *fakeEnder) Reap(_ context.Context, p *corev1.Pod) error {
	f.reaped = append(f.reaped, p.Name)
	return f.err
}

type fakeFlows struct {
	finished map[string]bool // by node id; absent: unknown
	err      error
}

func (f *fakeFlows) NodeFinished(_ context.Context, _, _, node string) (bool, bool, error) {
	fin, ok := f.finished[node]
	return fin, ok, f.err
}

func key(n string) PodKey { return PodKey{Namespace: "proj", Name: n} }

func newWatchdog(p *fakePods, u *fakeUtil, l *fakeLogs, e *fakeEnder) *Watchdog {
	return &Watchdog{Cfg: Config{}.WithDefaults(), Pods: p, Util: u, Logs: l, Ender: e, Flows: &fakeFlows{},
		Now: func() time.Time { return t0 }, Log: logr.Discard()}
}

func TestScan(t *testing.T) {
	old := t0.Add(-2 * time.Hour)
	quiet := UtilStat{Max: 0, Samples: 20}
	silent := t0.Add(-40 * time.Minute)
	cases := []struct {
		name       string
		pods       []*corev1.Pod
		util       *fakeUtil
		logs       *fakeLogs
		wantEnded  []string
		wantErrors int
		wantLogs   int
	}{
		{"should_end_the_quiet_silent_pod_and_keep_the_busy_one",
			[]*corev1.Pod{gpuPod("hung", old, nil), gpuPod("training", old, nil)},
			&fakeUtil{stats: map[PodKey]UtilStat{key("hung"): quiet, key("training"): {Max: 90, Samples: 20}}},
			&fakeLogs{last: map[string]time.Time{"hung": silent}}, []string{"hung"}, 0, 1},
		{"should_keep_a_quiet_pod_whose_log_still_grows",
			[]*corev1.Pod{gpuPod("loading", old, nil)},
			&fakeUtil{stats: map[PodKey]UtilStat{key("loading"): quiet}},
			&fakeLogs{last: map[string]time.Time{"loading": t0.Add(-time.Minute)}}, nil, 0, 1},
		{"should_end_nothing_when_the_metric_query_fails",
			[]*corev1.Pod{gpuPod("hung", old, nil)},
			&fakeUtil{err: errors.New("down")}, &fakeLogs{}, nil, 1, 0},
		{"should_end_nothing_when_the_pod_has_no_samples",
			[]*corev1.Pod{gpuPod("hung", old, nil)},
			&fakeUtil{stats: map[PodKey]UtilStat{}}, &fakeLogs{}, nil, 0, 0},
		{"should_end_nothing_when_the_log_read_fails",
			[]*corev1.Pod{gpuPod("hung", old, nil)},
			&fakeUtil{stats: map[PodKey]UtilStat{key("hung"): quiet}}, &fakeLogs{err: errors.New("403")}, nil, 1, 1},
		{"should_skip_a_pod_with_idle_timeout_zero",
			[]*corev1.Pod{gpuPod("opt-out", old, map[string]string{AnnotationIdleTimeout: "0"})},
			&fakeUtil{stats: map[PodKey]UtilStat{key("opt-out"): quiet}}, &fakeLogs{}, nil, 0, 0},
		{"should_honour_a_longer_window_from_the_annotation",
			[]*corev1.Pod{gpuPod("slow", t0.Add(-30*time.Minute), map[string]string{AnnotationIdleTimeout: "45"})},
			&fakeUtil{stats: map[PodKey]UtilStat{key("slow"): {Max: 0, Samples: 45}}}, &fakeLogs{}, nil, 0, 0},
		{"should_not_query_for_pods_younger_than_the_window",
			[]*corev1.Pod{gpuPod("new", t0.Add(-5*time.Minute), nil)},
			&fakeUtil{}, &fakeLogs{}, nil, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pods := &fakePods{}
			for _, p := range tc.pods {
				pods.running = append(pods.running, *p)
			}
			e := &fakeEnder{}
			res := newWatchdog(pods, tc.util, tc.logs, e).Scan(context.Background())
			if len(e.ended) != len(tc.wantEnded) || (len(tc.wantEnded) > 0 && e.ended[0] != tc.wantEnded[0]) {
				t.Fatalf("ended %v want %v", e.ended, tc.wantEnded)
			}
			if res.Errors != tc.wantErrors || tc.logs.calls != tc.wantLogs {
				t.Fatalf("errors %d (want %d), log reads %d (want %d)", res.Errors, tc.wantErrors, tc.logs.calls, tc.wantLogs)
			}
		})
	}
}

func TestScanQueriesOncePerWindow(t *testing.T) {
	old := t0.Add(-3 * time.Hour)
	pods := &fakePods{running: []corev1.Pod{*gpuPod("a", old, nil), *gpuPod("b", old, nil),
		*gpuPod("c", old, map[string]string{AnnotationIdleTimeout: "60"})}}
	u := &fakeUtil{stats: map[PodKey]UtilStat{}}
	newWatchdog(pods, u, &fakeLogs{}, &fakeEnder{}).Scan(context.Background())
	if len(u.calls) != 2 {
		t.Fatalf("want one query per distinct window (20m, 60m), got %v", u.calls)
	}
}

func endedPod(node string, phase corev1.PodPhase, ago time.Duration) corev1.Pod {
	p := gpuPod("p", t0.Add(-time.Hour), map[string]string{
		AnnotationIdleEnded: t0.Add(-ago).Format(time.RFC3339), AnnotationArgoNodeID: node})
	p.Labels[LabelIdleEnded] = valueTrue
	p.Status.Phase = phase
	return *p
}

func TestReap(t *testing.T) {
	cases := []struct {
		name       string
		pod        corev1.Pod
		finished   map[string]bool
		wantReaped int
		wantEnded  int
	}{
		{"should_delete_once_argo_recorded_the_step", endedPod("n1", corev1.PodFailed, 20*time.Second),
			map[string]bool{"n1": true}, 1, 0},
		{"should_wait_while_argo_has_not_recorded_the_step", endedPod("n1", corev1.PodFailed, 20*time.Second),
			map[string]bool{"n1": false}, 0, 0},
		{"should_wait_while_the_workflow_node_is_unreadable", endedPod("n1", corev1.PodFailed, 20*time.Second),
			nil, 0, 0},
		{"should_delete_after_the_confirm_timeout_when_unreadable", endedPod("n1", corev1.PodFailed, 6*time.Minute),
			nil, 1, 0},
		{"should_repeat_failed_when_the_kubelet_put_the_pod_back_to_running",
			endedPod("n1", corev1.PodRunning, 20*time.Second), map[string]bool{"n1": false}, 0, 1},
		{"should_not_delete_a_running_pod_before_argo_recorded_the_step",
			endedPod("n1", corev1.PodRunning, 20*time.Second), nil, 0, 1},
		{"should_delete_a_pod_without_a_node_id_only_after_the_timeout",
			endedPod("", corev1.PodFailed, 20*time.Second), nil, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := &fakeEnder{}
			w := newWatchdog(&fakePods{ended: []corev1.Pod{tc.pod}}, &fakeUtil{}, &fakeLogs{}, e)
			w.Flows = &fakeFlows{finished: tc.finished}
			res := w.Scan(context.Background())
			if res.Reaped != tc.wantReaped || len(e.ended) != tc.wantEnded {
				t.Fatalf("reaped %d (want %d), status repeated %d (want %d)", res.Reaped, tc.wantReaped, len(e.ended), tc.wantEnded)
			}
		})
	}
}

func TestAnEndedPodIsNeverJudgedAgain(t *testing.T) {
	p := endedPod("n1", corev1.PodRunning, time.Minute)
	u := &fakeUtil{stats: map[PodKey]UtilStat{key("p"): {Max: 0, Samples: 20}}}
	e := &fakeEnder{}
	w := newWatchdog(&fakePods{running: []corev1.Pod{p}}, u, &fakeLogs{}, e)
	w.Scan(context.Background())
	if len(u.calls) != 0 {
		t.Fatalf("an ended pod must not be a candidate, metric queried %v", u.calls)
	}
}
