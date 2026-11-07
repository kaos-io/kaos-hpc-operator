package idlewatch

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
)

// logTailBytes bounds the read: the timestamp is the first token of the line, the rest is not needed.
const logTailBytes = 512

// LogReader tells when a container last wrote a log line.
type LogReader interface {
	// LastLine returns the time of the newest log line, or the zero time when the container has
	// written nothing. An error means "unknown".
	LastLine(ctx context.Context, namespace, pod, container string) (time.Time, error)
}

// KubeLogReader reads the last line through the pods/log subresource: one line, a few hundred
// bytes, answered from the end of the kubelet's log file, so it costs the same however long the log is.
type KubeLogReader struct {
	Client kubernetes.Interface
}

// LastLine implements LogReader.
func (k *KubeLogReader) LastLine(ctx context.Context, namespace, pod, container string) (time.Time, error) {
	rc, err := k.Client.CoreV1().Pods(namespace).GetLogs(pod, &corev1.PodLogOptions{
		Container: container, TailLines: ptr.To(int64(1)), Timestamps: true, LimitBytes: ptr.To(int64(logTailBytes)),
	}).Stream(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("idlewatch: read log of %s/%s: %w", namespace, pod, err)
	}
	defer func() { _ = rc.Close() }()
	line, err := bufio.NewReader(rc).ReadString('\n')
	if line == "" && err != nil && !errors.Is(err, io.EOF) {
		return time.Time{}, fmt.Errorf("idlewatch: read log line of %s/%s: %w", namespace, pod, err)
	}
	return ParseLogTime(line)
}

// ParseLogTime reads the RFC 3339 timestamp that starts a line read with Timestamps=true. An empty
// line is no output (zero time); a line that does not start with a timestamp is an error.
func ParseLogTime(line string) (time.Time, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return time.Time{}, nil
	}
	ts, _, _ := strings.Cut(line, " ")
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return time.Time{}, fmt.Errorf("idlewatch: log line without a timestamp: %w", err)
	}
	return t, nil
}
