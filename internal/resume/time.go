package resume

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func metaTime(t time.Time) metav1.Time { return metav1.NewTime(t) }
