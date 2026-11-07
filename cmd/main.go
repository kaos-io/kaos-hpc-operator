/*
Copyright 2026 The KAOS Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"net/http"
	"os"
	"path/filepath"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/controller"
	"github.com/kaos-io/kaos-hpc-operator/internal/lanes"
	"github.com/kaos-io/kaos-hpc-operator/internal/runnerjob"
	"github.com/kaos-io/kaos-hpc-operator/internal/sitestate"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(hpcv1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// defaultSitePollInterval is $SITE_POLL_INTERVAL when it parses, else the built-in default.
func defaultSitePollInterval() time.Duration {
	if d, err := time.ParseDuration(os.Getenv("SITE_POLL_INTERVAL")); err == nil && d >= 0 {
		return d
	}
	return controller.DefaultSitePollInterval
}

// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var tlsOpts []func(*tls.Config)
	var runnerImage, runnerScheduling, laneCandidates string
	var sitePollInterval time.Duration
	var siteRestRate float64
	var siteRestBurst int
	var siteMissingGrace, sweepGiveUp time.Duration
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	flag.StringVar(&runnerImage, "runner-image", os.Getenv("RUNNER_IMAGE"),
		"Image of the runner Jobs of serve-mode jobs (the same image as the controller). Default: $RUNNER_IMAGE.")
	flag.StringVar(&runnerScheduling, "runner-scheduling", os.Getenv("RUNNER_SCHEDULING"),
		"JSON {nodeSelector, tolerations, affinity} for runner pods. Default: $RUNNER_SCHEDULING.")
	flag.StringVar(&laneCandidates, "lane-candidates", os.Getenv("LANE_CANDIDATES"),
		"JSON list of lanes tried in order for serve-mode attempts, for example "+
			`[{"qos":"test"},{"qos":"dev","reservationByPartition":{"gpu":"gpudev"}}]`+
			". Empty (default): every attempt uses its class's own QoS. Default: $LANE_CANDIDATES.")
	flag.DurationVar(&sitePollInterval, "site-poll-interval", defaultSitePollInterval(),
		"How often the controller reads the state of all site jobs of one HPCSite (one batched call). "+
			"0 turns the poller off and runners poll the site themselves. Default: $SITE_POLL_INTERVAL or 30s.")
	flag.Float64Var(&siteRestRate, "site-rest-rate", 2,
		"Token bucket per site profile: REST calls per second the controller may make.")
	flag.DurationVar(&siteMissingGrace, "site-missing-grace", controller.DefaultSiteMissingGrace,
		"How long a booked job survives its HPCSite (or project label) being gone before it is cancelled.")
	flag.DurationVar(&sweepGiveUp, "sweep-give-up", controller.DefaultSweepGiveUp,
		"How long the controller waits for the site to confirm that a job's site job ended before it settles without proof.")
	flag.IntVar(&siteRestBurst, "site-rest-burst", 5, "Token bucket per site profile: burst size.")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Create watchers for metrics and webhooks certificates
	var metricsCertWatcher, webhookCertWatcher *certwatcher.CertWatcher

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		var err error
		webhookCertWatcher, err = certwatcher.New(
			filepath.Join(webhookCertPath, webhookCertName),
			filepath.Join(webhookCertPath, webhookCertKey),
		)
		if err != nil {
			setupLog.Error(err, "Failed to initialize webhook certificate watcher")
			os.Exit(1)
		}

		webhookTLSOpts = append(webhookTLSOpts, func(config *tls.Config) {
			config.GetCertificate = webhookCertWatcher.GetCertificate
		})
	}

	webhookServer := webhook.NewServer(webhook.Options{
		TLSOpts: webhookTLSOpts,
	})

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// TODO(user): If you enable certManager, uncomment the following lines:
	// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		var err error
		metricsCertWatcher, err = certwatcher.New(
			filepath.Join(metricsCertPath, metricsCertName),
			filepath.Join(metricsCertPath, metricsCertKey),
		)
		if err != nil {
			setupLog.Error(err, "to initialize metrics certificate watcher", "error", err)
			os.Exit(1)
		}

		metricsServerOptions.TLSOpts = append(metricsServerOptions.TLSOpts, func(config *tls.Config) {
			config.GetCertificate = metricsCertWatcher.GetCertificate
		})
	}

	var scheduling runnerjob.Scheduling
	if runnerScheduling != "" {
		if err := json.Unmarshal([]byte(runnerScheduling), &scheduling); err != nil {
			setupLog.Error(err, "invalid --runner-scheduling")
			os.Exit(1)
		}
	}
	if runnerImage == "" {
		setupLog.Info("no --runner-image: serve-mode jobs (spec.stepName) cannot start until it is set")
	}

	candidates, err := lanes.Parse(laneCandidates)
	if err != nil {
		setupLog.Error(err, "invalid --lane-candidates")
		os.Exit(1)
	}
	if len(candidates) > 0 {
		setupLog.Info("lane placement on", "candidates", len(candidates))
	}

	// Only the runner Jobs and their pods are cached, not every Job and Pod of the cluster.
	jobSel, err := labels.Parse(runnerjob.LabelJobUID)
	if err != nil {
		setupLog.Error(err, "invalid runner Job selector")
		os.Exit(1)
	}
	podSel, err := labels.Parse(runnerjob.LabelRunnerJob)
	if err != nil {
		setupLog.Error(err, "invalid runner pod selector")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		Cache: cache.Options{ByObject: map[client.Object]cache.ByObject{
			&batchv1.Job{}: {Label: jobSel},
			&corev1.Pod{}:  {Label: podSel},
		}},
		LeaderElection:   enableLeaderElection,
		LeaderElectionID: "65fe7cb2.kaos.io",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	jobs := &controller.HPCArgoWorkflowJobReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("kaos-hpc-operator"),
		// Reads that must not be stale (are the runner Jobs gone?) skip the cache.
		APIReader:        mgr.GetAPIReader(),
		RunnerImage:      runnerImage,
		RunnerScheduling: scheduling,
		LaneCandidates:   candidates,
		SiteMissingGrace: siteMissingGrace,
		SweepGiveUp:      sweepGiveUp,
	}
	if err := jobs.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "HPCArgoWorkflowJob")
		os.Exit(1)
	}
	if err := (&controller.HPCSiteReconciler{Client: mgr.GetClient(), Jobs: jobs}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "HPCSite")
		os.Exit(1)
	}
	if sitePollInterval > 0 {
		poller := &controller.SitePoller{
			Client: mgr.GetClient(), Secrets: mgr.GetAPIReader(), Interval: sitePollInterval,
			Reader:     &sitestate.Client{HTTP: &http.Client{Timeout: 30 * time.Second}},
			BucketRate: siteRestRate, BucketBurst: siteRestBurst,
		}
		if err := mgr.Add(poller); err != nil {
			setupLog.Error(err, "unable to add the site poller")
			os.Exit(1)
		}
	}
	// +kubebuilder:scaffold:builder

	if metricsCertWatcher != nil {
		setupLog.Info("Adding metrics certificate watcher to manager")
		if err := mgr.Add(metricsCertWatcher); err != nil {
			setupLog.Error(err, "unable to add metrics certificate watcher to manager")
			os.Exit(1)
		}
	}

	if webhookCertWatcher != nil {
		setupLog.Info("Adding webhook certificate watcher to manager")
		if err := mgr.Add(webhookCertWatcher); err != nil {
			setupLog.Error(err, "unable to add webhook certificate watcher to manager")
			os.Exit(1)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
