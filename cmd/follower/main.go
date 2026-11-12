// Command follower is the pipeline-side step of an HPC job (PRD-HPC-1461 D-59). It runs in the
// workflow pod, holds no site credential, and lets the controller do everything else.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
	"github.com/kaos-io/kaos-hpc-operator/internal/follower"
)

const (
	resultPath  = "/tmp/outputs/kaos-result.json"
	slurmIDPath = "/tmp/slurm-job-id"
	// terminationLog is the pod's termination message: the workflow shows it as the step's message.
	terminationLog = "/dev/termination-log"
)

func main() { os.Exit(run()) }

func run() int {
	cfg, err := follower.ConfigFromEnv(os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "follower:", err)
		// The step's message in the run is the pod's termination message: say why there, not only in the log.
		_ = os.WriteFile(terminationLog, []byte(err.Error()), 0o644)
		return follower.ExitFailed
	}
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(hpcv1alpha1.AddToScheme(scheme))
	restCfg, err := ctrl.GetConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, "follower: no cluster configuration:", err)
		return follower.ExitFailed
	}
	c, err := client.NewWithWatch(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		fmt.Fprintln(os.Stderr, "follower: cannot build the client:", err)
		return follower.ExitFailed
	}
	// Argo stops a step with SIGTERM; the controller (not this process) cancels the site job.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	return follower.Run(ctx, c, cfg, follower.Options{
		Poll: follower.DefaultPoll, Recheck: follower.DefaultRecheck,
		ResultPath: resultPath, SlurmIDPath: slurmIDPath, TerminationPath: terminationLog, Out: os.Stdout,
	})
}
