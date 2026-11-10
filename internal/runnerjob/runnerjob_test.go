package runnerjob

import (
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	hpcv1alpha1 "github.com/kaos-io/kaos-hpc-operator/api/v1alpha1"
)

func fixtures() (*hpcv1alpha1.HPCArgoWorkflowJob, *hpcv1alpha1.HPCSite) {
	job := &hpcv1alpha1.HPCArgoWorkflowJob{
		ObjectMeta: metav1.ObjectMeta{Name: "hpc-abc", Namespace: "env-ns", UID: "11111111-2222"},
		Spec: hpcv1alpha1.HPCArgoWorkflowJobSpec{Class: "meluxina-gpu", Image: "reg/app:v1", Command: []string{"python", "t.py"},
			WallTimeMinutes: 120, Dataset: &hpcv1alpha1.DatasetPin{Repo: "dataset-ds", Version: "v1", Commit: "c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2"}, Outputs: []string{"model", "m"}, StepName: "train",
			Workflow:  &hpcv1alpha1.WorkflowRef{Name: "wf-1", UID: "wf-uid"},
			Resumable: &hpcv1alpha1.Resumable{LeadMinutes: 20, MaxContinuations: 5},
			Sharing:   &hpcv1alpha1.Sharing{Scope: "abcdef12"},
			Env:       []hpcv1alpha1.NameValue{{Name: "OUTPUTS_REPO", Value: "outputs-app"}}},
	}
	site := &hpcv1alpha1.HPCSite{
		ObjectMeta: metav1.ObjectMeta{Name: "meluxina", Namespace: "ml-proj"},
		Spec: hpcv1alpha1.HPCSiteSpec{Site: "meluxina", Sharing: true,
			Profile: hpcv1alpha1.SiteProfile{Endpoint: "https://slurm-api.example", RESTVersion: "v0.0.44",
				ScratchRoot: "/project/scratch/p1", Classes: []hpcv1alpha1.SiteClass{{Name: "meluxina-gpu", Partition: "gpu",
					QOS: "default", Tier: hpcv1alpha1.TierGPU, GPUs: 4, MaxWallTimeMinutes: 2880}}},
			Identity: hpcv1alpha1.ProjectIdentity{User: "u1", Account: "p1"}},
	}
	return job, site
}

func envOf(t *testing.T, vars []corev1.EnvVar) map[string]string {
	t.Helper()
	m := map[string]string{}
	for _, v := range vars {
		if _, dup := m[v.Name]; dup {
			t.Fatalf("env %s set twice", v.Name)
		}
		m[v.Name] = v.Value
	}
	return m
}

func TestShouldBuildTheRunnerJobFromSpecAndSite(t *testing.T) {
	job, site := fixtures()
	rj, err := Build(Config{Image: "kaos/op:1"}, job, site, 1)
	if err != nil {
		t.Fatal(err)
	}
	if rj.Name != Name(job.UID, 1) || !strings.HasPrefix(rj.Name, "hpcr-") || !strings.HasSuffix(rj.Name, "-a1") || rj.Namespace != "ml-proj" {
		t.Errorf("name/namespace %s/%s", rj.Namespace, rj.Name)
	}
	if rj.Labels[LabelJobUID] != "11111111-2222" || rj.Labels[LabelAttempt] != "1" ||
		rj.Annotations[AnnotationJobName] != "hpc-abc" || rj.Annotations[AnnotationJobNamespace] != "env-ns" {
		t.Errorf("labels %v annotations %v", rj.Labels, rj.Annotations)
	}
	if *rj.Spec.BackoffLimit != 4 || *rj.Spec.TTLSecondsAfterFinished != 3600 || *rj.Spec.ActiveDeadlineSeconds != (120+480+30)*60 {
		t.Errorf("limits %d %d %d", *rj.Spec.BackoffLimit, *rj.Spec.TTLSecondsAfterFinished, *rj.Spec.ActiveDeadlineSeconds)
	}
	rules := rj.Spec.PodFailurePolicy.Rules
	if len(rules) != 1 || rules[0].Action != "Ignore" || rules[0].OnPodConditions[0].Type != corev1.DisruptionTarget {
		t.Errorf("podFailurePolicy %+v", rules)
	}
	checkPod(t, rj)
}

func checkPod(t *testing.T, rj *batchv1.Job) {
	t.Helper()
	p := rj.Spec.Template.Spec
	c := p.Containers[0]
	if p.ServiceAccountName != "hpc-runner" || p.RestartPolicy != corev1.RestartPolicyNever || c.Image != "kaos/op:1" {
		t.Errorf("pod %+v", p)
	}
	if c.EnvFrom[0].ConfigMapRef.Name != "hpc-runner-config" || c.EnvFrom[0].ConfigMapRef.Optional != nil {
		t.Errorf("envFrom %+v", c.EnvFrom)
	}
	if c.Resources.Requests.Cpu().String() != "50m" || c.Resources.Requests.Memory().String() != "128Mi" || c.Resources.Limits.Memory().String() != "512Mi" {
		t.Errorf("resources %+v", c.Resources)
	}
	vols := map[string]*corev1.SecretVolumeSource{}
	for _, v := range p.Volumes {
		vols[v.Name] = v.Secret
	}
	if vols["meluxina-jwt"] == nil || *vols["meluxina-jwt"].Optional || *vols["meluxina-jwt"].DefaultMode != 0o400 ||
		vols["meluxina-staging"] == nil || !*vols["meluxina-staging"].Optional || *vols["meluxina-staging"].DefaultMode != 0o400 ||
		vols["mlflow-svc"] == nil {
		t.Errorf("volumes %+v", p.Volumes)
	}
}

func TestShouldSetTheRunnerEnvironmentFromSpecAndSite(t *testing.T) {
	job, site := fixtures()
	job.Status.Resume = &hpcv1alpha1.ResumeInfo{CheckpointRef: "ckpt-1", FromAttempt: 1}
	env, err := Env(job, site, 2)
	if err != nil {
		t.Fatal(err)
	}
	m := envOf(t, env)
	want := map[string]string{
		"HPC_JOB_NAMESPACE": "env-ns", "HPC_JOB_NAME": "hpc-abc", "KAOS_ATTEMPT": "2",
		"KAOS_RESUME_REF": "ckpt-1", "KAOS_RESUME_FROM_ATTEMPT": "1",
		"IMAGE_REF": "reg/app:v1", "STEP_COMMAND": `["python","t.py"]`, "SLURM_TIME_LIMIT": "120",
		"HPC_RESUMABLE_LEAD": "20", "STEP_OUTPUTS": "model,m", "HPC_CLASS": "meluxina-gpu", "STEP_NAME": "train",
		"WF_UID": "wf-uid", "KUBECORE_WORKFLOW_NAME": "wf-1", "KUBECORE_RUN_ID": "wf-1", "DATASET_REPO": "dataset-ds", "DATASET_VERSION": "v1", "DATASET_COMMIT": "c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2", "DATASET_OWNER": "", "DATASET_UNPINNED": "",
		"HPC_SHARING": "1", "HPC_SCOPE": "abcdef12", "OUTPUTS_REPO": "outputs-app",
		"HPC_SITE": "meluxina", "HPC_ACCOUNT": "p1", "HPC_USER": "u1", "HPC_SCRATCH": "/project/scratch/p1",
		"HPC_HOME": "/home/users/u1", "HPC_ENDPOINT": "https://slurm-api.example", "HPC_REST_VERSION": "v0.0.44",
		"ZITADEL_MACHINE_KEY_FILE": "/etc/mlflow-svc/ZITADEL_MACHINE_KEY",
	}
	for k, v := range want {
		if m[k] != v {
			t.Errorf("%s = %q, want %q", k, m[k], v)
		}
	}
	if !strings.Contains(m["HPC_CLASSES_JSON"], `"name":"meluxina-gpu"`) || !strings.Contains(m["HPC_CLASSES_JSON"], `"partition":"gpu"`) {
		t.Errorf("HPC_CLASSES_JSON = %s", m["HPC_CLASSES_JSON"])
	}
	last := env[len(env)-1]
	if last.Name != "SLURM_TOKEN" || last.ValueFrom.SecretKeyRef.Name != "meluxina-jwt" || last.ValueFrom.SecretKeyRef.Key != "token" {
		t.Errorf("SLURM_TOKEN %+v", last)
	}
}

func TestShouldNotResumeOnTheFirstAttemptOrShareWhenTheSiteDoesNot(t *testing.T) {
	job, site := fixtures()
	job.Status.Resume = &hpcv1alpha1.ResumeInfo{CheckpointRef: "old", FromAttempt: 1}
	site.Spec.Sharing = false
	env, _ := Env(job, site, 1)
	m := envOf(t, env)
	if _, ok := m["KAOS_RESUME_REF"]; ok {
		t.Error("attempt 1 must not carry resume info")
	}
	if m["HPC_SHARING"] != "0" || m["HPC_SCOPE"] != "" {
		t.Errorf("sharing %q scope %q", m["HPC_SHARING"], m["HPC_SCOPE"])
	}
}

func TestEveryEndpointAndIdentityComesFromTheSiteNotTheSpec(t *testing.T) {
	job, site := fixtures()
	env, _ := Env(job, site, 1)
	allowed := map[string]bool{}
	for _, n := range hpcv1alpha1.AllowedEnvNames {
		allowed[n] = true
	}
	// Every variable a tenant can influence is a spec field or the allowlist; none carries an
	// endpoint, identity or path: those names must come from the site.
	siteOnly := []string{"HPC_ACCOUNT", "HPC_USER", "HPC_SCRATCH", "HPC_HOME", "HPC_CLASSES_JSON", "HPC_ENDPOINT", "HPC_REST_VERSION"}
	job.Spec.Image, job.Spec.Dataset = "x", nil
	site.Spec.Identity.User = "other"
	env2, _ := Env(job, site, 1)
	m1, m2 := envOf(t, env), envOf(t, env2)
	for _, k := range siteOnly {
		if k == "HPC_USER" || k == "HPC_HOME" {
			if m1[k] == m2[k] {
				t.Errorf("%s must follow the site", k)
			}
			continue
		}
		if m1[k] != m2[k] {
			t.Errorf("%s changed with the job spec", k)
		}
	}
	for _, e := range job.Spec.Env {
		if !allowed[e.Name] {
			t.Errorf("%s is not allowlisted", e.Name)
		}
	}
}

func TestValidateEnv(t *testing.T) {
	long := strings.Repeat("a", 2049)
	many := make([]hpcv1alpha1.NameValue, 17)
	for i := range many {
		many[i] = hpcv1alpha1.NameValue{Name: "OUTPUTS_REF", Value: "x"}
	}
	cases := []struct {
		name string
		env  []hpcv1alpha1.NameValue
		bad  bool
	}{
		{"should_accept_the_allowlist", []hpcv1alpha1.NameValue{{Name: "OUTPUTS_REPO", Value: "outputs-a-b"}, {Name: "OUTPUTS_REF", Value: "main"},
			{Name: "CHECKPOINT_BUCKET", Value: "outputs-a"}, {Name: "CHECKPOINT_PREFIX", Value: "p/"}, {Name: "KAOS_LIB_SUPPORT", Value: "{}"}}, false},
		{"should_refuse_a_slurm_endpoint", []hpcv1alpha1.NameValue{{Name: "HPC_ENDPOINT", Value: "https://evil"}}, true},
		{"should_refuse_a_token_override", []hpcv1alpha1.NameValue{{Name: "SLURM_TOKEN", Value: "x"}}, true},
		{"should_refuse_mlflow_and_lakefs_urls", []hpcv1alpha1.NameValue{{Name: "MLFLOW_EXTERNAL_URL", Value: "https://evil"}}, true},
		{"should_refuse_an_outputs_repo_that_is_not_an_outputs_repo", []hpcv1alpha1.NameValue{{Name: "OUTPUTS_REPO", Value: "secrets"}}, true},
		{"should_refuse_a_checkpoint_bucket_with_a_path", []hpcv1alpha1.NameValue{{Name: "CHECKPOINT_BUCKET", Value: "outputs-a/../x"}}, true},
		{"should_refuse_a_duplicate", []hpcv1alpha1.NameValue{{Name: "OUTPUTS_REF", Value: "a"}, {Name: "OUTPUTS_REF", Value: "b"}}, true},
		{"should_refuse_a_long_value", []hpcv1alpha1.NameValue{{Name: "OUTPUTS_REF", Value: long}}, true},
		{"should_refuse_more_than_sixteen", many, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateEnv(tc.env); (err != nil) != tc.bad {
				t.Fatalf("err = %v, want error: %v", err, tc.bad)
			}
		})
	}
}

func TestBuildRefusesAnEnvOutsideTheAllowlist(t *testing.T) {
	job, site := fixtures()
	job.Spec.Env = []hpcv1alpha1.NameValue{{Name: "HPC_ENDPOINT", Value: "https://evil"}}
	if _, err := Build(Config{Image: "i"}, job, site, 1); err == nil {
		t.Fatal("want an error")
	}
}

func TestNameIsDeterministicPerUIDAndAttempt(t *testing.T) {
	first := Name("a", 1)
	if first != Name("a", 1) || Name("a", 1) == Name("a", 2) || Name("a", 1) == Name("b", 1) {
		t.Fatal("name must be stable and unique per uid and attempt")
	}
}

func TestShouldGiveASharingRunnerTimeToFollowItsHostJobToItsEnd(t *testing.T) {
	// D-26 (live 2026-11-10, host 5439966): a host's Slurm job outlives its step while guests run, up to twice the
	// step's limit, and its runner follows it to its end; the runner Job's deadline leaves room for that.
	job, site := fixtures()
	job.Status.Placement = &hpcv1alpha1.Placement{TimeLimitMinutes: 30, Source: hpcv1alpha1.PlacementDeclared}
	job.Spec.Resumable = nil // a resumable step never hosts
	if job.Spec.Sharing == nil || !site.Spec.Sharing {
		t.Fatal("the fixtures share nodes")
	}
	rj, err := Build(Config{Image: "kaos/op:1"}, job, site, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := *rj.Spec.ActiveDeadlineSeconds; got != (2*30+480+30)*60 {
		t.Errorf("deadline %d, want twice the step's 30 min for the host job's tail", got)
	}
	site.Spec.Sharing = false
	if rj, _ = Build(Config{Image: "kaos/op:1"}, job, site, 1); *rj.Spec.ActiveDeadlineSeconds != (30+480+30)*60 {
		t.Errorf("deadline %d, want the step's limit when the site does not share nodes", *rj.Spec.ActiveDeadlineSeconds)
	}
}

func TestShouldUseThePlacementLimitForTheEnvAndTheDeadline(t *testing.T) {
	job, site := fixtures()
	job.Status.Placement = &hpcv1alpha1.Placement{TimeLimitMinutes: 30, Source: hpcv1alpha1.PlacementLearned, Samples: 5}
	rj, err := Build(Config{Image: "kaos/op:1"}, job, site, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := *rj.Spec.ActiveDeadlineSeconds; got != (30+480+30)*60 {
		t.Errorf("deadline %d, want it computed from the learned 30 min", got)
	}
	if got := envOf(t, rj.Spec.Template.Spec.Containers[0].Env)["SLURM_TIME_LIMIT"]; got != "30" {
		t.Errorf("SLURM_TIME_LIMIT %q, want 30", got)
	}
}

func TestShouldFallBackToTheSpecLimitWithoutAPlacement(t *testing.T) {
	job, site := fixtures()
	rj, err := Build(Config{Image: "kaos/op:1"}, job, site, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := envOf(t, rj.Spec.Template.Spec.Containers[0].Env)["SLURM_TIME_LIMIT"]; got != "120" {
		t.Errorf("SLURM_TIME_LIMIT %q, want the spec's 120", got)
	}
}

func TestShouldPassTheChosenLaneToTheRunnerAndNothingWithoutOne(t *testing.T) {
	cases := []struct {
		name       string
		placement  *hpcv1alpha1.Placement
		qos, resrv string
	}{
		{"should_pass_qos_and_reservation_of_a_reserved_lane", &hpcv1alpha1.Placement{TimeLimitMinutes: 60, QOS: "dev", Reservation: "gpudev"}, "dev", "gpudev"},
		{"should_pass_only_the_qos_of_a_lane_without_reservation", &hpcv1alpha1.Placement{TimeLimitMinutes: 20, QOS: "test"}, "test", ""},
		{"should_pass_nothing_when_the_class_default_applies", &hpcv1alpha1.Placement{TimeLimitMinutes: 60}, "", ""},
		{"should_pass_nothing_without_a_placement", nil, "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			job, site := fixtures()
			site.Spec.Profile.QOS = []hpcv1alpha1.SiteQOS{{Name: "test", MaxWallTimeMinutes: 30}, {Name: "dev"}}
			job.Status.Placement = c.placement
			vars, err := Env(job, site, 1)
			if err != nil {
				t.Fatal(err)
			}
			env := envOf(t, vars)
			maxWant := ""
			if c.qos == "test" {
				maxWant = "30" // the lane's maximum bounds the host's padding
			}
			for k, want := range map[string]string{"HPC_QOS": c.qos, "HPC_RESERVATION": c.resrv, "HPC_QOS_MAX_MINUTES": maxWant} {
				got, set := env[k]
				if (want != "") != set || got != want {
					t.Fatalf("%s = %q (set %v), want %q", k, got, set, want)
				}
			}
		})
	}
}

func TestShouldPassTheIdleTimeoutToTheRunner(t *testing.T) {
	cases := []struct {
		name string
		set  *int32
		want string
	}{
		{"should_default_to_20_minutes_when_the_spec_has_none", nil, "20"},
		{"should_pass_an_explicit_window", ptr.To(int32(45)), "45"},
		{"should_pass_0_which_turns_the_watchdog_off", ptr.To(int32(0)), "0"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			job, site := fixtures()
			job.Spec.IdleTimeoutMinutes = c.set
			rj, err := Build(Config{Image: "kaos/op:1"}, job, site, 1)
			if err != nil {
				t.Fatal(err)
			}
			if got := envOf(t, rj.Spec.Template.Spec.Containers[0].Env)["HPC_IDLE_TIMEOUT_MINUTES"]; got != c.want {
				t.Errorf("HPC_IDLE_TIMEOUT_MINUTES %q, want %q", got, c.want)
			}
		})
	}
}

func TestShouldBuildACancelOnlyRunnerThatNeverCollidesWithTheAttemptsRunner(t *testing.T) {
	job, site := fixtures()
	job.UID = "uid-1"
	normal, err := Build(Config{Image: "img"}, job, site, 1)
	if err != nil {
		t.Fatal(err)
	}
	c, err := BuildCancel(Config{Image: "img"}, job, site, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if c.Name == normal.Name || c.Name != CancelName(job.UID, 1, 2) || CancelName(job.UID, 1, 2) == CancelName(job.UID, 1, 3) {
		t.Fatalf("names: %s %s", c.Name, normal.Name)
	}
	if c.Labels[LabelCancelOnly] != "true" || c.Labels[LabelJobUID] != string(job.UID) || c.Labels[LabelRunnerJob] != c.Name ||
		c.Spec.Template.Labels[LabelRunnerJob] != c.Name {
		t.Fatalf("labels: %v", c.Labels)
	}
	if got := c.Spec.Template.Spec.Containers[0].Args; len(got) != 1 || got[0] != "--cancel-only" {
		t.Fatalf("args: %v", got)
	}
	if *c.Spec.ActiveDeadlineSeconds != 3600 || *c.Spec.BackoffLimit != 2 {
		t.Fatalf("bounds: %d %d", *c.Spec.ActiveDeadlineSeconds, *c.Spec.BackoffLimit)
	}
	if normal.Spec.Template.Spec.Containers[0].Args != nil || normal.Labels[LabelCancelOnly] != "" {
		t.Fatal("the attempt's own runner must be untouched")
	}
}

func TestEnvCarriesTheSitesSubmitCapOnlyWhenKnown(t *testing.T) {
	job, site := fixtures()
	env, err := Env(job, site, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range env {
		if e.Name == "HPC_MAX_SUBMIT_JOBS" {
			t.Fatalf("no cap known, yet %s=%s", e.Name, e.Value)
		}
	}
	site.Spec.Profile.MaxSubmitJobs = 5
	env, _ = Env(job, site, 1)
	for _, e := range env {
		if e.Name == "HPC_MAX_SUBMIT_JOBS" && e.Value == "5" {
			return
		}
	}
	t.Fatal("HPC_MAX_SUBMIT_JOBS=5 missing")
}

// A job stored before the run's data was pinned (or created by an older follower) names its dataset
// without a commit. It is refused before it is admitted, and an attempt of one that was already running
// tells its runner so: it never gets an environment that looks like "no dataset".
func TestAJobThatNamesADatasetWithoutACommitNeverLooksDataless(t *testing.T) {
	job, site := fixtures()
	job.Spec.Dataset, job.Spec.DatasetRef = nil, "dataset-a@latest"
	if err := ValidateDataset(&job.Spec); err == nil || !strings.Contains(err.Error(), "without a pinned commit") {
		t.Errorf("ValidateDataset = %v", err)
	}
	env, err := Env(job, site, 2)
	if err != nil {
		t.Fatal(err)
	}
	got := envOf(t, env)
	if got["DATASET_UNPINNED"] != "dataset-a@latest" || got["DATASET_REPO"] != "" {
		t.Errorf("env = DATASET_UNPINNED=%q DATASET_REPO=%q", got["DATASET_UNPINNED"], got["DATASET_REPO"])
	}
	job.Spec.DatasetRef = ""
	if err := ValidateDataset(&job.Spec); err != nil {
		t.Errorf("a job without a dataset is fine: %v", err)
	}
	pinned, _ := fixtures()
	if err := ValidateDataset(&pinned.Spec); err != nil {
		t.Errorf("a pinned dataset is fine: %v", err)
	}
}
