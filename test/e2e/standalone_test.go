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

package e2e

import (
	"encoding/json"
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/kaos-io/kaos-hpc-operator/test/utils"
)

// Names of the fixtures of the standalone run. The environment namespace (e2eEnvNS) holds the job;
// the HPC namespace (e2eHPCNS) holds the site, the runner's service account, Secrets and ConfigMap
// that kubecore-operator renders in a KAOS install. Here the test renders them: nothing in the
// controller or the chart needs KAOS.
const (
	fakeSlurmNS   = "fake-slurm"
	fakeSlurmHost = "fake-slurm." + fakeSlurmNS + ".svc"
	fakeSlurmTok  = "e2e-token"
	e2eEnvNS      = "e2e-train"
	e2eHPCNS      = "e2e-ml"
	e2eJobName    = "e2e-job"
	e2eApp        = "e2e-app"
	// e2eStrangerJob is a job of an app the platform never deployed to the project: no service account, no grant.
	e2eStrangerJob = "e2e-stranger-job"
	e2eStrangerApp = "e2e-stranger"
	e2eStrangerWF  = "e2e-stranger-wf-uid"
)

// fakeSlurmManifest runs the fake Slurm site (test/fakeslurmd) behind a Service.
const fakeSlurmManifest = `
apiVersion: v1
kind: Secret
metadata: {name: fake-slurm-token, namespace: ` + fakeSlurmNS + `}
stringData: {token: ` + fakeSlurmTok + `}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: fake-slurm, namespace: ` + fakeSlurmNS + `}
spec:
  replicas: 1
  selector: {matchLabels: {app: fake-slurm}}
  template:
    metadata: {labels: {app: fake-slurm}}
    spec:
      containers:
      - name: fakeslurmd
        image: ` + "%s" + `
        env:
        - name: FAKE_SLURM_TOKEN
          valueFrom: {secretKeyRef: {name: fake-slurm-token, key: token}}
        ports: [{containerPort: 8443}]
        readinessProbe: {tcpSocket: {port: 8443}}
        volumeMounts: [{name: tls, mountPath: /etc/fakeslurm, readOnly: true}]
      volumes:
      - name: tls
        secret: {secretName: fake-slurm-tls}
---
apiVersion: v1
kind: Service
metadata: {name: fake-slurm, namespace: ` + fakeSlurmNS + `}
spec:
  selector: {app: fake-slurm}
  ports: [{port: 443, targetPort: 8443}]
`

// projectManifest is what a KAOS install renders for one project: two labelled namespaces, the
// runner's service account, ConfigMap and Secrets, its RoleBinding, and the HPCSite.
const projectManifest = `
apiVersion: v1
kind: Namespace
metadata: {name: ` + e2eEnvNS + `, labels: {hpc.kaos.io/project: e2e}}
---
apiVersion: v1
kind: Namespace
metadata: {name: ` + e2eHPCNS + `, labels: {hpc.kaos.io/project: e2e}}
---
apiVersion: v1
kind: ServiceAccount
metadata: {name: hpc-runner, namespace: ` + e2eHPCNS + `}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: hpc-runner-config, namespace: ` + e2eHPCNS + `}
data:
  KAOS_E2E: standalone
  # The data plane the step's result is handed back through: the fake site stands in for the identity
  # provider and for lakeFS too (test/scale/fakeslurm/dataplane.go).
  ZITADEL_DOMAIN: ` + fakeSlurmHost + `
  LAKEFS_EXTERNAL_URL: https://` + fakeSlurmHost + `/lakefs
---
apiVersion: v1
kind: Secret
metadata: {name: meluxina-jwt, namespace: ` + e2eHPCNS + `}
stringData: {token: ` + fakeSlurmTok + `}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: hpc-runner, namespace: ` + e2eEnvNS + `}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: hpcargoworkflowjob-runner-role}
subjects: [{kind: ServiceAccount, name: hpc-runner, namespace: ` + e2eHPCNS + `}]
---
apiVersion: hpc.kaos.io/v1alpha1
kind: HPCSite
metadata: {name: site, namespace: ` + e2eHPCNS + `, labels: {hpc.kaos.io/project: e2e}}
spec:
  site: e2e
  profile:
    transport: slurmrestd
    endpoint: https://` + fakeSlurmHost + `
    restVersion: v0.0.44
    scratchRoot: /scratch/e2e
    classes:
    - {name: hpc-cpu, partition: cpu, qos: default, tier: cpu, maxWallTimeMinutes: 120}
    qos: [{name: default, maxWallTimeMinutes: 120}]
  identity:
    user: u100001
    account: p200001
    sshKeySecretRef: {name: none, key: none}
  budget: {source: share, cpuNodeHours: 10}
  limits: {maxConcurrentJobs: 2}
`

// appGrantManifest is what the platform renders for a deployed app (the app's composition): its workflow service account
// in the environment namespace, and the Role and RoleBinding that let the runner's service account (in the site's
// namespace) create a token for that one service account and no other. The runner exchanges that token at the data store.
const appGrantManifest = `
apiVersion: v1
kind: ServiceAccount
metadata: {name: ` + e2eApp + `-workflow, namespace: ` + e2eEnvNS + `}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: ` + e2eApp + `-workflow-hpc-token, namespace: ` + e2eEnvNS + `}
rules:
- apiGroups: [""]
  resources: [serviceaccounts/token]
  resourceNames: [` + e2eApp + `-workflow]
  verbs: [create]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: ` + e2eApp + `-workflow-hpc-token, namespace: ` + e2eEnvNS + `}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: ` + e2eApp + `-workflow-hpc-token}
subjects: [{kind: ServiceAccount, name: hpc-runner, namespace: ` + e2eHPCNS + `}]
`

// jobManifest is one served job: spec.stepName makes the controller run it through a runner Job.
const jobManifest = `
apiVersion: hpc.kaos.io/v1alpha1
kind: HPCArgoWorkflowJob
metadata: {name: ` + e2eJobName + `, namespace: ` + e2eEnvNS + `}
spec:
  class: hpc-cpu
  image: registry.invalid/app:v1
  command: [python, -m, app.entry]
  wallTimeMinutes: 15
  stepName: e2e-step
  # The step contract: the app and the step context. No input: the fake site serves no dataset.
  app: e2e-app
  context: '` + e2eContext + `'
  # The runner needs the id of the run it belongs to. No Argo in this cluster: the controller reads
  # the Workflow only where its API exists, so any name and uid do.
  workflow: {name: e2e-wf, uid: e2e-wf-uid}
`

// strangerJobManifest is a served job of an app that has no grant: the platform never deployed it to the project.
const strangerJobManifest = `
apiVersion: hpc.kaos.io/v1alpha1
kind: HPCArgoWorkflowJob
metadata: {name: ` + e2eStrangerJob + `, namespace: ` + e2eEnvNS + `}
spec:
  class: hpc-cpu
  image: registry.invalid/app:v1
  command: [python, -m, app.entry]
  wallTimeMinutes: 15
  stepName: e2e-step
  app: ` + e2eStrangerApp + `
  context: '` + strangerContext + `'
  workflow: {name: e2e-stranger-wf, uid: ` + e2eStrangerWF + `}
`

const strangerContext = `{"version":1,"run":"e2e-stranger-wf","app":"e2e-stranger","project":"e2e","step":"e2e-step","kind":"train",` +
	`"experiment":"e2e","params":{},"ledger":{"version":2,"reads":[],"writes":[]},"produces":[],"upstream":{}}`

// e2eContext is the step context of the standalone job (docs/step-contract.md, section 3).
const e2eContext = `{"version":1,"run":"e2e-wf","app":"e2e-app","project":"e2e","step":"e2e-step","kind":"train",` +
	`"experiment":"e2e","params":{},"ledger":{"version":2,"reads":[],"writes":[]},"produces":[],"upstream":{}}`

// kubectlGet returns a jsonpath of one object, "" when it is absent.
func kubectlGet(jsonpath string, args ...string) string {
	out, err := utils.Run(execCommand("kubectl", append(append([]string{"get"}, args...), "-o", "jsonpath="+jsonpath)...))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

// fakeSiteGet reads one of the fake site's inspection endpoints from inside the controller's container (same DNS and trust
// as the runner's own calls).
func fakeSiteGet(path string, out any) {
	probe := "import urllib.request as u;r=u.Request('https://" + fakeSlurmHost + path + "'," +
		"headers={'X-SLURM-USER-TOKEN':'" + fakeSlurmTok + "'});print(u.urlopen(r,timeout=10).read().decode())"
	body, err := utils.Run(execCommand("kubectl", "exec", "-n", namespace, "deploy/"+release+"-controller-manager", "--",
		"python3", "-c", probe))
	Expect(err).NotTo(HaveOccurred())
	Expect(json.Unmarshal([]byte(strings.TrimSpace(body)), out)).To(Succeed(), body)
}

// submission is what the fake site recorded of one accepted submission.
type submission struct {
	Name   string            `json:"name"`
	Env    map[string]string `json:"env"`
	Script string            `json:"script"`
}

// standaloneSpecs declares the AC-41 specs: no KAOS in the cluster, a job run to Succeeded.
func standaloneSpecs() {
	It("should run in a cluster that has no KAOS, Crossplane, Argo or cert-manager APIs", func() {
		out, err := utils.Run(execCommand("kubectl", "get", "crd", "-o", "jsonpath={.items[*].spec.group}"))
		Expect(err).NotTo(HaveOccurred())
		groups := strings.Fields(out)
		Expect(groups).To(ContainElement("hpc.kaos.io"), "the chart installs the controller's own CRDs")
		for _, g := range groups {
			for _, foreign := range []string{"crossplane.io", "kubecore.io", "argoproj.io", "cert-manager.io"} {
				Expect(g).NotTo(HaveSuffix(foreign), "CRD group %s is not standalone", g)
			}
		}
	})

	It("should run a job against the fake Slurm site and settle it Succeeded", func() {
		By("starting the fake Slurm site behind a TLS certificate the controller image trusts")
		_, err := utils.Run(execCommand("kubectl", "create", "namespace", fakeSlurmNS))
		Expect(err).NotTo(HaveOccurred())
		_, err = utils.Run(execCommand("kubectl", "create", "secret", "generic", "fake-slurm-tls", "-n", fakeSlurmNS,
			"--from-literal=tls.crt="+string(pki.certPEM), "--from-literal=tls.key="+string(pki.keyPEM)))
		Expect(err).NotTo(HaveOccurred())
		out, err := kubectlApply(strings.Replace(fakeSlurmManifest, "%s", fakeSlurmImage, 1))
		Expect(err).NotTo(HaveOccurred(), out)
		_, err = utils.Run(execCommand("kubectl", "rollout", "status", "deploy/fake-slurm", "-n", fakeSlurmNS,
			"--timeout=2m"))
		Expect(err).NotTo(HaveOccurred())

		By("creating the project's namespaces, runner identity and HPCSite")
		out, err = kubectlApply(projectManifest)
		Expect(err).NotTo(HaveOccurred(), out)

		By("giving the runner the project's HPC identity for the experiment tracker (Secret mlflow-hpc)")
		key, err := machineKeyJSON()
		Expect(err).NotTo(HaveOccurred())
		_, err = utils.Run(execCommand("kubectl", "create", "secret", "generic", "mlflow-hpc", "-n", e2eHPCNS,
			"--from-literal=ZITADEL_MACHINE_KEY="+key))
		Expect(err).NotTo(HaveOccurred())

		By("rendering what the platform renders for the deployed app: its workflow service account and the runner's grant to it")
		out, err = kubectlApply(appGrantManifest)
		Expect(err).NotTo(HaveOccurred(), out)

		By("creating the HPCArgoWorkflowJob")
		out, err = kubectlApply(jobManifest)
		Expect(err).NotTo(HaveOccurred(), out)

		By("waiting for the job to be Succeeded")
		describe := func() string {
			return fmt.Sprintf("phase=%q reason=%q siteJobID=%q",
				kubectlGet("{.status.phase}", "hpcargoworkflowjob", e2eJobName, "-n", e2eEnvNS),
				kubectlGet("{.status.reason}", "hpcargoworkflowjob", e2eJobName, "-n", e2eEnvNS),
				kubectlGet("{.status.run.siteJobID}", "hpcargoworkflowjob", e2eJobName, "-n", e2eEnvNS))
		}
		polls := 0
		Eventually(func() string {
			GinkgoWriter.Println(describe())
			if polls++; polls == 20 { // a minute in: the cause is still there to be read, whatever happens later
				GinkgoWriter.Println("diagnostics after 1 minute:" + diagnostics())
			}
			return kubectlGet("{.status.phase}", "hpcargoworkflowjob", e2eJobName, "-n", e2eEnvNS)
		}, "5m", "3s").Should(Equal("Succeeded"), func() string {
			return describe() + "\n" + diagnostics()
		})

		By("checking that the step's result was handed back: fetched, checked and written in wire form")
		result := kubectlGet("{.status.result}", "hpcargoworkflowjob", e2eJobName, "-n", e2eEnvNS)
		Expect(result).To(HavePrefix(`{"version":1,"status":"ok","values":{`), "status.result of a job that Succeeded")
		Expect(result).To(HaveSuffix(`"reads":[],"writes":[]}`))

		By("checking that the site ran it and the controller booked and settled it")
		Expect(kubectlGet("{.status.run.siteJobID}", "hpcargoworkflowjob", e2eJobName, "-n", e2eEnvNS)).NotTo(BeEmpty())
		booked := kubectlGet("{.status.bookedNodeMinutes}", "hpcsite", "site", "-n", e2eHPCNS)
		Expect(booked).To(SatisfyAny(BeEmpty(), Equal("0")),
			"nothing stays booked once the job has settled")

		By("checking that the data store was asked as the app: a token of its workflow service account, for kaos-lake")
		var exchanges []struct {
			Subject  string   `json:"subject"`
			Audience []string `json:"audience"`
			Granted  bool     `json:"granted"`
		}
		fakeSiteGet("/fake/v1/exchanges", &exchanges)
		Expect(exchanges).NotTo(BeEmpty(), "the runner exchanged a token for the app's key")
		for _, e := range exchanges {
			Expect(e.Subject).To(Equal("system:serviceaccount:" + e2eEnvNS + ":" + e2eApp + "-workflow"))
			Expect(e.Audience).To(ConsistOf("kaos-lake"))
			Expect(e.Granted).To(BeTrue())
		}

		By("checking that what reached the site holds the key file's place and no data-store credential")
		var subs []submission
		fakeSiteGet("/fake/v1/submissions", &subs)
		var sent *submission
		for i := range subs {
			if strings.Contains(subs[i].Env["WF_UID"], "e2e-wf-uid") {
				sent = &subs[i]
			}
		}
		Expect(sent).NotTo(BeNil(), "the job was submitted to the site")
		Expect(sent.Script).To(ContainSubstring("LAKEFS_KEY_FILE"), "the job script reads the app's key from a file")
		Expect(sent.Script).To(ContainSubstring("/kaos/key"))
		Expect(sent.Env).To(HaveKey("LAKEFS_ENDPOINT"))
		for k, v := range sent.Env {
			Expect(k).NotTo(MatchRegexp(`(?i)LAKEFS_(ACCESS|SECRET|TOKEN|KEY_ID|AUTH)`), "no data-store credential in the environment")
			Expect(v).NotTo(HavePrefix("eyJ"), "%s holds a bearer token", k)
		}
		Expect(sent.Env["LAKEFS_KEY_FILE"]).To(BeEmpty(), "the key file's path is set by the job script, never in the environment")
		for _, key := range []string{"fake-app-secret-key", "AKIAFAKEAPPKEY"} {
			Expect(sent.Script).NotTo(ContainSubstring(key), "the app's key is never in the job script")
			for k, v := range sent.Env {
				Expect(v).NotTo(ContainSubstring(key), "the app's key is in %s", k)
			}
		}
	})

	It("should refuse a job whose app has no identity, and send nothing to the site", func() {
		By("creating a job of an app that was never deployed to the project")
		out, err := kubectlApply(strangerJobManifest)
		Expect(err).NotTo(HaveOccurred(), out)

		By("waiting for it to end Failed, AppIdentityUnavailable")
		Eventually(func() string {
			return kubectlGet("{.status.phase}/{.status.reason}", "hpcargoworkflowjob", e2eStrangerJob, "-n", e2eEnvNS)
		}, "3m", "3s").Should(Equal("Failed/AppIdentityUnavailable"), func() string {
			return diagnostics()
		})
		msg := kubectlGet("{.status.message}", "hpcargoworkflowjob", e2eStrangerJob, "-n", e2eEnvNS)
		Expect(msg).To(ContainSubstring(e2eStrangerApp))
		Expect(msg).To(ContainSubstring("Nothing was staged and nothing was submitted to the site"))

		By("checking that nothing of it reached the site, and the data store was never asked for it")
		var subs []submission
		fakeSiteGet("/fake/v1/submissions", &subs)
		for _, s := range subs {
			Expect(s.Env["WF_UID"]).NotTo(ContainSubstring(e2eStrangerWF))
		}
		var exchanges []struct{ Subject string }
		fakeSiteGet("/fake/v1/exchanges", &exchanges)
		for _, e := range exchanges {
			Expect(e.Subject).NotTo(ContainSubstring(e2eStrangerApp))
		}
		Expect(kubectlGet("{.status.bookedNodeMinutes}", "hpcsite", "site", "-n", e2eHPCNS)).To(SatisfyAny(BeEmpty(), Equal("0")))
	})
}

// diagnostics collects what explains a job that did not finish: the job and site status, the runner and fake
// site logs, and a probe of the fake site from inside the controller's own container (same DNS, network, trust).
func diagnostics() string {
	probe := "import ssl,urllib.request as u;r=u.Request('https://" + fakeSlurmHost + "/slurm/v0.0.44/ping'," +
		"headers={'X-SLURM-USER-TOKEN':'" + fakeSlurmTok + "'});print(u.urlopen(r,timeout=10).read())"
	cmds := [][]string{
		{"get", "hpcargoworkflowjob", e2eJobName, "-n", e2eEnvNS, "-o", "yaml"},
		{"get", "hpcsite", "site", "-n", e2eHPCNS, "-o", "yaml"},
		{"logs", "-n", e2eHPCNS, "-l", "app.kubernetes.io/name=kaos-hpc-runner", "--tail=60"},
		{"logs", "-n", fakeSlurmNS, "deploy/fake-slurm", "--tail=30"},
		{"exec", "-n", namespace, "deploy/" + release + "-controller-manager", "--", "python3", "-c", probe},
	}
	var b strings.Builder
	for _, c := range cmds {
		out, _ := utils.Run(execCommand("kubectl", c...))
		b.WriteString("\n$ kubectl " + strings.Join(c[:3], " ") + "\n" + out)
	}
	return b.String()
}
