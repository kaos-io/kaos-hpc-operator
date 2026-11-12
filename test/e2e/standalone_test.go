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

		By("giving the runner a machine key to mint its data-plane token with")
		key, err := machineKeyJSON()
		Expect(err).NotTo(HaveOccurred())
		_, err = utils.Run(execCommand("kubectl", "create", "secret", "generic", "mlflow-svc", "-n", e2eHPCNS,
			"--from-literal=ZITADEL_MACHINE_KEY="+key))
		Expect(err).NotTo(HaveOccurred())

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
