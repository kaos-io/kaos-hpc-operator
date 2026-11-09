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
	"os"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/kaos-io/kaos-hpc-operator/test/utils"
)

// The e2e suite proves PRD-HPC-1461 AC-41: the controller installs with its Helm chart in a plain
// Kind cluster (no KAOS CRDs, no Crossplane, no Argo, no cert-manager) and runs a job against a
// fake Slurm site. Run it with `make test-e2e`.
var (
	// projectImage is the controller image (also the runner image: one image, one version) with the
	// test CA added to its trust store, so the controller and the runner pods trust the fake site.
	projectImage = "example.com/kaos-hpc-operator:e2e"
	// baseImage is the unmodified image `make docker-build` produces.
	baseImage = "example.com/kaos-hpc-operator-base:e2e"
	// fakeSlurmImage is the fake Slurm site (test/fakeslurmd).
	fakeSlurmImage = "example.com/kaos-hpc-fakeslurmd:e2e"

	// pki is the CA and the fake site's serving certificate, made once per run.
	pki *testPKI
)

// TestE2E runs the end-to-end (e2e) test suite for the project. It needs Kind, Docker, kubectl and
// helm; it builds the images and loads them into the Kind cluster.
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting kaos-hpc-operator standalone e2e suite\n")
	RunSpecs(t, "e2e suite")
}

var _ = BeforeSuite(func() {
	dir, err := os.MkdirTemp("", "hpc-e2e-pki-")
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(os.RemoveAll, dir)

	By("making the test CA and the fake site's certificate")
	pki, err = newTestPKI(fakeSlurmHost)
	Expect(err).NotTo(HaveOccurred())
	Expect(pki.write(dir)).To(Succeed())

	By("building the controller image")
	_, err = utils.Run(execCommand("make", "docker-build", "IMG="+baseImage))
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the controller image")

	By("adding the test CA to the controller image's trust store")
	dockerfile := fmt.Sprintf("FROM %s\nUSER 0\nCOPY ca.crt /tmp/ca.crt\n"+
		"RUN echo >> /etc/ssl/certs/ca-certificates.crt && cat /tmp/ca.crt >> /etc/ssl/certs/ca-certificates.crt"+
		" && rm /tmp/ca.crt\nUSER 65532:65532\n", baseImage)
	Expect(os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(dockerfile), 0o600)).To(Succeed())
	_, err = utils.Run(execCommand("docker", "build", "-t", projectImage, dir))
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the trusted controller image")

	By("building the fake Slurm image")
	_, err = utils.Run(execCommand("docker", "build", "-t", fakeSlurmImage, "-f", "test/fakeslurmd/Dockerfile", "."))
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the fake Slurm image")

	By("loading both images on Kind")
	for _, img := range []string{projectImage, fakeSlurmImage} {
		ExpectWithOffset(1, utils.LoadImageToKindClusterWithName(img)).To(Succeed(), "Failed to load %s into Kind", img)
	}
})
