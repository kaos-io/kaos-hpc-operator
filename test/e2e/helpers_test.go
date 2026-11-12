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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// execCommand is exec.Command; it exists so the suite reads the same everywhere.
func execCommand(name string, args ...string) *exec.Cmd { return exec.Command(name, args...) }

// kubectlApply applies a manifest given as text.
func kubectlApply(manifest string) (string, error) {
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// testPKI is a throwaway CA and one serving certificate signed by it.
type testPKI struct {
	caPEM, certPEM, keyPEM []byte
}

func newTestPKI(host string) (*testPKI, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "hpc-e2e-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: host},
		DNSNames:  []string{host},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	enc := func(typ string, b []byte) []byte { return pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: b}) }
	return &testPKI{caPEM: enc("CERTIFICATE", caDER), certPEM: enc("CERTIFICATE", der),
		keyPEM: enc("EC PRIVATE KEY", keyDER)}, nil
}

// write puts ca.crt, tls.crt and tls.key in dir.
func (p *testPKI) write(dir string) error {
	for name, b := range map[string][]byte{"ca.crt": p.caPEM, "tls.crt": p.certPEM, "tls.key": p.keyPEM} {
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o600); err != nil {
			return err
		}
	}
	return nil
}

// loadTestPKI reads the files write produced (or an equivalent made by openssl) from dir.
func loadTestPKI(dir string) (*testPKI, error) {
	read := func(name string) ([]byte, error) { return os.ReadFile(filepath.Join(dir, name)) }
	ca, err := read("ca.crt")
	if err != nil {
		return nil, err
	}
	cert, err := read("tls.crt")
	if err != nil {
		return nil, err
	}
	key, err := read("tls.key")
	if err != nil {
		return nil, err
	}
	return &testPKI{caPEM: ca, certPEM: cert, keyPEM: key}, nil
}

// machineKeyJSON is a machine key as the identity provider issues it (user, key id, RSA private key): what the
// runner signs its token request with. The fake site's identity provider accepts any.
func machineKeyJSON() (string, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", err
	}
	pemKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	out, err := json.Marshal(map[string]string{"userId": "e2e-machine", "keyId": "e2e-key", "key": string(pemKey)})
	return string(out), err
}
