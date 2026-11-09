// Command fakeslurmd serves the fake Slurm site of test/scale/fakeslurm (slurmrestd + slurmdbd over
// TLS) as an ordinary process, so the standalone Kind e2e (test/e2e) can run it as a Deployment.
// Jobs are simulated, not executed: each ends COMPLETED after a short run (time dilation 60, so a
// "1 to 2 minute" job takes 1 to 2 real seconds). It is test infrastructure and never ships.
package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/kaos-io/kaos-hpc-operator/test/scale/fakeslurm"
)

func main() {
	listen := flag.String("listen", ":8443", "address to serve TLS on")
	cert := flag.String("tls-cert", "/etc/fakeslurm/tls.crt", "server certificate (PEM)")
	key := flag.String("tls-key", "/etc/fakeslurm/tls.key", "server private key (PEM)")
	token := flag.String("token", os.Getenv("FAKE_SLURM_TOKEN"), "X-SLURM-USER-TOKEN every call must carry")
	flag.Parse()

	site := fakeslurm.New(fakeslurm.Options{
		Name: "e2e", Token: *token,
		RunMin: 1, RunMax: 2, // dilated minutes: 1 to 2 real seconds
		Capacity: 10,
		QOS:      []fakeslurm.QOS{{Name: "default", MaxWallMinutes: 120}},
	})
	// New starts an httptest listener on a random port; only its handler is wanted here.
	handler := site.Srv.Config.Handler
	site.Srv.Close()

	srv := &http.Server{Addr: *listen, Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	log.Printf("fakeslurmd: serving on %s", *listen)
	log.Fatal(srv.ListenAndServeTLS(*cert, *key))
}
