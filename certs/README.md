# certs

Optional private CA certificates (PEM files named `*.crt`) for the image. Everything in this
directory is added to the image's trust store at build time (`update-ca-certificates`), for the
controller and for the runner Jobs, which run the same image. Use it when a site's scheduler API
is served with a certificate that a public CA did not sign. Empty (this file only) by default.

The standalone CI e2e (a CA generated per run, `fakeslurmd` as the site) builds the image with its
CA dropped here.
