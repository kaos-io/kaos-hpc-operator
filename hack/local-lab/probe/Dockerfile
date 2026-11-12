# The probe step image of the local lab: prints the step's context, lists input folders with
# sha256 checksums, writes a result file, and talks to the lake over its HTTP API.
FROM alpine:3.20
RUN apk add --no-cache curl jq coreutils
COPY probe lake /usr/local/bin/
RUN chmod +x /usr/local/bin/probe /usr/local/bin/lake
