# One image, one version (PRD-HPC-1461 D-58, D-63): the controller (/manager), the pipeline-side
# follower (/follower) and the runner program (/runner/) that the controller's runner Jobs run.
# The entrypoint is chosen by `command`: /manager, /follower, or python3 /runner/main.py.

# Build the Go binaries (static: the follower runs in pipeline pods of any image base).
FROM golang:1.26.10 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source
COPY cmd/ cmd/
COPY api/ api/
COPY internal/ internal/

# Build
# the GOARCH has not a default value to allow the binary be built according to the host where the command
# was called. For example, if we call make docker-build in a local env which has the Apple Silicon M1 SO
# the docker BUILDPLATFORM arg will be linux/arm64 when for Apple x86 it will be linux/amd64. Therefore,
# by leaving it empty we can ensure that the container and binary shipped on it will have the same platform.
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -o manager cmd/main.go && \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -o follower ./cmd/follower

# Final stage: a pinned python alpine (Python 3 stdlib is all the runner needs) with the ssh client
# the runner uses to mint site tokens. Pinned by digest so the CVE scan sees the same bytes every
# build; bump the digest (and re-scan) to take a base-image fix.
FROM python:3.13-alpine@sha256:4d511039677430640754cd1417737349067dfcdaba4dcf7e576334516f9f4c30
# pip and setuptools are removed: the runner is stdlib-only and their vendored libraries (urllib3,
# msgpack) are the base image's only HIGH findings.
RUN apk upgrade --no-cache && apk add --no-cache openssh-client ca-certificates \
    && python3 -m pip uninstall -y pip setuptools wheel \
    && rm -rf /usr/local/lib/python3.13/site-packages/pip* /usr/local/lib/python3.13/site-packages/setuptools* \
              /usr/local/lib/python3.13/site-packages/pkg_resources /usr/local/lib/python3.13/site-packages/_distutils_hack \
              /usr/local/bin/pip* /usr/local/bin/wheel \
    && addgroup -g 65532 -S kaos && adduser -u 65532 -S -G kaos -h /tmp kaos
# Private CAs for a site whose scheduler API certificate no public CA signed (certs/README.md).
COPY certs/ /usr/local/share/ca-certificates/
RUN update-ca-certificates
WORKDIR /
COPY --chmod=0555 --from=builder /workspace/manager /manager
COPY --chmod=0555 --from=builder /workspace/follower /follower
COPY --chmod=0555 runner/ /runner/
USER 65532:65532
ENV HOME=/tmp PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

ENTRYPOINT ["/manager"]
