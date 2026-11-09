# Image of the fake Slurm site for the standalone Kind e2e (test/e2e). Build context: the repo root.
FROM golang:1.26.10 AS builder
WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download
COPY test/scale/fakeslurm/ test/scale/fakeslurm/
COPY test/fakeslurmd/ test/fakeslurmd/
RUN CGO_ENABLED=0 go build -o /fakeslurmd ./test/fakeslurmd

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /fakeslurmd /fakeslurmd
USER 65532:65532
ENTRYPOINT ["/fakeslurmd"]
