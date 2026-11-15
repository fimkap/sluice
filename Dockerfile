# syntax=docker/dockerfile:1
FROM golang:1.27.4-trixie AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    for cmd in sluice sluicectl sluice-bench; do \
      CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/$cmd ./cmd/$cmd || exit 1; \
    done && mkdir -p /out/data

# Static binaries on distroless: no shell, no package manager, runs as uid 65532.
FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/sluice /out/sluicectl /out/sluice-bench /
COPY --from=build --chown=65532:65532 /out/data /data
USER nonroot:nonroot
VOLUME /data
EXPOSE 9092 9100
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=5 CMD ["/sluice", "healthcheck"]
ENTRYPOINT ["/sluice"]
CMD ["--data-dir=/data"]
