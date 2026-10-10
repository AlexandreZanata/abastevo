# anpfuel backend images.
#
# One pinned builder compiles all four process roles (api, worker, migrate, station-load)
# from the same base layers; each target ships only its static binary on a
# non-root distroless runtime with no shell. Same revision label across roles
# is the release provenance. Secrets travel at runtime (env), never in layers.
#
# Build from the repository root, e.g.:
#   docker build -f infra/docker/Dockerfile --target api \
#     --build-arg REVISION=$(git rev-parse --short HEAD) .
ARG GOLANG_IMAGE=golang:1.27.2-bookworm@sha256:5cf287a799e6b94384bad13d16b14904c531f51ba65792237e122ce42b392f61
ARG RUNTIME_IMAGE=gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
ARG REVISION=dev

FROM ${GOLANG_IMAGE} AS builder
ENV CGO_ENABLED=0 GOOS=linux GOTOOLCHAIN=local
WORKDIR /src
COPY backend/go.mod backend/go.sum ./
RUN go mod download
COPY backend/cmd ./cmd
COPY backend/internal ./internal
COPY backend/db ./db
RUN go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api \
 && go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker \
 && go build -trimpath -ldflags="-s -w" -o /out/migrate ./cmd/migrate \
 && go build -trimpath -ldflags="-s -w" -o /out/station-load ./cmd/station-load

FROM ${RUNTIME_IMAGE} AS api
ARG REVISION
LABEL org.opencontainers.image.title="anpfuel-api" \
      org.opencontainers.image.revision="${REVISION}"
COPY --from=builder --chown=65532:65532 /out/api /api
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/api"]

FROM ${RUNTIME_IMAGE} AS worker
ARG REVISION
LABEL org.opencontainers.image.title="anpfuel-worker" \
      org.opencontainers.image.revision="${REVISION}"
COPY --from=builder --chown=65532:65532 /out/worker /worker
USER 65532:65532
ENTRYPOINT ["/worker"]

FROM ${RUNTIME_IMAGE} AS migrate
ARG REVISION
LABEL org.opencontainers.image.title="anpfuel-migrate" \
      org.opencontainers.image.revision="${REVISION}"
COPY --from=builder --chown=65532:65532 /out/migrate /migrate
USER 65532:65532
ENTRYPOINT ["/migrate"]

FROM ${RUNTIME_IMAGE} AS station-load
ARG REVISION
LABEL org.opencontainers.image.title="anpfuel-station-load" \
      org.opencontainers.image.revision="${REVISION}"
COPY --from=builder --chown=65532:65532 /out/station-load /station-load
USER 65532:65532
ENTRYPOINT ["/station-load"]
