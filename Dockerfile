# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.26.3-alpine AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go build -trimpath -ldflags="-s -w -X main.version=${VERSION} -X main.imageControlPlaneOnly=true" -o /out/factory ./cmd/factory

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/factory /factory
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/factory", "server", "--config", "/etc/factory/server.json"]
