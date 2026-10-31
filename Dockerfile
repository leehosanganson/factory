# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.26.3-alpine AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/factory ./cmd/factory

FROM gcr.io/distroless/static-debian13:nonroot AS controller
COPY --from=build /out/factory /factory
ENV FACTORY_IMAGE_ROLE=controller
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/factory", "server", "--config", "/etc/factory/server.json"]

FROM alpine:3.22 AS worker
RUN apk add --no-cache git && addgroup -S -g 65532 factory && adduser -S -D -H -u 65532 -G factory factory
COPY --from=build /out/factory /factory
ENV FACTORY_IMAGE_ROLE=worker
USER 65532:65532
ENTRYPOINT ["/factory", "worker", "--config", "/etc/factory/worker.json"]

FROM controller AS default
