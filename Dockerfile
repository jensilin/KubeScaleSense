# syntax=docker/dockerfile:1

# --- build stage ----------------------------------------------------------
FROM golang:1.27-alpine AS build

ARG VERSION=dev
ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /src

# Copy the module files first so that dependency download is cached
# independently of source edits.
COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .

# CGO is off and the binary is fully static, which is what allows the
# distroless static runtime below.
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/kubescalesense ./cmd/kubescalesense

# --- runtime stage --------------------------------------------------------
# distroless static: no shell, no package manager, no libc. The controller
# needs none of them, and their absence removes most of the image's attack
# surface along with every CVE that would otherwise be reported against
# packages this binary never calls.
FROM gcr.io/distroless/static:nonroot

# 65532 is distroless' "nonroot" user. Set numerically so that the manifest's
# runAsNonRoot check can be satisfied without resolving a name.
USER 65532:65532

WORKDIR /

COPY --from=build /out/kubescalesense /usr/local/bin/kubescalesense

# The config file is mounted from a ConfigMap at /etc/kubescalesense in the
# deployment manifests; it is deliberately not baked into the image, so that a
# configuration change does not require a rebuild.
ENTRYPOINT ["/usr/local/bin/kubescalesense"]
CMD ["-config", "/etc/kubescalesense/kubescalesense.yaml"]
