# One Dockerfile builds every binary under cmd/. Pick one with --build-arg CMD=<name>,
# e.g. evaluator, pod, buyers, simulator.

# Must match the go directive in go.mod. Alpine, so the build and the final
# image share a base and the binary is built with CGO disabled.
ARG GO_VERSION=1.27.3

FROM golang:${GO_VERSION}-alpine AS build
ARG CMD
WORKDIR /src
# Dependencies first, so source edits do not invalidate the module cache layer.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN test -n "$CMD" || (echo "build-arg CMD is required" && exit 1)
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

# alpine rather than scratch: it has busybox wget for the compose healthchecks
# and a shell for debugging, at about 7 MB.
FROM alpine:3.20
RUN adduser -D -u 10001 app
USER app
COPY --from=build /out/app /usr/local/bin/app
ENTRYPOINT ["/usr/local/bin/app"]
