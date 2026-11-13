upstream: github.com/skrashevich/quasar-bridge
base commit: dcb362b4f87235486bc1eb1b780593ad2e16d85f
local change: llm_timeout_ms config (see ../quasar-bridge-llm-timeout.patch)
build: docker run --rm -v $PWD:/src -w /src -e GOOS=linux -e GOARCH=arm64 -e CGO_ENABLED=0 golang:1 go build -o quasar-bridge-arm64 ./cmd/quasar-bridge
