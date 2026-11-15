#!/usr/bin/env bash
set -euo pipefail
export GOTOOLCHAIN=local GOWORK=off GOFLAGS=-mod=readonly GOTELEMETRY=off
: "${RUNNER_TEMP:?IU checks require an explicit disposable output directory}"
baseline=a05e4d24bc9ffef54244dc5424b32d090f87e849
test "$(go env GOVERSION)" = go1.26.5
python3 -B -m unittest discover -s scripts/ci -p 'test_iu_*.py' -v
python3 scripts/ci/iu-source-boundary.py
python3 scripts/ci/iu-module-pin.py "$baseline" --download
python3 scripts/ci/iu-secret-scan.py
mapfile -t files < <(git diff --name-only --diff-filter=AM "$baseline" HEAD -- '*.go')
test "${#files[@]}" -gt 0
test -z "$(gofmt -l "${files[@]}")"
for mode in native cgo-free race; do
  flags=()
  if test "$mode" = race; then flags=(-race); fi
  if test "$mode" = cgo-free; then export CGO_ENABLED=0; else export CGO_ENABLED=1; fi
  go test -p 1 -count=1 -timeout=10m "${flags[@]}" -json ./internal/audioforward -run '^TestIUWS3' | tee "$RUNNER_TEMP/iu-$mode.json"
  python3 scripts/ci/iu-json-events.py scripts/ci/iu-required-tests.txt "$RUNNER_TEMP/iu-$mode.json"
done
go vet -p 1 ./internal/audioforward
mkdir -p "$RUNNER_TEMP/iu-cross"
for arch in amd64 arm64; do
  CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -p 1 -trimpath ./internal/audioforward
  output="$RUNNER_TEMP/iu-cross/audioforward-linux-$arch.test"
  CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go test -p 1 -trimpath -c ./internal/audioforward -o "$output"
  go version -m "$output"
  sha256sum "$output"
done
# Scope: CGO-free audioforward, not the DAVE/Opus runtime. The unchanged DAVE job
# retains full native tests/build and its existing gateway/voice/Race checks.
# Cross-built test binaries are never presented as native executions.
git diff --exit-code -- go.mod go.sum
python3 scripts/ci/iu-source-boundary.py
