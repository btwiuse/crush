#!/usr/bin/env bash
set -euo pipefail

HACK=/tmp/hackpad
GOGO=/tmp/go-toolchain

if ! [[ -d "$HACK" ]]; then
  mkdir -p "$HACK"
  git clone --depth=1 https://github.com/btwiuse/hackpad "$HACK"
fi

if ! [[ -d "$GOGO/go" ]]; then
  mkdir -p "$GOGO/go"
  TAG=go1.26.3-hackpad.4
  curl -sL "https://github.com/btwiuse/go/releases/download/${TAG}/${TAG}.linux-amd64.tar.gz" | tar -xzC "$GOGO"
fi

export PATH="$GOGO/go/bin:$PATH"

which go

mkdir -p internal2

cp -rv $HACK/internal/{log,process,global,js,interop,fs,common,promise,fsutil} internal2

grep -rl 'github.com/hack-pad/hackpad/internal' internal2/ \
  | xargs perl -pi -e \
's,github.com/hack-pad/hackpad/internal,github.com/charmbracelet/crush/internal2,g'

go mod tidy

go run github.com/btwiuse/boba/cmd/boba-wasm-build -o ./build/crush.wasm .

cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" ./assets

bun build --compile --outfile=./build/index.html --target=browser ./assets/index.html

# RELAY=:8841 ufo pub ./build
