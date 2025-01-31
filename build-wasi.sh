#!/bin/sh

export RUSTFLAGS="-Ctarget-feature=+simd128"

cargo \
	build \
	--target wasm32-wasip1 \
	--profile release-wasi \
	--no-default-features \
	--features enable_snappy
