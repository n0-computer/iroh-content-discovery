#!/usr/bin/env bash
# Builds the Rust gateway library for the app's ABIs and copies it, stripped,
# to app/src/main/jniLibs. Needs cargo-ndk and ANDROID_NDK_HOME (or an NDK in
# $ANDROID_HOME/ndk).
set -euo pipefail
cd "$(dirname "$0")"

: "${ANDROID_HOME:=$HOME/Android/Sdk}"
if [[ -z "${ANDROID_NDK_HOME:-}" ]]; then
    ANDROID_NDK_HOME="$(ls -d "$ANDROID_HOME"/ndk/* | sort -V | tail -1)"
fi
export ANDROID_NDK_HOME

profile="${1:-release}"
target_dir="$(cargo metadata --format-version 1 --no-deps --manifest-path native/Cargo.toml |
    python3 -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])')"
strip="$(ls "$ANDROID_NDK_HOME"/toolchains/llvm/prebuilt/*/bin/llvm-strip | head -1)"

declare -A triples=([arm64-v8a]=aarch64-linux-android [x86_64]=x86_64-linux-android)
args=()
for abi in "${!triples[@]}"; do args+=(-t "$abi"); done
profile_flag=()
[[ "$profile" == release ]] && profile_flag=(--release)
cargo ndk "${args[@]}" --platform 26 build -p iroh-link-android "${profile_flag[@]}"

rm -rf app/src/main/jniLibs
for abi in "${!triples[@]}"; do
    mkdir -p "app/src/main/jniLibs/$abi"
    "$strip" --strip-unneeded -o "app/src/main/jniLibs/$abi/libiroh_link_android.so" \
        "$target_dir/${triples[$abi]}/$profile/libiroh_link_android.so"
done
ls -l app/src/main/jniLibs/*/
