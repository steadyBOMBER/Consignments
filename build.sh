#!/bin/bash
# Install Rust toolchain if needed
if ! command -v rustc &> /dev/null; then
    echo "Installing Rust toolchain..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source /tmp/rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin/activate
fi
# Ensure Cargo cache is writable
mkdir -p /tmp/cargo
export CARGO_HOME=/tmp/cargo
# Install Python dependencies
pip install -r requirements.txt
