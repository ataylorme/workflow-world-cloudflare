#!/usr/bin/env bash
set -euo pipefail

rm -rf dist

# Bundle each entry point with ncc (all dependencies inlined)
ncc build src/index.ts -o dist --no-source-map-register
ncc build src/durable-object.ts -o dist/durable-object --no-source-map-register
ncc build src/d1.ts -o dist/d1 --no-source-map-register
ncc build src/setup.ts -o dist/setup --no-source-map-register

# ncc outputs index.js in each dir — rename subpath entry points
mv dist/durable-object/index.js dist/durable-object.js
mv dist/d1/index.js dist/d1.js
mv dist/setup/index.js dist/setup.js
rm -rf dist/durable-object dist/d1 dist/setup

# Ensure CLI entry point is executable
chmod +x dist/setup.js

# Generate type declarations only (ncc bundles JS, tsc emits .d.ts)
tsc --emitDeclarationOnly --outDir dist

# Clean up ncc artifacts we don't need
rm -f dist/package.json
