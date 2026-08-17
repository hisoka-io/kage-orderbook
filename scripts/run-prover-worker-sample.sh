#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
tools_root="$repo_root/tools"
darkpool_root="${DARKPOOL_ROOT:-$repo_root/../darkpool}"
request="$tools_root/mock-kage-user/fixtures/proof-request-v1.jsonl"
response="$repo_root/artifacts/proof-response-v1.json"

if [[ ! -d "$tools_root/node_modules/tsx" ]]; then
  echo "Mock-user TypeScript dependencies are missing" >&2
  echo "Run: pnpm --dir $tools_root install" >&2
  exit 1
fi
if [[ ! -f "$darkpool_root/packages/prover/dist/index.cjs" ]]; then
  echo "Darkpool prover is not built" >&2
  echo "Run: pnpm --dir $darkpool_root --filter @hisoka/prover build" >&2
  exit 1
fi

mkdir -p "$repo_root/artifacts"
export DARKPOOL_ROOT="$darkpool_root"
cd "$tools_root"
node --import tsx mock-kage-user/src/worker.ts < "$request" > "$response"
node -e '
const response = require(process.argv[1]);
if (!response.ok) throw new Error(`${response.error.code}: ${response.error.message}`);
console.log(`verified worker proof: fields=${response.proof.proofAsFields.length} public_inputs=${response.proof.publicInputs.length}`);
' "$response"
