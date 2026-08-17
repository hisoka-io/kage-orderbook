import { createInterface } from "node:readline";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { loadDarkpoolRuntime } from "./darkpool.js";
import { createIntentProofV1 } from "./intent-proof-v1.js";
import {
  intentOptionsFromRequest,
  parseProofRequestV1,
  proofFailure,
  proofSuccess,
  type ProofResponseV1,
} from "./protocol.js";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");

async function main(): Promise<void> {
  routeLibraryLogsToStderr();
  const darkpoolRoot = resolve(
    process.env.DARKPOOL_ROOT ?? resolve(repoRoot, "../darkpool"),
  );
  const runtime = loadDarkpoolRuntime(darkpoolRoot);
  const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
  console.error("[prover-worker] ready protocol=v1");

  try {
    for await (const line of lines) {
      if (line.trim().length === 0) {
        continue;
      }

      let requestId: string | null = null;
      let response: ProofResponseV1;
      try {
        const request = parseProofRequestV1(line);
        requestId = request.request_id;
        console.error(
          `[prover-worker] proving request=${requestId} order=${request.order.order_id}`,
        );
        const generated = await runtime.prove(
          intentOptionsFromRequest(request),
        );
        const proof = createIntentProofV1(generated, runtime.pins);
        response = proofSuccess(requestId, proof);
        console.error(
          `[prover-worker] proved request=${requestId} fields=${proof.proofAsFields.length}`,
        );
      } catch (error) {
        response = proofFailure(requestId, error);
        console.error(
          `[prover-worker] rejected request=${requestId ?? "unknown"} code=${response.error.code}`,
        );
      }
      await writeResponse(response);
    }
  } finally {
    await runtime.close();
  }
}

async function writeResponse(response: ProofResponseV1): Promise<void> {
  const line = `${JSON.stringify(response)}\n`;
  if (!process.stdout.write(line)) {
    await new Promise<void>((resolveWrite) =>
      process.stdout.once("drain", resolveWrite),
    );
  }
}

function routeLibraryLogsToStderr(): void {
  console.log = (...values: unknown[]) => console.error(...values);
  console.info = (...values: unknown[]) => console.error(...values);
  console.debug = (...values: unknown[]) => console.error(...values);
}

main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`[prover-worker] fatal: ${message}`);
  process.exitCode = 1;
});
