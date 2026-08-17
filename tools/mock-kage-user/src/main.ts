import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { loadDarkpoolRuntime, type IntentOptions } from "./darkpool.js";
import { createIntentProofV1 } from "./intent-proof-v1.js";

interface TokenConfig {
  symbol: string;
  address: string;
}

interface OrderbookConfig {
  chains: Array<{
    tokens: TokenConfig[];
    markets: Array<{ token_in: string; token_out: string }>;
  }>;
}

interface CliOptions {
  output: string;
  intent: IntentOptions;
}

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");

async function main(): Promise<void> {
  const cli = await parseArgs(process.argv.slice(2));
  const darkpoolRoot = resolve(
    process.env.DARKPOOL_ROOT ?? resolve(repoRoot, "../darkpool"),
  );
  const runtime = loadDarkpoolRuntime(darkpoolRoot);

  try {
    console.error("[mock-kage-user] generating real swap_intent proof");
    const proof = await runtime.prove(cli.intent);
    const envelope = createIntentProofV1(proof, runtime.pins);
    await mkdir(dirname(cli.output), { recursive: true });
    await writeFile(cli.output, `${JSON.stringify(envelope, null, 2)}\n`);
    console.error(
      `[mock-kage-user] verified proof written to ${cli.output} ` +
        `(proof_fields=${envelope.proofAsFields.length} public_inputs=${envelope.publicInputs.length})`,
    );
  } finally {
    await runtime.close();
  }
}

async function parseArgs(args: string[]): Promise<CliOptions> {
  const market = await firstConfiguredMarket();
  const values = new Map<string, string>();

  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    if (argument === "--help" || argument === "-h") {
      printHelp();
      process.exit(0);
    }
    if (!argument.startsWith("--")) {
      throw new Error(`unexpected argument: ${argument}`);
    }
    const value = args[index + 1];
    if (!value || value.startsWith("--")) {
      throw new Error(`missing value for ${argument}`);
    }
    values.set(argument, value);
    index += 1;
  }

  const inputValue = bigintArg(values, "--input-value", 100n);
  const fromAmount = bigintArg(values, "--from-amount", 60n);
  return {
    output: resolve(
      values.get("--output") ??
        resolve(repoRoot, "artifacts/intent-proof-v1.json"),
    ),
    intent: {
      fromAsset: bigintArg(values, "--from-asset", market.fromAsset),
      toAsset: bigintArg(values, "--to-asset", market.toAsset),
      inputValue,
      fromAmount,
      receivedAmount: bigintArg(values, "--received-amount", 50n),
      expiry: bigintArg(
        values,
        "--expiry",
        BigInt(Math.floor(Date.now() / 1000) + 300),
      ),
    },
  };
}

async function firstConfiguredMarket(): Promise<{
  fromAsset: bigint;
  toAsset: bigint;
}> {
  const path = process.env.KAGE_ORDERBOOK_CONFIG
    ? resolve(process.env.KAGE_ORDERBOOK_CONFIG)
    : resolve(repoRoot, "config.json");
  const config = JSON.parse(await readFile(path, "utf8")) as OrderbookConfig;
  const chain = config.chains.at(0);
  const market = chain?.markets.at(0);
  if (!chain || !market) {
    throw new Error(`no market configured in ${path}`);
  }
  const tokenIn = chain.tokens.find(
    (token) => token.symbol === market.token_in,
  );
  const tokenOut = chain.tokens.find(
    (token) => token.symbol === market.token_out,
  );
  if (!tokenIn || !tokenOut) {
    throw new Error(`configured market tokens are missing in ${path}`);
  }
  return {
    fromAsset: BigInt(tokenIn.address),
    toAsset: BigInt(tokenOut.address),
  };
}

function bigintArg(
  values: Map<string, string>,
  name: string,
  fallback: bigint,
): bigint {
  const raw = values.get(name);
  if (raw === undefined) {
    return fallback;
  }
  try {
    return BigInt(raw);
  } catch {
    throw new Error(`${name} must be an integer or 0x-prefixed integer`);
  }
}

function printHelp(): void {
  console.log(`mock-kage-user proof generator

Options:
  --output PATH             Output JSON path
  --from-asset VALUE        Input asset field; defaults to first configured market
  --to-asset VALUE          Output asset field; defaults to first configured market
  --input-value VALUE       Input note value (default: 100)
  --from-amount VALUE       Amount spent from input note (default: 60)
  --received-amount VALUE   Amount placed in received note (default: 50)
  --expiry VALUE            Unix timestamp in seconds (default: now + 300)
`);
}

main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`[mock-kage-user] failed: ${message}`);
  process.exitCode = 1;
});
