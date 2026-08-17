import { createRequire } from "node:module";
import { existsSync } from "node:fs";
import { join } from "node:path";

import type {
  IntentProofPins,
  SwapIntentProofLike,
} from "./intent-proof-v1.js";
import { INTENT_PROOF_V1_PINS } from "./intent-proof-v1.js";

interface FrValue {
  toString(): string;
  toBigInt(): bigint;
}

interface FrConstructor {
  new (value: bigint | number | string): FrValue;
}

interface ProverModule {
  proveSwapIntent(
    inputs: Record<string, unknown>,
  ): Promise<SwapIntentProofLike>;
  INTENT_PROOF_LEN: number;
  INTENT_PI_LEN: number;
  INTENT_VK_LEN: number;
  INTENT_VK_HASH: string;
}

interface WalletModule {
  deriveCek(eph: FrValue, compliancePk: [bigint, bigint]): unknown;
  computePsi(cek: unknown): Promise<FrValue>;
}

interface BarretenbergModule {
  Barretenberg: {
    destroySingleton(): Promise<void>;
  };
}

export interface IntentOptions {
  fromAsset: bigint;
  toAsset: bigint;
  inputValue: bigint;
  fromAmount: bigint;
  receivedAmount: bigint;
  expiry: bigint;
}

export interface DarkpoolRuntime {
  pins: IntentProofPins;
  prove(options: IntentOptions): Promise<SwapIntentProofLike>;
  close(): Promise<void>;
}

const COMPLIANCE_PK: [bigint, bigint] = [
  0x085ed469c9a9f102b6d4f6f909b8ceaf6ca49b39759ac2e0feb7e0aada8b7111n,
  0x245e25ab2bd42f0280a5ade750828dd6868f5225ae798d6b51c676f519c8f4e8n,
];
const OWNER =
  0x2874ae964d8b283e2f521a7f14125fc92747bb9770139b8d4b70ee09e2d83785n;
const MAX_U128 = (1n << 128n) - 1n;

export function loadDarkpoolRuntime(darkpoolRoot: string): DarkpoolRuntime {
  const proverRoot = join(darkpoolRoot, "packages", "prover");
  const proverEntry = join(proverRoot, "dist", "index.cjs");
  if (!existsSync(proverEntry)) {
    throw new Error(`Darkpool prover is not built: ${proverEntry}`);
  }

  const requireFromProver = createRequire(join(proverRoot, "package.json"));
  const prover = requireFromProver(proverEntry) as ProverModule;
  const { Fr } = requireFromProver("@aztec/foundation/fields") as {
    Fr: FrConstructor;
  };
  const wallets = requireFromProver("@hisoka/wallets") as WalletModule;
  const bb = requireFromProver("@aztec/bb.js") as BarretenbergModule;
  assertSdkPins(prover);

  return {
    pins: INTENT_PROOF_V1_PINS,
    prove: async (options) => {
      validateOptions(options);
      const inputPsi = await wallets.computePsi(
        wallets.deriveCek(new Fr(111n), COMPLIANCE_PK),
      );
      const changePsi = await wallets.computePsi(
        wallets.deriveCek(new Fr(9n), COMPLIANCE_PK),
      );
      const receivedPsi = await wallets.computePsi(
        wallets.deriveCek(new Fr(15n), COMPLIANCE_PK),
      );
      const note = (
        asset: bigint,
        value: bigint,
        psi: FrValue,
        parents: bigint,
      ): Record<string, unknown> => ({
        noteVersion: new Fr(1n),
        assetId: new Fr(asset),
        noteType: new Fr(0n),
        conditionsHash: new Fr(0n),
        value: new Fr(value),
        owner: new Fr(OWNER),
        psi,
        parents: new Fr(parents),
      });

      return prover.proveSwapIntent({
        compliancePk: COMPLIANCE_PK,
        noteIn: note(options.fromAsset, options.inputValue, inputPsi, 0n),
        spendScalar: new Fr(789n),
        indexIn: 0,
        pathIn: Array.from({ length: 32 }, () => new Fr(0n)),
        changeNote: note(
          options.fromAsset,
          options.inputValue - options.fromAmount,
          changePsi,
          0n,
        ),
        changeEph: new Fr(9n),
        receivedNote: note(
          options.toAsset,
          options.receivedAmount,
          receivedPsi,
          0n,
        ),
        receivedEph: new Fr(15n),
        toAsset: new Fr(options.toAsset),
        fromAmount: new Fr(options.fromAmount),
        expiry: new Fr(options.expiry),
      });
    },
    close: () => bb.Barretenberg.destroySingleton(),
  };
}

function assertSdkPins(prover: ProverModule): void {
  if (
    prover.INTENT_PROOF_LEN !== INTENT_PROOF_V1_PINS.proofFields ||
    prover.INTENT_PI_LEN !== INTENT_PROOF_V1_PINS.publicInputs ||
    prover.INTENT_VK_LEN !== INTENT_PROOF_V1_PINS.verificationKeyFields ||
    prover.INTENT_VK_HASH.toLowerCase() !==
      INTENT_PROOF_V1_PINS.verificationKeyHash
  ) {
    throw new Error(
      "Darkpool swap_intent circuit pins changed; define a new proof envelope version",
    );
  }
}

function validateOptions(options: IntentOptions): void {
  for (const [name, value] of Object.entries(options)) {
    if (value < 0n) {
      throw new Error(`${name} must not be negative`);
    }
  }
  if (options.fromAmount === 0n || options.receivedAmount === 0n) {
    throw new Error("swap amounts must be greater than zero");
  }
  if (options.inputValue < options.fromAmount) {
    throw new Error("input note value must cover from amount");
  }
  if (
    options.inputValue > MAX_U128 ||
    options.fromAmount > MAX_U128 ||
    options.receivedAmount > MAX_U128
  ) {
    throw new Error("note values and swap amounts must fit u128");
  }
}
