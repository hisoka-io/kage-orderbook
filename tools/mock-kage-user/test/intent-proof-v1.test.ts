import assert from "node:assert/strict";
import test from "node:test";

import {
  assertIntentProofV1,
  bytesToHex,
  createIntentProofV1,
  type IntentProofPins,
  type SwapIntentProofLike,
} from "../src/intent-proof-v1.js";

const bytes = Uint8Array.from({ length: 64 }, (_, index) => index);
const proofFields = [bytes.slice(0, 32), bytes.slice(32)].map(bytesToHex);
const field = (suffix: string): string => `0x${suffix.padStart(64, "0")}`;
const pins: IntentProofPins = {
  proofFields: 2,
  publicInputs: 2,
  verificationKeyFields: 2,
  verificationKeyHash: field("aa"),
};

const sdkProof = (): SwapIntentProofLike => ({
  proof: bytes,
  proofAsFields: proofFields,
  publicInputs: [field("01"), field("02")],
  vkAsFields: [field("03"), field("04")],
  vkHash: pins.verificationKeyHash,
  verified: true,
});

test("creates a validated IntentProofV1 envelope", () => {
  const envelope = createIntentProofV1(sdkProof(), pins);
  assert.equal(envelope.version, 1);
  assert.equal(envelope.circuit, "swap_intent");
  assert.doesNotThrow(() => assertIntentProofV1(envelope, pins));
});

test("rejects a proof the SDK did not verify", () => {
  assert.throws(
    () => createIntentProofV1({ ...sdkProof(), verified: false }, pins),
    /did not verify/,
  );
});

test("rejects tampered proof fields", () => {
  const envelope = createIntentProofV1(sdkProof(), pins);
  envelope.proofAsFields[0] = field("ff");
  assert.throws(
    () => assertIntentProofV1(envelope, pins),
    /proof bytes and proof fields do not match/,
  );
});

test("rejects an unpinned verification key", () => {
  const envelope = createIntentProofV1(sdkProof(), pins);
  envelope.verificationKeyHash = field("bb");
  assert.throws(
    () => assertIntentProofV1(envelope, pins),
    /does not match the pinned circuit/,
  );
});
