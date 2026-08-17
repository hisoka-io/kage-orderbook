export const INTENT_PROOF_VERSION = 1 as const;
export const INTENT_CIRCUIT = "swap_intent" as const;
export const INTENT_PROOF_SYSTEM = "ultra_honk" as const;
export const INTENT_VERIFIER_TARGET = "noir-recursive" as const;

export interface IntentProofPins {
  readonly proofFields: number;
  readonly publicInputs: number;
  readonly verificationKeyFields: number;
  readonly verificationKeyHash: string;
}

export const INTENT_PROOF_V1_PINS: IntentProofPins = Object.freeze({
  proofFields: 458,
  publicInputs: 27,
  verificationKeyFields: 115,
  verificationKeyHash:
    "0x2e0b51ed4736571c1daa939f67d50684aa262bab910d8971e77c0d65f89efc50",
});

export interface SwapIntentProofLike {
  proof: Uint8Array;
  proofAsFields: string[];
  publicInputs: string[];
  vkAsFields: string[];
  vkHash: string;
  verified: boolean;
}

export interface IntentProofV1 {
  version: typeof INTENT_PROOF_VERSION;
  circuit: typeof INTENT_CIRCUIT;
  proofSystem: typeof INTENT_PROOF_SYSTEM;
  verifierTarget: typeof INTENT_VERIFIER_TARGET;
  proof: string;
  proofAsFields: string[];
  publicInputs: string[];
  verificationKeyFields: string[];
  verificationKeyHash: string;
}

const FIELD_HEX = /^0x[0-9a-f]{64}$/;
const BYTES_HEX = /^0x(?:[0-9a-f]{2})+$/;

export function createIntentProofV1(
  proof: SwapIntentProofLike,
  pins: IntentProofPins,
): IntentProofV1 {
  if (!proof.verified) {
    throw new Error("Darkpool SDK did not verify the generated intent proof");
  }

  const envelope: IntentProofV1 = {
    version: INTENT_PROOF_VERSION,
    circuit: INTENT_CIRCUIT,
    proofSystem: INTENT_PROOF_SYSTEM,
    verifierTarget: INTENT_VERIFIER_TARGET,
    proof: bytesToHex(proof.proof),
    proofAsFields: [...proof.proofAsFields],
    publicInputs: [...proof.publicInputs],
    verificationKeyFields: [...proof.vkAsFields],
    verificationKeyHash: proof.vkHash.toLowerCase(),
  };

  assertIntentProofV1(envelope, pins);
  return envelope;
}

export function assertIntentProofV1(
  envelope: IntentProofV1,
  pins: IntentProofPins,
): void {
  if (
    envelope.version !== INTENT_PROOF_VERSION ||
    envelope.circuit !== INTENT_CIRCUIT ||
    envelope.proofSystem !== INTENT_PROOF_SYSTEM ||
    envelope.verifierTarget !== INTENT_VERIFIER_TARGET
  ) {
    throw new Error("unsupported intent proof envelope");
  }

  assertLength("proof fields", envelope.proofAsFields, pins.proofFields);
  assertLength("public inputs", envelope.publicInputs, pins.publicInputs);
  assertLength(
    "verification-key fields",
    envelope.verificationKeyFields,
    pins.verificationKeyFields,
  );

  assertFields("proof fields", envelope.proofAsFields);
  assertFields("public inputs", envelope.publicInputs);
  assertFields("verification-key fields", envelope.verificationKeyFields);

  if (!BYTES_HEX.test(envelope.proof)) {
    throw new Error("proof must be non-empty, lowercase, byte-aligned hex");
  }
  if (!FIELD_HEX.test(envelope.verificationKeyHash)) {
    throw new Error("verification-key hash must be a canonical field");
  }
  if (envelope.verificationKeyHash !== pins.verificationKeyHash.toLowerCase()) {
    throw new Error("verification-key hash does not match the pinned circuit");
  }

  const fieldsFromProof = proofHexToFields(envelope.proof);
  if (
    fieldsFromProof.length !== envelope.proofAsFields.length ||
    fieldsFromProof.some(
      (field, index) => field !== envelope.proofAsFields[index],
    )
  ) {
    throw new Error("proof bytes and proof fields do not match");
  }
}

export function bytesToHex(bytes: Uint8Array): string {
  return `0x${Buffer.from(bytes).toString("hex")}`;
}

function proofHexToFields(proof: string): string[] {
  const body = proof.slice(2);
  if (body.length % 64 !== 0) {
    throw new Error("proof byte length must be divisible into 32-byte fields");
  }

  const fields: string[] = [];
  for (let offset = 0; offset < body.length; offset += 64) {
    fields.push(`0x${body.slice(offset, offset + 64)}`);
  }
  return fields;
}

function assertLength(name: string, values: string[], expected: number): void {
  if (values.length !== expected) {
    throw new Error(`${name} length ${values.length}; expected ${expected}`);
  }
}

function assertFields(name: string, values: string[]): void {
  if (values.some((value) => !FIELD_HEX.test(value))) {
    throw new Error(`${name} must contain canonical lowercase fields`);
  }
}
