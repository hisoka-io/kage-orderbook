import type { IntentOptions } from "./darkpool.js";
import type { IntentProofV1 } from "./intent-proof-v1.js";

export const PROOF_PROTOCOL_VERSION = 1 as const;
export const MAX_PROOF_REQUEST_BYTES = 1024 * 1024;

const MAX_U128 = (1n << 128n) - 1n;
const UUID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const ADDRESS = /^0x[0-9a-f]{40}$/;
const DECIMAL = /^(?:0|[1-9][0-9]*)$/;

export interface ProofOrderV1 {
  order_id: string;
  chain_id: number;
  token_in: string;
  token_out: string;
  amount_in: string;
  amount_out: string;
  expires_at_ms: number;
}

export interface ProofRequestV1 {
  version: typeof PROOF_PROTOCOL_VERSION;
  type: "prove_swap_intent";
  request_id: string;
  wallet_fixture: "default";
  order: ProofOrderV1;
}

export interface ProofSuccessResponseV1 {
  version: typeof PROOF_PROTOCOL_VERSION;
  type: "proof_response";
  request_id: string;
  ok: true;
  proof: IntentProofV1;
}

export interface ProofErrorResponseV1 {
  version: typeof PROOF_PROTOCOL_VERSION;
  type: "proof_response";
  request_id: string | null;
  ok: false;
  error: {
    code: string;
    message: string;
  };
}

export type ProofResponseV1 = ProofSuccessResponseV1 | ProofErrorResponseV1;

export class ProtocolError extends Error {
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "ProtocolError";
  }
}

export function parseProofRequestV1(line: string): ProofRequestV1 {
  if (Buffer.byteLength(line, "utf8") > MAX_PROOF_REQUEST_BYTES) {
    throw new ProtocolError("REQUEST_TOO_LARGE", "proof request is too large");
  }

  let value: unknown;
  try {
    value = JSON.parse(line);
  } catch {
    throw new ProtocolError("INVALID_JSON", "proof request is not valid JSON");
  }

  const root = record(value, "proof request");
  exactKeys(root, ["version", "type", "request_id", "wallet_fixture", "order"]);
  if (root.version !== PROOF_PROTOCOL_VERSION) {
    throw new ProtocolError(
      "UNSUPPORTED_VERSION",
      `unsupported proof protocol version: ${String(root.version)}`,
    );
  }
  if (root.type !== "prove_swap_intent") {
    throw new ProtocolError(
      "UNSUPPORTED_REQUEST",
      "unsupported proof request type",
    );
  }
  if (root.wallet_fixture !== "default") {
    throw new ProtocolError("UNKNOWN_WALLET", "unknown mock wallet fixture");
  }

  const requestId = uuid(root.request_id, "request_id");
  const rawOrder = record(root.order, "order");
  exactKeys(rawOrder, [
    "order_id",
    "chain_id",
    "token_in",
    "token_out",
    "amount_in",
    "amount_out",
    "expires_at_ms",
  ]);

  const amountIn = amount(rawOrder.amount_in, "amount_in", MAX_U128 - 1n);
  const amountOut = amount(rawOrder.amount_out, "amount_out", MAX_U128);
  if (amountIn === 0n || amountOut === 0n) {
    throw new ProtocolError(
      "INVALID_AMOUNT",
      "order amounts must be greater than zero",
    );
  }

  return {
    version: PROOF_PROTOCOL_VERSION,
    type: "prove_swap_intent",
    request_id: requestId,
    wallet_fixture: "default",
    order: {
      order_id: uuid(rawOrder.order_id, "order.order_id"),
      chain_id: safeInteger(rawOrder.chain_id, "order.chain_id", 1),
      token_in: address(rawOrder.token_in, "order.token_in"),
      token_out: address(rawOrder.token_out, "order.token_out"),
      amount_in: amountIn.toString(),
      amount_out: amountOut.toString(),
      expires_at_ms: safeInteger(
        rawOrder.expires_at_ms,
        "order.expires_at_ms",
        1,
      ),
    },
  };
}

export function intentOptionsFromRequest(
  request: ProofRequestV1,
  nowMs = Date.now(),
): IntentOptions {
  if (request.order.expires_at_ms <= nowMs) {
    throw new ProtocolError(
      "ORDER_EXPIRED",
      "order expired before proof generation",
    );
  }

  const fromAmount = BigInt(request.order.amount_in);
  return {
    fromAsset: BigInt(request.order.token_in),
    toAsset: BigInt(request.order.token_out),
    inputValue: fromAmount + 1n,
    fromAmount,
    receivedAmount: BigInt(request.order.amount_out),
    expiry: BigInt(Math.floor(request.order.expires_at_ms / 1000)),
  };
}

export function proofSuccess(
  requestId: string,
  proof: IntentProofV1,
): ProofSuccessResponseV1 {
  return {
    version: PROOF_PROTOCOL_VERSION,
    type: "proof_response",
    request_id: requestId,
    ok: true,
    proof,
  };
}

export function proofFailure(
  requestId: string | null,
  error: unknown,
): ProofErrorResponseV1 {
  const protocolError = error instanceof ProtocolError ? error : null;
  return {
    version: PROOF_PROTOCOL_VERSION,
    type: "proof_response",
    request_id: requestId,
    ok: false,
    error: {
      code: protocolError?.code ?? "PROOF_GENERATION_FAILED",
      message:
        error instanceof Error
          ? error.message
          : "unknown proof generation error",
    },
  };
}

function record(value: unknown, name: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new ProtocolError("INVALID_REQUEST", `${name} must be an object`);
  }
  return value as Record<string, unknown>;
}

function exactKeys(value: Record<string, unknown>, allowed: string[]): void {
  const unknown = Object.keys(value).find((key) => !allowed.includes(key));
  if (unknown) {
    throw new ProtocolError(
      "UNKNOWN_FIELD",
      `unknown proof request field: ${unknown}`,
    );
  }
  const missing = allowed.find((key) => !(key in value));
  if (missing) {
    throw new ProtocolError(
      "MISSING_FIELD",
      `missing proof request field: ${missing}`,
    );
  }
}

function uuid(value: unknown, name: string): string {
  if (typeof value !== "string" || !UUID.test(value)) {
    throw new ProtocolError("INVALID_ID", `${name} must be a lowercase UUID`);
  }
  return value;
}

function address(value: unknown, name: string): string {
  if (typeof value !== "string" || !ADDRESS.test(value)) {
    throw new ProtocolError(
      "INVALID_ADDRESS",
      `${name} must be a canonical lowercase EVM address`,
    );
  }
  return value;
}

function amount(value: unknown, name: string, maximum: bigint): bigint {
  if (typeof value !== "string" || !DECIMAL.test(value)) {
    throw new ProtocolError(
      "INVALID_AMOUNT",
      `${name} must be a decimal string`,
    );
  }
  const parsed = BigInt(value);
  if (parsed > maximum) {
    throw new ProtocolError(
      "INVALID_AMOUNT",
      `${name} exceeds the supported u128 range`,
    );
  }
  return parsed;
}

function safeInteger(value: unknown, name: string, minimum: number): number {
  if (!Number.isSafeInteger(value) || (value as number) < minimum) {
    throw new ProtocolError(
      "INVALID_INTEGER",
      `${name} must be a safe integer`,
    );
  }
  return value as number;
}
