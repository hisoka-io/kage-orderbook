import assert from "node:assert/strict";
import test from "node:test";

import {
  intentOptionsFromRequest,
  parseProofRequestV1,
  ProtocolError,
} from "../src/protocol.js";

const validRequest = () => ({
  version: 1,
  type: "prove_swap_intent",
  request_id: "11111111-1111-4111-8111-111111111111",
  wallet_fixture: "default",
  order: {
    order_id: "22222222-2222-4222-8222-222222222222",
    chain_id: 31337,
    token_in: "0x0101010101010101010101010101010101010101",
    token_out: "0x0202020202020202020202020202020202020202",
    amount_in: "60",
    amount_out: "50",
    expires_at_ms: 2_000_000_000_000,
  },
});

test("parses a canonical ProofRequestV1", () => {
  const request = parseProofRequestV1(JSON.stringify(validRequest()));
  assert.equal(request.version, 1);
  assert.equal(request.order.chain_id, 31337);
});

test("maps exact order terms into the Darkpool intent", () => {
  const request = parseProofRequestV1(JSON.stringify(validRequest()));
  const intent = intentOptionsFromRequest(request, 1_000_000_000_000);
  assert.equal(intent.fromAsset, BigInt(request.order.token_in));
  assert.equal(intent.toAsset, BigInt(request.order.token_out));
  assert.equal(intent.fromAmount, 60n);
  assert.equal(intent.receivedAmount, 50n);
  assert.equal(intent.inputValue, 61n);
  assert.equal(intent.expiry, 2_000_000_000n);
});

test("rejects non-string amounts", () => {
  const request = validRequest();
  request.order.amount_in = 60 as unknown as string;
  assertProtocolError(request, "INVALID_AMOUNT");
});

test("rejects unknown fields", () => {
  const request = { ...validRequest(), unexpected: true };
  assertProtocolError(request, "UNKNOWN_FIELD");
});

test("rejects an expired order before proving", () => {
  const request = parseProofRequestV1(JSON.stringify(validRequest()));
  assert.throws(
    () => intentOptionsFromRequest(request, request.order.expires_at_ms),
    (error) => error instanceof ProtocolError && error.code === "ORDER_EXPIRED",
  );
});

function assertProtocolError(value: unknown, code: string): void {
  assert.throws(
    () => parseProofRequestV1(JSON.stringify(value)),
    (error) => error instanceof ProtocolError && error.code === code,
  );
}
