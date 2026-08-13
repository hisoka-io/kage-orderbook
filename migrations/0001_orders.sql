CREATE TABLE orders (
    id TEXT PRIMARY KEY NOT NULL,
    state TEXT NOT NULL CHECK (state IN (
        'created',
        'validated',
        'reserving',
        'assigned',
        'awaiting_user_proof',
        'proof_relayed',
        'executing',
        'filled',
        'expired',
        'cancelled',
        'failed'
    )),
    version INTEGER NOT NULL CHECK (version >= 0),
    token_in BLOB NOT NULL CHECK (length(token_in) = 20),
    token_out BLOB NOT NULL CHECK (length(token_out) = 20),
    amount_in TEXT NOT NULL,
    amount_out TEXT NOT NULL,
    solver_id TEXT,
    solver_noise_public_key BLOB,
    tx_hash BLOB CHECK (tx_hash IS NULL OR length(tx_hash) = 32),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER
) STRICT;

CREATE INDEX orders_state_idx ON orders (state);
CREATE INDEX orders_solver_state_idx ON orders (solver_id, state);
CREATE INDEX orders_expires_at_idx ON orders (expires_at_ms)
    WHERE expires_at_ms IS NOT NULL;

CREATE TABLE proof_payloads (
    order_id TEXT PRIMARY KEY NOT NULL,
    solver_id TEXT NOT NULL,
    ciphertext BLOB NOT NULL CHECK (length(ciphertext) > 0),
    created_at_ms INTEGER NOT NULL,
    delivered_at_ms INTEGER,
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
) STRICT;

CREATE INDEX proof_payloads_solver_idx
    ON proof_payloads (solver_id, delivered_at_ms);
