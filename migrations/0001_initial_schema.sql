CREATE TABLE orders (
    id TEXT PRIMARY KEY NOT NULL,
    chain_id INTEGER NOT NULL CHECK (chain_id > 0),
    state TEXT NOT NULL CHECK (state IN (
        'created',
        'validated',
        'reserving',
        'assigned',
        'awaiting_user_proof',
        'expired'
    )),
    version INTEGER NOT NULL CHECK (version >= 0),
    token_in BLOB NOT NULL CHECK (length(token_in) = 20),
    token_out BLOB NOT NULL CHECK (length(token_out) = 20),
    amount_in TEXT NOT NULL,
    amount_out TEXT NOT NULL,
    order_commitment BLOB NOT NULL CHECK (length(order_commitment) = 32),
    solver_address BLOB CHECK (
        solver_address IS NULL OR length(solver_address) = 20
    ),
    solver_noise_public_key BLOB,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER
) STRICT;

CREATE UNIQUE INDEX orders_order_commitment_idx
    ON orders (order_commitment);
CREATE INDEX orders_state_idx ON orders (state);
CREATE INDEX orders_chain_state_idx ON orders (chain_id, state);
CREATE INDEX orders_solver_address_state_idx
    ON orders (solver_address, state);
CREATE INDEX orders_expires_at_idx ON orders (expires_at_ms)
    WHERE expires_at_ms IS NOT NULL;
