ALTER TABLE orders
ADD COLUMN chain_id INTEGER NOT NULL DEFAULT 31337
CHECK (chain_id > 0);

CREATE INDEX orders_chain_state_idx
ON orders (chain_id, state);
