ALTER TABLE orders
ADD COLUMN solver_address BLOB
CHECK (solver_address IS NULL OR length(solver_address) = 20);

CREATE INDEX orders_solver_address_state_idx
ON orders (solver_address, state);

ALTER TABLE proof_payloads
ADD COLUMN solver_address BLOB
CHECK (solver_address IS NULL OR length(solver_address) = 20);

CREATE INDEX proof_payloads_solver_address_idx
ON proof_payloads (solver_address, delivered_at_ms);
