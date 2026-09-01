CREATE TABLE orders (
    id TEXT PRIMARY KEY NOT NULL,
    chain_id INTEGER NOT NULL CHECK (chain_id > 0),
    state TEXT NOT NULL CHECK (state IN (
        'submitted',
        'reservation_pending',
        'assigned',
        'proof_delivered',
        'proof_accepted',
        'proof_rejected',
        'expired',
        'complaint_verified',
        'closed'
    )),
    version INTEGER NOT NULL CHECK (version >= 0),
    token_in BLOB NOT NULL CHECK (length(token_in) = 20),
    token_out BLOB NOT NULL CHECK (length(token_out) = 20),
    amount_in TEXT NOT NULL,
    amount_out TEXT NOT NULL,
    solver_address BLOB CHECK (
        solver_address IS NULL OR length(solver_address) = 20
    ),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER
) STRICT;

CREATE INDEX orders_state_idx ON orders (state);
CREATE INDEX orders_chain_state_idx ON orders (chain_id, state);
CREATE INDEX orders_solver_address_state_idx
    ON orders (solver_address, state);
CREATE INDEX orders_expires_at_idx ON orders (expires_at_ms)
    WHERE expires_at_ms IS NOT NULL;

CREATE TABLE proof_orders (
    order_id TEXT PRIMARY KEY NOT NULL,
    access_token_hash BLOB NOT NULL UNIQUE CHECK (length(access_token_hash) = 32),
    preview_id BLOB NOT NULL CHECK (length(preview_id) = 32),
    category_id TEXT NOT NULL CHECK (length(category_id) BETWEEN 1 AND 64),
    state TEXT NOT NULL CHECK (state IN (
        'submitted', 'reservation_pending', 'assigned', 'proof_delivered',
        'proof_accepted', 'proof_rejected', 'expired',
        'complaint_verified', 'closed'
    )),
    version INTEGER NOT NULL CHECK (version >= 0),
    chain_id INTEGER NOT NULL CHECK (chain_id > 0),
    token_in BLOB NOT NULL CHECK (length(token_in) = 20),
    token_out BLOB NOT NULL CHECK (length(token_out) = 20),
    amount_in TEXT NOT NULL,
    amount_out TEXT NOT NULL,
    fee_bps INTEGER NOT NULL CHECK (fee_bps BETWEEN 1 AND 10000),
    domain_hash BLOB NOT NULL CHECK (length(domain_hash) = 32),
    exact_terms_digest BLOB NOT NULL CHECK (length(exact_terms_digest) = 32),
    settlement_commitment BLOB NOT NULL CHECK (length(settlement_commitment) = 32),
    ciphertext_digest BLOB NOT NULL CHECK (length(ciphertext_digest) = 32),
    proof_expires_at_ms INTEGER NOT NULL CHECK (
        proof_expires_at_ms > 0 AND proof_expires_at_ms % 1000 = 0
    ),
    request_digest BLOB NOT NULL CHECK (length(request_digest) = 32),
    assigned_solver BLOB CHECK (
        assigned_solver IS NULL OR length(assigned_solver) = 20
    ),
    assigned_key_id BLOB CHECK (
        assigned_key_id IS NULL OR length(assigned_key_id) = 32
    ),
    proof_accepted_at_ms INTEGER,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
    CHECK (
        (assigned_solver IS NULL AND assigned_key_id IS NULL) OR
        (assigned_solver IS NOT NULL AND assigned_key_id IS NOT NULL)
    )
) STRICT;

CREATE INDEX proof_orders_state_expiry_idx
    ON proof_orders (state, proof_expires_at_ms);
CREATE INDEX proof_orders_assigned_solver_state_idx
    ON proof_orders (assigned_solver, state)
    WHERE assigned_solver IS NOT NULL;
CREATE INDEX proof_orders_cleanup_idx
    ON proof_orders (updated_at_ms, state);

-- The encrypted proof can be erased independently of durable accountability
-- evidence. A missing row means retention cleanup already destroyed it.
CREATE TABLE proof_order_payloads (
    order_id TEXT PRIMARY KEY NOT NULL,
    envelope_suite TEXT NOT NULL,
    nonce BLOB NOT NULL CHECK (length(nonce) = 24),
    ciphertext BLOB NOT NULL,
    ciphertext_digest BLOB NOT NULL CHECK (length(ciphertext_digest) = 32),
    disclosed_at_ms INTEGER,
    erase_after_ms INTEGER NOT NULL,
    FOREIGN KEY (order_id) REFERENCES proof_orders(order_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX proof_order_payloads_cleanup_idx
    ON proof_order_payloads (erase_after_ms);

-- Candidate storage intentionally excludes public keys, margins, and amount
-- ranges. Only routing identity and the encrypted key wrap are retained.
CREATE TABLE proof_order_candidates (
    order_id TEXT NOT NULL,
    position INTEGER NOT NULL CHECK (position >= 0),
    solver_id BLOB NOT NULL CHECK (length(solver_id) = 20),
    key_id BLOB NOT NULL CHECK (length(key_id) = 32),
    encapsulated_key BLOB NOT NULL,
    wrapped_key BLOB NOT NULL,
    PRIMARY KEY (order_id, solver_id),
    UNIQUE (order_id, position),
    UNIQUE (order_id, solver_id, key_id),
    FOREIGN KEY (order_id) REFERENCES proof_orders(order_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE proof_order_reservation_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL,
    attempt_number INTEGER NOT NULL CHECK (attempt_number >= 0),
    solver_id BLOB NOT NULL CHECK (length(solver_id) = 20),
    key_id BLOB NOT NULL CHECK (length(key_id) = 32),
    attempt_nonce BLOB NOT NULL CHECK (length(attempt_nonce) = 32),
    requested_at_ms INTEGER NOT NULL,
    deadline_ms INTEGER NOT NULL CHECK (deadline_ms > requested_at_ms),
    outcome TEXT NOT NULL CHECK (outcome IN (
        'pending', 'accepted', 'declined', 'timed_out'
    )),
    reservation_ack BLOB,
    signed_decline BLOB,
    responded_at_ms INTEGER,
    UNIQUE (order_id, attempt_number),
    UNIQUE (order_id, solver_id),
    FOREIGN KEY (order_id) REFERENCES proof_orders(order_id) ON DELETE CASCADE
) STRICT;

CREATE UNIQUE INDEX proof_order_one_pending_attempt_idx
    ON proof_order_reservation_attempts (order_id)
    WHERE outcome = 'pending';
CREATE INDEX proof_order_attempt_deadline_idx
    ON proof_order_reservation_attempts (outcome, deadline_ms);
CREATE INDEX proof_order_attempt_target_idx
    ON proof_order_reservation_attempts (solver_id, outcome, requested_at_ms);

CREATE TABLE proof_order_assignments (
    order_id TEXT PRIMARY KEY NOT NULL,
    solver_id BLOB NOT NULL CHECK (length(solver_id) = 20),
    key_id BLOB NOT NULL CHECK (length(key_id) = 32),
    assignment_ticket BLOB NOT NULL,
    assignment_ticket_digest BLOB NOT NULL CHECK (
        length(assignment_ticket_digest) = 32
    ),
    reservation_ack BLOB,
    assigned_at_ms INTEGER NOT NULL,
    disclosed_at_ms INTEGER NOT NULL CHECK (disclosed_at_ms >= assigned_at_ms),
    FOREIGN KEY (order_id) REFERENCES proof_orders(order_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE proof_order_results (
    order_id TEXT PRIMARY KEY NOT NULL,
    acceptance_ack BLOB,
    rejection_ack BLOB,
    accepted_at_ms INTEGER,
    rejected_at_ms INTEGER,
    FOREIGN KEY (order_id) REFERENCES proof_orders(order_id) ON DELETE CASCADE,
    CHECK (NOT (acceptance_ack IS NOT NULL AND rejection_ack IS NOT NULL)),
    CHECK (NOT (accepted_at_ms IS NOT NULL AND rejected_at_ms IS NOT NULL))
) STRICT;

-- These events are evidence about work performed after proof acceptance. They
-- never move the proof-order state backwards and must not be interpreted as
-- proof rejection or settlement confirmation.
CREATE TABLE proof_order_operational_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL,
    event_kind TEXT NOT NULL CHECK (event_kind IN (
        'proving_failure', 'submission_failure', 'transaction_failure'
    )),
    error_code TEXT NOT NULL CHECK (length(error_code) BETWEEN 1 AND 100),
    retryable INTEGER NOT NULL CHECK (retryable IN (0, 1)),
    occurred_at_ms INTEGER NOT NULL,
    FOREIGN KEY (order_id) REFERENCES proof_orders(order_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX proof_order_operational_events_order_time_idx
    ON proof_order_operational_events (order_id, occurred_at_ms);

CREATE TABLE proof_order_complaints (
    order_id TEXT PRIMARY KEY NOT NULL,
    evidence_kind TEXT NOT NULL CHECK (evidence_kind IN (
        'no_response_after_disclosure', 'accepted_not_settled'
    )),
    lifecycle_status TEXT NOT NULL CHECK (lifecycle_status IN (
        'submitted', 'verified', 'rejected', 'resolved'
    )),
    evidence_key_id BLOB NOT NULL CHECK (length(evidence_key_id) = 32),
    opening_nonce BLOB NOT NULL CHECK (length(opening_nonce) = 24),
    opening_ciphertext BLOB NOT NULL CHECK (length(opening_ciphertext) = 80),
    reason TEXT NOT NULL CHECK (length(reason) BETWEEN 1 AND 500),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    retain_until_ms INTEGER NOT NULL,
    legal_hold INTEGER NOT NULL DEFAULT 0 CHECK (legal_hold IN (0, 1)),
    FOREIGN KEY (order_id) REFERENCES proof_orders(order_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX proof_order_complaints_cleanup_idx
    ON proof_order_complaints (lifecycle_status, legal_hold, retain_until_ms);

-- Previews are durable admission capabilities. Public category output and
-- internal oracle inputs are stored together so admission never requotes.
CREATE TABLE proof_order_previews (
    preview_id BLOB PRIMARY KEY NOT NULL CHECK (length(preview_id) = 32),
    chain_id INTEGER NOT NULL CHECK (chain_id > 0),
    token_in BLOB NOT NULL CHECK (length(token_in) = 20),
    token_out BLOB NOT NULL CHECK (length(token_out) = 20),
    token_in_decimals INTEGER NOT NULL CHECK (token_in_decimals BETWEEN 0 AND 255),
    token_out_decimals INTEGER NOT NULL CHECK (token_out_decimals BETWEEN 0 AND 255),
    amount_in TEXT NOT NULL,
    midpoint_amount_out TEXT NOT NULL,
    confidence_amount_out TEXT NOT NULL,
    oracle_adjustment_bps INTEGER NOT NULL CHECK (oracle_adjustment_bps BETWEEN 0 AND 10000),
    oracle_adjustment_amount TEXT NOT NULL,
    price_in_e18 TEXT NOT NULL,
    price_out_e18 TEXT NOT NULL,
    price_in_lower_e18 TEXT NOT NULL,
    price_out_upper_e18 TEXT NOT NULL,
    pricing_sequence INTEGER NOT NULL CHECK (pricing_sequence >= 0),
    published_at_ms INTEGER NOT NULL,
    valid_until_ms INTEGER NOT NULL,
    recommended_proof_lifetime_seconds INTEGER NOT NULL CHECK (
        recommended_proof_lifetime_seconds > 0
    ),
    minimum_remaining_seconds INTEGER NOT NULL CHECK (minimum_remaining_seconds > 0),
    created_at_ms INTEGER NOT NULL,
    erase_after_ms INTEGER NOT NULL CHECK (erase_after_ms > valid_until_ms)
) STRICT;

CREATE INDEX proof_order_previews_cleanup_idx
    ON proof_order_previews (erase_after_ms);

CREATE TABLE proof_order_preview_categories (
    preview_id BLOB NOT NULL CHECK (length(preview_id) = 32),
    position INTEGER NOT NULL CHECK (position >= 0),
    category_id TEXT NOT NULL CHECK (length(category_id) BETWEEN 1 AND 64),
    fee_bps INTEGER NOT NULL CHECK (fee_bps BETWEEN 1 AND 10000),
    exact_amount_out TEXT NOT NULL,
    fee_amount TEXT NOT NULL,
    PRIMARY KEY (preview_id, category_id),
    UNIQUE (preview_id, position),
    FOREIGN KEY (preview_id) REFERENCES proof_order_previews(preview_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE proof_order_preview_routes (
    preview_id BLOB NOT NULL CHECK (length(preview_id) = 32),
    category_id TEXT NOT NULL CHECK (length(category_id) BETWEEN 1 AND 64),
    position INTEGER NOT NULL CHECK (position >= 0),
    solver_id BLOB NOT NULL CHECK (length(solver_id) = 20),
    min_amount_in TEXT NOT NULL,
    max_amount_in TEXT NOT NULL,
    encryption_key_id BLOB NOT NULL CHECK (length(encryption_key_id) = 32),
    encryption_public_key BLOB NOT NULL CHECK (length(encryption_public_key) = 32),
    key_expires_at_ms INTEGER NOT NULL,
    PRIMARY KEY (preview_id, category_id, solver_id),
    UNIQUE (preview_id, category_id, position),
    UNIQUE (preview_id, category_id, solver_id, encryption_key_id),
    FOREIGN KEY (preview_id, category_id)
        REFERENCES proof_order_preview_categories(preview_id, category_id)
        ON DELETE CASCADE
) STRICT;
