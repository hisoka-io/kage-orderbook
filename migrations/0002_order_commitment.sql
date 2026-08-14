ALTER TABLE orders
ADD COLUMN order_commitment BLOB
CHECK (
    order_commitment IS NULL OR length(order_commitment) = 32
);

CREATE UNIQUE INDEX orders_order_commitment_idx
ON orders (order_commitment)
WHERE order_commitment IS NOT NULL;
