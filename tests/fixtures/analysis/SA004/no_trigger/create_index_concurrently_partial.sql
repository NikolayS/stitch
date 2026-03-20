CREATE INDEX CONCURRENTLY idx_orders_status ON orders (status) WHERE status = 'pending';
