CREATE INDEX idx_orders_status ON orders (status) WHERE status = 'pending';
