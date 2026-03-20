-- On PG < 11, even non-volatile defaults cause a table rewrite
ALTER TABLE users ADD COLUMN status text DEFAULT 'active';
