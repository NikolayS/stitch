-- On PG >= 11, non-volatile defaults are metadata-only
ALTER TABLE users ADD COLUMN status text DEFAULT 'active';
