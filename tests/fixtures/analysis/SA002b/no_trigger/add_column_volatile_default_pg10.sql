-- Volatile defaults are handled by SA002, not SA002b
ALTER TABLE users ADD COLUMN id uuid DEFAULT gen_random_uuid();
