-- now() is STABLE, safe on PG 11+
ALTER TABLE events ADD COLUMN created_at timestamptz DEFAULT now();
