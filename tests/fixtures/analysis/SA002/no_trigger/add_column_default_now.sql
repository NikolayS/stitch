-- now() is STABLE, not volatile — safe on PG 11+
ALTER TABLE events ADD COLUMN created_at timestamptz DEFAULT now();
