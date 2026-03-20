-- now() is STABLE, not volatile, but on PG < 11 all defaults cause rewrite
ALTER TABLE events ADD COLUMN created_at timestamptz DEFAULT now();
