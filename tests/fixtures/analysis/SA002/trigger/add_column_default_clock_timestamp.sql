ALTER TABLE events ADD COLUMN recorded_at timestamptz DEFAULT clock_timestamp();
