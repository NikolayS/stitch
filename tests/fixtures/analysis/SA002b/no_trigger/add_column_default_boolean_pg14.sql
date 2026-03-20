-- Non-volatile default on PG >= 11 should not fire
ALTER TABLE users ADD COLUMN is_active boolean DEFAULT false;
