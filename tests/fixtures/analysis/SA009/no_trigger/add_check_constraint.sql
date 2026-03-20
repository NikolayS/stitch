-- CHECK constraint is not a foreign key
ALTER TABLE users ADD CONSTRAINT check_email CHECK (email IS NOT NULL);
