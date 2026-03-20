-- UNIQUE constraint is not a foreign key
ALTER TABLE users ADD CONSTRAINT unique_email UNIQUE (email);
