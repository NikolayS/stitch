-- CREATE TABLE with NOT NULL columns should not trigger SA001
-- SA001 only cares about ADD COLUMN on existing tables
CREATE TABLE users (
  id bigint PRIMARY KEY,
  email text NOT NULL,
  name text NOT NULL
);
