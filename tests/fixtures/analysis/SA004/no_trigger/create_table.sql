-- CREATE TABLE with inline index should not trigger SA004
CREATE TABLE users (
  id bigint PRIMARY KEY,
  email text UNIQUE
);
