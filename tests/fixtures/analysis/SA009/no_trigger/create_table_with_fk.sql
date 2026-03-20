-- FK in CREATE TABLE is not the same as ALTER TABLE ADD
CREATE TABLE orders (
  id bigint PRIMARY KEY,
  user_id bigint REFERENCES users(id)
);
