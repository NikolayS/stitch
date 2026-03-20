-- random() wrapped in a type cast is still volatile
ALTER TABLE orders ADD COLUMN priority int DEFAULT random()::int;
