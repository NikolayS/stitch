ALTER TABLE order_items ADD CONSTRAINT fk_order_items
  FOREIGN KEY (order_id, product_id) REFERENCES orders(id, product_id);
