-- TRUNCATE inside a DO block is excluded from SA008
DO $$
BEGIN
  TRUNCATE old_data;
END;
$$;
