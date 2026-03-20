-- DELETE inside a DO block is excluded from SA010
DO $$
BEGIN
  DELETE FROM temp_data;
END;
$$;
