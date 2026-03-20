-- TRUNCATE inside a function body is excluded from SA008
CREATE FUNCTION reset_data() RETURNS void AS $$
BEGIN
  TRUNCATE users;
END;
$$ LANGUAGE plpgsql;
