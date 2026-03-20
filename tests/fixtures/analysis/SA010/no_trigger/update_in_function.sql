-- UPDATE inside a function body is excluded from SA010
CREATE FUNCTION reset_users() RETURNS void AS $$
BEGIN
  UPDATE users SET status = 'inactive';
END;
$$ LANGUAGE plpgsql;
