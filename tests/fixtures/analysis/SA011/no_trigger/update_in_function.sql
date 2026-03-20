CREATE FUNCTION archive_users() RETURNS void AS $$
BEGIN
  UPDATE users SET archived = true WHERE last_login < '2020-01-01';
END;
$$ LANGUAGE plpgsql;
