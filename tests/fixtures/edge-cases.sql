-- Edge case SQL fixture for parser spike testing
-- Tests: multi-statement, dollar-quoting, CREATE FUNCTION, comments, various DDL/DML

-- 1. Simple DDL
CREATE TABLE users (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email text NOT NULL UNIQUE,
    display_name text,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- 2. Multi-column index
CREATE INDEX idx_users_email ON users (email);

-- 3. Another table with FK
CREATE TABLE posts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    author_id bigint NOT NULL REFERENCES users(id),
    title text NOT NULL,
    body text,
    published_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- 4. Block comment
/* This is a block comment
   spanning multiple lines
   with special characters: '; DROP TABLE users; --
*/

-- 5. Dollar-quoted string in a function
CREATE FUNCTION update_modified_column()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 6. Named dollar-quoting
CREATE FUNCTION complex_function(p_input text)
RETURNS text AS $fn_body$
DECLARE
    v_result text;
BEGIN
    -- This is a comment inside a function body
    v_result := 'Hello, ' || p_input || '!';

    /* Block comment inside function */
    IF v_result IS NULL THEN
        v_result := 'default';
    END IF;

    RETURN v_result;
END;
$fn_body$ LANGUAGE plpgsql VOLATILE;

-- 7. CREATE TRIGGER
CREATE TRIGGER set_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();

-- 8. ALTER TABLE with multiple subcommands
ALTER TABLE posts
    ADD COLUMN updated_at timestamptz,
    ADD COLUMN slug text;

-- 9. INSERT with multiple rows
INSERT INTO users (email, display_name) VALUES
    ('alice@example.com', 'Alice'),
    ('bob@example.com', 'Bob');

-- 10. CTE (WITH clause)
WITH active_users AS (
    SELECT id, email FROM users WHERE created_at > now() - interval '30 days'
)
SELECT u.email, count(p.id) AS post_count
FROM active_users u
LEFT JOIN posts p ON p.author_id = u.id
GROUP BY u.email;

-- 11. View creation
CREATE VIEW recent_posts AS
SELECT p.id, p.title, u.display_name AS author_name, p.published_at
FROM posts p
JOIN users u ON u.id = p.author_id
WHERE p.published_at > now() - interval '7 days';

-- 12. DO block (anonymous code block)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'uuid-ossp') THEN
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
    END IF;
END;
$$;

-- 13. Volatile default (table rewrite on older PG)
ALTER TABLE users ADD COLUMN uuid uuid DEFAULT gen_random_uuid();

-- 14. GRANT/REVOKE
GRANT SELECT ON users TO readonly_role;
REVOKE INSERT ON users FROM public;

-- 15. Complex query with subquery and window function
SELECT
    email,
    display_name,
    created_at,
    ROW_NUMBER() OVER (ORDER BY created_at DESC) AS signup_order
FROM users
WHERE email LIKE '%@example.com'
ORDER BY created_at DESC
LIMIT 10;

-- 16. CREATE TYPE (enum)
CREATE TYPE post_status AS ENUM ('draft', 'published', 'archived');

-- 17. ALTER TABLE with NOT NULL on existing column (dangerous!)
ALTER TABLE posts ALTER COLUMN title SET NOT NULL;

-- 18. CREATE INDEX CONCURRENTLY (non-transactional DDL)
CREATE INDEX CONCURRENTLY idx_posts_published
ON posts (published_at)
WHERE published_at IS NOT NULL;
