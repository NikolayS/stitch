-- Deploy seed_initial_data
-- Pattern 4: Migration that would use \i but uses inline SQL instead
-- In a real project this might be: \i seeds/users.sql
-- For testing we inline the seed data directly.

BEGIN;

INSERT INTO public.users (username, email)
VALUES
    ('admin', 'admin@example.com'),
    ('demo_user', 'demo@example.com');

INSERT INTO public.user_profiles (user_id, display_name, bio)
SELECT id, 'Admin User', 'System administrator'
FROM public.users WHERE username = 'admin';

INSERT INTO public.user_profiles (user_id, display_name, bio)
SELECT id, 'Demo User', 'Demo account for testing'
FROM public.users WHERE username = 'demo_user';

COMMIT;
