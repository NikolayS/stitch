-- Verify seed_initial_data
SELECT 1/COUNT(*) FROM public.users WHERE username = 'admin';
