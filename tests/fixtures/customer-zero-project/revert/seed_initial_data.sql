-- Revert seed_initial_data
BEGIN;
DELETE FROM public.user_profiles;
DELETE FROM public.users;
COMMIT;
