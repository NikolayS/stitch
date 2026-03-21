-- Revert add_user_profiles
BEGIN;
DROP TABLE IF EXISTS public.user_profiles;
COMMIT;
