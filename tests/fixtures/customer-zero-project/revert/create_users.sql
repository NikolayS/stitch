-- Revert create_users
BEGIN;
DROP TABLE IF EXISTS public.users CASCADE;
COMMIT;
