-- Revert add_user_notes
BEGIN;
DROP TABLE IF EXISTS public.user_notes;
COMMIT;
