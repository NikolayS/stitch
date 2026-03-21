-- Revert create_audit_func
BEGIN;
DROP TRIGGER IF EXISTS users_audit_trigger ON public.users;
DROP FUNCTION IF EXISTS public.audit_trigger_func();
DROP TABLE IF EXISTS public.audit_log;
COMMIT;
