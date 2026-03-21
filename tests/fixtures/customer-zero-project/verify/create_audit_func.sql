-- Verify create_audit_func
SELECT id, table_name, operation, row_id, changed_at, changed_by FROM public.audit_log WHERE FALSE;
SELECT 1/COUNT(*) FROM pg_proc WHERE proname = 'audit_trigger_func';
