-- Deploy create_audit_func
-- Pattern 2: CREATE FUNCTION (multi-statement, requires transaction)

BEGIN;

CREATE TABLE IF NOT EXISTS public.audit_log (
    id          SERIAL PRIMARY KEY,
    table_name  TEXT NOT NULL,
    operation   TEXT NOT NULL,
    row_id      INTEGER,
    changed_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    changed_by  TEXT
);

CREATE OR REPLACE FUNCTION public.audit_trigger_func()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO public.audit_log (table_name, operation, row_id, changed_by)
    VALUES (TG_TABLE_NAME, TG_OP, COALESCE(NEW.id, OLD.id), current_user);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER users_audit_trigger
    AFTER INSERT OR UPDATE OR DELETE ON public.users
    FOR EACH ROW EXECUTE FUNCTION public.audit_trigger_func();

COMMIT;
