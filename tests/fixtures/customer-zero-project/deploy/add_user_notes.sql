-- Deploy add_user_notes
-- Pattern 5: Migration with a note containing special characters

BEGIN;

CREATE TABLE public.user_notes (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
    title       TEXT NOT NULL,
    body        TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE public.user_notes IS 'Notes with "special" chars: <html>, &amp;, backslash-n';

CREATE INDEX idx_user_notes_user_id ON public.user_notes (user_id);

COMMIT;
