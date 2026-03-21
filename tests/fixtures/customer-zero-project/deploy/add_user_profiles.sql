-- Deploy add_user_profiles
-- Pattern 3: Migration with a dependency (FK to users)

BEGIN;

CREATE TABLE public.user_profiles (
    user_id     INTEGER PRIMARY KEY REFERENCES public.users(id) ON DELETE CASCADE,
    display_name TEXT,
    bio         TEXT,
    avatar_url  TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

COMMIT;
