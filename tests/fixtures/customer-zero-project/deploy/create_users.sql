-- Deploy create_users
-- Pattern 1: Simple CREATE TABLE

BEGIN;

CREATE TABLE public.users (
    id          SERIAL PRIMARY KEY,
    username    TEXT NOT NULL UNIQUE,
    email       TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

COMMIT;
