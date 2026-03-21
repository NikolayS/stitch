-- Verify add_user_profiles
SELECT user_id, display_name, bio, avatar_url, updated_at FROM public.user_profiles WHERE FALSE;
