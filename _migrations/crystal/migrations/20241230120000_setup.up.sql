CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Immutable wrapper for varchar/text to bytea conversion.
-- Needed for GENERATED ALWAYS columns (which require IMMUTABLE expressions).
-- The built-in textsend() is only STABLE, not IMMUTABLE, in PostgreSQL <= 16.
CREATE OR REPLACE FUNCTION immutable_textsend(val text)
RETURNS bytea
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
AS $$ SELECT textsend(val); $$;