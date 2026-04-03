-- Supabase migration: Add personal data columns to contacts table
-- Run this in the Supabase SQL Editor (dashboard -> SQL -> New Query)
-- Safe to re-run (IF NOT EXISTS)

ALTER TABLE contacts ADD COLUMN IF NOT EXISTS address TEXT;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS birthday TEXT;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS anniversary TEXT;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS website TEXT;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS social_profiles JSONB DEFAULT '{}';
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS personal_data_source JSONB DEFAULT '{}';
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS gmail_mined_at TIMESTAMPTZ;
