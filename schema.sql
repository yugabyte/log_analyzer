-- Schema for storing reports
-- Created: 16 June 2025

CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE SCHEMA IF NOT EXISTS log_analyzer;
CREATE TABLE log_analyzer.reports (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    universe_name TEXT NOT NULL,
    ticket INT,
    json_report JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE public.reports (
    id UUID PRIMARY KEY,
    support_bundle_name TEXT NOT NULL,
    json_report JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_reports_support_bundle_name ON public.reports (support_bundle_name);