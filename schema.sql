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

CREATE TABLE public.log_analyzer_reports (
    id UUID PRIMARY KEY,
    support_bundle_name TEXT NOT NULL,
    json_report JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_reports_support_bundle_name ON public.log_analyzer_reports (support_bundle_name);

-- Queries used to fetch reports
-- Get the support bundle name for a given report ID

SELECT support_bundle_name FROM public.log_analyzer_reports WHERE id::text = %s

-- Get cluster_uuid and organization for this report:

SELECT h.cluster_uuid, h.organization, h.cluster_name
FROM public.support_bundle_header h
WHERE h.support_bundle = 'yb-support-bundle-cbdc-dr-20250710035142.995-logs'

-- Get the report for same cluster_uuid and organization:

SELECT r.id, r.support_bundle_name, h.cluster_name, h.organization, h.cluster_uuid, h.case_id, r.created_at
FROM public.log_analyzer_reports r
JOIN public.support_bundle_header h ON r.support_bundle_name = h.support_bundle
WHERE (h.organization = 'Mindgate' OR h.cluster_uuid = '023eb068-59ec-4aa3-8941-b66e90b87203')
  AND r.id::text != '023eb068-59ec-4aa3-8941-b66e90b87203'
ORDER BY r.created_at DESC LIMIT 20;

-- Get node information for log analyzer

SELECT * FROM public.view_node_info_for_log_analyzer
WHERE support_bundle_name = 'yb-support-bundle-MULTI-DC-NODE-20250526181519.059-logs';