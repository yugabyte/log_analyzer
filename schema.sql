-- Schema for storing reports
-- Created: 16 June 2025

CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE SCHEMA IF NOT EXISTS log_analyzer;
-- CREATE TABLE log_analyzer.reports (
--     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
--     universe_name TEXT NOT NULL,
--     ticket INT,
--     json_report JSONB NOT NULL,
--     created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
-- );

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

-- Tablet report schema

CREATE TABLE tablet_report_cluster (
    report_id UUID,
    type TEXT,
    uuid TEXT,
    ip TEXT,
    port INTEGER,
    region TEXT,
    zone TEXT,
    role TEXT,
    uptime INTERVAL
);

CREATE INDEX idx_tablet_report_cluster_report_id ON tablet_report_cluster (report_id);
GRANT ALL ON TABLE tablet_report_cluster TO log_analyzer_user;

CREATE TABLE tablet_report_tablets (
    report_id UUID,
    node_uuid TEXT,
    tablet_uuid TEXT,
    table_name TEXT,
    table_uuid TEXT,
    namespace TEXT,
    state TEXT,
    status TEXT,
    start_key TEXT,
    end_key TEXT,
    sst_size BIGINT,
    wal_size BIGINT,
    cterm INTEGER,
    cidx INTEGER,
    leader TEXT,
    lease_status TEXT);

CREATE INDEX idx_tablet_report_report_id ON tablet_report_tablets (report_id);
GRANT ALL ON TABLE tablet_report_tablets TO log_analyzer_user;
CREATE TABLE tablet_report_tableinfo (
    report_id UUID,
    namespace TEXT,
    tablename TEXT,
    table_uuid TEXT,
    tot_tablet_count INTEGER,
    uniq_tablet_count INTEGER,
    uniq_tablets_estimate INTEGER,
    leader_tablets INTEGER,
    node_tablet_min INTEGER,
    node_tablet_max INTEGER,
    keys_per_tablet INTEGER,
    key_range_overlap INTEGER,
    unmatched_key_size INTEGER,
    comment TEXT,
    sst_tot_bytes BIGINT,
    wal_tot_bytes BIGINT,
    sst_tot_human TEXT,
    wal_tot_human TEXT,
    sst_rf1_human TEXT,
    tot_human TEXT);

CREATE INDEX idx_tablet_report_tableinfo_report_id ON tablet_report_tableinfo (report_id);
GRANT ALL ON TABLE tablet_report_tableinfo TO log_analyzer_user;
CREATE TABLE tablet_report_region_zone_tablets (
    report_id UUID,
    region TEXT,
    zone TEXT,
    tservers INTEGER,
    missing_replicas TEXT,
    "1_replicas" TEXT,
    balanced TEXT
    );

CREATE INDEX idx_tablet_report_region_zone_report_id ON tablet_report_region_zone_tablets (report_id);
GRANT ALL ON TABLE tablet_report_region_zone_tablets TO log_analyzer_user;  