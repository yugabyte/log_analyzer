-- Query to get unique message prefixes (labels) from long_operations for a specific report
-- Report ID: fe68a707-950a-4e3e-be46-02bec1239aec

SELECT DISTINCT
    message_prefix,
    LENGTH(message_prefix) AS prefix_length,
    -- Count how many time intervals this prefix appears in
    (
        SELECT COUNT(*)
        FROM jsonb_object_keys(
            json_report->'long_operations'->message_prefix
        ) AS time_interval
    ) AS time_interval_count,
    -- Get total occurrences across all time intervals
    (
        SELECT SUM((value->>'c')::int)
        FROM jsonb_each(
            json_report->'long_operations'->message_prefix
        ) AS time_data(value)
    ) AS total_occurrences,
    -- Get max duration across all time intervals
    (
        SELECT MAX((value->>'max')::numeric)
        FROM jsonb_each(
            json_report->'long_operations'->message_prefix
        ) AS time_data(value)
    ) AS max_duration,
    -- Get average duration across all time intervals
    (
        SELECT AVG((value->>'avg')::numeric)
        FROM jsonb_each(
            json_report->'long_operations'->message_prefix
        ) AS time_data(value)
    ) AS avg_duration
FROM public.log_analyzer_reports,
     jsonb_object_keys(json_report->'long_operations') AS message_prefix
WHERE id::text = 'fe68a707-950a-4e3e-be46-02bec1239aec'
  AND json_report->'long_operations' IS NOT NULL
ORDER BY 
    total_occurrences DESC NULLS LAST,
    message_prefix;

-- Simpler version: Just get the unique labels with counts
-- Uncomment to use this simpler query instead:

/*
SELECT DISTINCT
    message_prefix,
    COUNT(*) OVER (PARTITION BY message_prefix) AS time_interval_count
FROM public.log_analyzer_reports,
     jsonb_object_keys(json_report->'long_operations') AS message_prefix
WHERE id::text = 'fe68a707-950a-4e3e-be46-02bec1239aec'
  AND json_report->'long_operations' IS NOT NULL
ORDER BY message_prefix;
*/

