-- Test query to verify long operations extraction logic
-- Run this against your parquet files to see what labels will be extracted
-- Usage: duckdb -c "INSTALL parquet; LOAD parquet; <paste query here>"

-- First query: Show sample messages with extracted labels
SELECT 
    "timestamp",
    file,
    SUBSTR(message, 1, 100) AS message_preview,
    long_op_value,
    timing_real_value,
    -- Test the extraction logic (same as in parquet_service.py)
    COALESCE(
        -- Pattern 1: Extract 1-2 words before "took a long time"
        CASE 
            WHEN message LIKE '%took a long time%'
            THEN TRIM(COALESCE(
                regexp_extract(message, '([^\\s]+\\s+[^\\s]+)\\s+took a long time', 1),
                regexp_extract(message, '([^\\s]+)\\s+took a long time', 1)
            ))
            ELSE NULL
        END,
        -- Pattern 2: Extract 1-2 words before "took a long" (without "time")
        CASE 
            WHEN message LIKE '%took a long%'
            THEN TRIM(COALESCE(
                regexp_extract(message, '([^\\s]+\\s+[^\\s]+)\\s+took a long', 1),
                regexp_extract(message, '([^\\s]+)\\s+took a long', 1)
            ))
            ELSE NULL
        END,
        -- Pattern 3: Extract 1-2 words after "Time spent"
        CASE 
            WHEN message LIKE 'Time spent%'
            THEN TRIM(COALESCE(
                regexp_extract(message, 'Time spent\\s+([^\\s]+\\s+[^\\s]+)', 1),
                regexp_extract(message, 'Time spent\\s+([^\\s]+)', 1)
            ))
            ELSE NULL
        END
    ) AS extracted_label,
    COALESCE(long_op_value, timing_real_value) AS op_value
FROM '*.parquet'
WHERE (long_op_value IS NOT NULL OR timing_real_value IS NOT NULL)
  AND (message LIKE '%took a long%' OR message LIKE 'Time spent%')
ORDER BY extracted_label, "timestamp"
LIMIT 50;

-- Second query: Summary of unique labels that will be extracted
SELECT 
    COALESCE(
        CASE 
            WHEN message LIKE '%took a long time%'
            THEN TRIM(COALESCE(
                regexp_extract(message, '([^\\s]+\\s+[^\\s]+)\\s+took a long time', 1),
                regexp_extract(message, '([^\\s]+)\\s+took a long time', 1)
            ))
            ELSE NULL
        END,
        CASE 
            WHEN message LIKE '%took a long%'
            THEN TRIM(COALESCE(
                regexp_extract(message, '([^\\s]+\\s+[^\\s]+)\\s+took a long', 1),
                regexp_extract(message, '([^\\s]+)\\s+took a long', 1)
            ))
            ELSE NULL
        END,
        CASE 
            WHEN message LIKE 'Time spent%'
            THEN TRIM(COALESCE(
                regexp_extract(message, 'Time spent\\s+([^\\s]+\\s+[^\\s]+)', 1),
                regexp_extract(message, 'Time spent\\s+([^\\s]+)', 1)
            ))
            ELSE NULL
        END
    ) AS extracted_label,
    COUNT(*) AS message_count,
    AVG(COALESCE(long_op_value, timing_real_value)) AS avg_duration,
    MAX(COALESCE(long_op_value, timing_real_value)) AS max_duration,
    MIN(COALESCE(long_op_value, timing_real_value)) AS min_duration
FROM '*.parquet'
WHERE (long_op_value IS NOT NULL OR timing_real_value IS NOT NULL)
  AND (message LIKE '%took a long%' OR message LIKE 'Time spent%')
GROUP BY extracted_label
HAVING extracted_label IS NOT NULL
ORDER BY message_count DESC;
