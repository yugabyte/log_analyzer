import os
import duckdb
import json
import re
from collections import defaultdict
from datetime import datetime
import glob
import time

PROCESS_TYPES = ['postgres', 'tserver', 'controller', 'master']

def analyzeParquetFiles(parquet_dir, log_patterns, output_path=None):
    start_total = time.time()
    parquet_path = os.path.join(parquet_dir, "*.parquet")
    patterns_list = list(log_patterns)
    pattern_name_map = {p: p for p in patterns_list}
    # Build WHERE clause using REGEXP_MATCHES for all patterns
    pattern_filters = []
    for pattern in patterns_list:
        safe_pattern = pattern.replace("'", "''").replace("\\", "\\\\")
        pattern_filters.append(f"REGEXP_MATCHES(message, '(?i){safe_pattern}')")
    where_clause = "node_name IS NOT NULL AND (" + " OR ".join(pattern_filters) + ")"
    sql = f"""
        SELECT node_name, log_type, timestamp, message
        FROM '{parquet_path}'
        WHERE {where_clause}
    """

    print("[LOG] Executing DuckDB query with REGEXP_MATCHES pattern filtering...")
    start_duckdb = time.time()
    con = duckdb.connect()
    rows = con.execute(sql).fetchall()
    con.close()
    print(f"[LOG] DuckDB query completed in {time.time() - start_duckdb:.2f} seconds. Rows fetched: {len(rows)}")
    nodes = set(row[0] for row in rows)
    start_pattern = time.time()
    node_proc_pat = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for idx, (node_name, log_type, timestamp, message) in enumerate(rows):
        if idx % 10000 == 0 and idx > 0:
            print(f"[LOG] Processed {idx} rows for pattern matching...")
        for pattern in patterns_list:
            if re.search(pattern, message):
                pattern_name = pattern_name_map.get(pattern, pattern)
                node_proc_pat[node_name][log_type][pattern_name].append(timestamp)
    print(f"[LOG] Pattern matching completed in {time.time() - start_pattern:.2f} seconds.")
    start_json = time.time()
    result = {"nodes": {}}
    for node_name in node_proc_pat:
        result["nodes"][node_name] = {}
        for proc in PROCESS_TYPES:
            log_messages = {}
            proc_data = node_proc_pat[node_name].get(proc, {})
            for pattern_name, timestamps in proc_data.items():
                if timestamps:
                    times = sorted(timestamps)
                    start_time = times[0]
                    end_time = times[-1]
                    total_count = len(times)
                    histogram = defaultdict(int)
                    for t in times:
                        if isinstance(t, (datetime)):
                            minute = t.replace(second=0, microsecond=0)
                            minute_str = minute.strftime('%Y-%m-%dT%H:%M:00Z')
                        else:
                            dt = datetime.strptime(str(t)[:19], "%Y-%m-%d %H:%M:%S")
                            minute = dt.replace(second=0, microsecond=0)
                            minute_str = minute.strftime('%Y-%m-%dT%H:%M:00Z')
                        histogram[minute_str] += 1
                    def iso(dt):
                        return dt.strftime('%Y-%m-%dT%H:%M:%SZ') if isinstance(dt, datetime) else str(dt)[:19].replace(' ', 'T') + 'Z'
                    log_messages[pattern_name] = {
                        "StartTime": iso(start_time),
                        "EndTime": iso(end_time),
                        "count": total_count,
                        "histogram": dict(histogram)
                    }
            result["nodes"][node_name][proc] = {"logMessages": log_messages}
    print(f"[LOG] JSON assembly completed in {time.time() - start_json:.2f} seconds.")
    if output_path is None:
        output_path = os.path.join(os.getcwd(), "node_log_summary.json")
    start_write = time.time()
    try:
        with open(output_path, "w") as f:
            json.dump(result, f, indent=2)
        print(f"[INFO] node_log_summary.json generated at: {output_path}")
    except Exception as e:
        print(f"[ERROR] Failed to write output file: {e}")
    print(f"[LOG] Writing JSON file took {time.time() - start_write:.2f} seconds.")
    print(f"[LOG] Total execution time: {time.time() - start_total:.2f} seconds.")
    return result