# YugabyteDB Log Analyzer API Endpoints

## 1. Get Report JSON

- **Endpoint:** `/api/reports/<uuid>`
- **Method:** GET
- **Description:** Returns the JSON report for the given report UUID.
- **Example:**
  ```bash
  curl http://localhost:5000/api/reports/123e4567-e89b-12d3-a456-426614174000
  ```

## 2. Get Histogram Data

- **Endpoint:** `/api/histogram/<report_id>`
- **Method:** GET
- **Description:** Returns histogram data for a report. Supports optional query parameters:
  - `interval` (minutes)
  - `start` (ISO datetime)
  - `end` (ISO datetime)
- **Example:**
  ```bash
  curl "http://localhost:5000/api/histogram/123e4567-e89b-12d3-a456-426614174000?interval=5&start=2025-07-09T00:00:00&end=2025-07-09T12:00:00"
  ```

## 3. Get GFlags Data

- **Endpoint:** `/api/gflags/<uuid>`
- **Method:** GET
- **Description:** Returns GFlags data for the given report UUID.
- **Example:**
  ```bash
  curl http://localhost:5000/api/gflags/123e4567-e89b-12d3-a456-426614174000
  ```

## 4. Get Related Reports

- **Endpoint:** `/api/related_reports/<uuid>`
- **Method:** GET
- **Description:** Returns a list of related reports (same org or cluster) for the given report UUID.
- **Example:**
  ```bash
  curl http://localhost:5000/api/related_reports/123e4567-e89b-12d3-a456-426614174000
  ```

## 5. Search Reports

- **Endpoint:** `/api/search_reports`
- **Method:** GET
- **Description:** Returns a list of reports matching a search query (by id, bundle, cluster, org, or case ID).
- **Query Parameter:** `q` (search string)
- **Example:**
  ```bash
  curl "http://localhost:5000/api/search_reports?q=cluster-123"
  ```

## 6. Get Uploaded Data

- **Endpoint:** `/data`
- **Method:** GET
- **Description:** Returns uploaded data (used for in-memory uploads, not DB).
- **Example:**
  ```bash
  curl http://localhost:5000/data
  ```

## 7. Upload a New Report

- **Endpoint:** `/upload`
- **Method:** POST
- **Description:** Accepts a JSON payload to upload a new report.
- **Example:**
  ```bash
  curl -X POST -H "Content-Type: application/json" -d @report.json http://localhost:5000/upload
  ```

Replace the UUIDs and parameters with actual values as needed.
