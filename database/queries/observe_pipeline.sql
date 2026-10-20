-- Recent audit runs
SELECT run_id, pipeline_name, status, started_at, finished_at,
       ROUND(EXTRACT(EPOCH FROM (finished_at-started_at))::numeric,2) AS duration_seconds,
       checks_total, checks_failed, error_message
FROM etl.pipeline_runs ORDER BY run_id DESC LIMIT 10;

-- Latest check details
SELECT r.run_id, q.check_name, q.observed_value, q.threshold, q.passed, q.severity
FROM etl.pipeline_runs r
JOIN etl.data_quality_results q USING (run_id)
WHERE r.run_id = (SELECT max(run_id) FROM etl.pipeline_runs)
ORDER BY q.passed, q.check_name;

-- Queue backlog and age, including unprocessed changes
SELECT COUNT(*) AS pending_events,
       ROUND(EXTRACT(EPOCH FROM (clock_timestamp()-MIN(recorded_at)))::numeric,1)
          AS oldest_pending_age_seconds
FROM etl.order_changes WHERE processed_at IS NULL;
