CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET_ID}.events_summary` AS
SELECT
    DATE(created_at) AS event_date,
    event_type,
    COUNT(*) AS event_count
FROM `${PROJECT_ID}.${DATASET_ID}.events`
GROUP BY 1, 2;
