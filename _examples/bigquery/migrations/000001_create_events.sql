CREATE TABLE `${PROJECT_ID}.${DATASET_ID}.events` (
    event_id STRING NOT NULL,
    event_type STRING NOT NULL,
    created_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(created_at)
CLUSTER BY event_type;
