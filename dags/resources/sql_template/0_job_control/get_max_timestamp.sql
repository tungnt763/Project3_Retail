SELECT 
    COALESCE(MAX(max_timestamp), TIMESTAMP('1900-01-01 00:00:00.000000')) AS max_timestamp
FROM 
    `festive-ellipse-441310-b5.edw.job_control`
WHERE 
    schema_name = '{_dataset_name}' AND 
    table_name = '{_table_name}';
