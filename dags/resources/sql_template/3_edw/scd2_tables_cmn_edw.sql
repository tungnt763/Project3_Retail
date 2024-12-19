-- Technique: SCD2
-- Mode: Over write
-- NK: airport_id, airport_code, airport_seq_id
-- PK: row_wid
DECLARE max_timestamp TIMESTAMP;

-- Create the target table if it does not exist
CREATE TABLE IF NOT EXISTS `{{ params.my_project }}.{{ params.my_dataset }}.{{ params.my_table_name }}` (
    row_wid STRING,  -- UUID for the record
    {{ params.create_typed_cols }},
    effective_start_dt TIMESTAMP,
    effective_end_dt TIMESTAMP,
    active_flg INT64,
    rundate STRING,
    insert_dt TIMESTAMP,
    update_dt TIMESTAMP,

);

-- Validate load type: FULL LOAD/INCREMENTAL LOAD
SET max_timestamp = TIMESTAMP('{{ task_instance.xcom_pull(task_ids="edw_layer.get_max_timestamp", key="max_timestamp") }}');

-- Truncate the table if the max timestamp is the default value
IF max_timestamp = TIMESTAMP('1900-01-01 00:00:00') THEN
    TRUNCATE TABLE `{{ params.my_project }}.{{ params.my_dataset }}.{{ params.my_table_name }}`;
END IF;

-- Create a temporary table to hold the staging data
CREATE TEMP TABLE table_stg AS 
SELECT
    GENERATE_UUID() AS row_wid,  -- Generate UUID for each record
    {{ params.select_cols }},
    effective_start_dt,
    effective_end_dt,
    active_flg,
    rundate,
    insert_date,
    update_date
FROM
    `{{ params.my_project }}.{{ params.my_serv_dataset }}.{{ params.serv_table_name }}`;

-- Merge the data from staging into the target table
MERGE INTO `{{ params.my_project }}.{{ params.my_dataset }}.{{ params.my_table_name }}` AS tgt
USING table_stg AS src
ON 
    -- tgt.airport_id = src.airport_id  -- Define the natural key condition
    {{ params.nk_4_condition }}
AND    
    tgt.active_flg = 1
WHEN MATCHED THEN
    UPDATE SET
        tgt.update_dt = src.update_dt,
        tgt.active_flg = 0,
        tgt.effective_end_dt = src.effective_start_dt - INTERVAL 1 SECOND;

-- Insert new records into the target table
INSERT INTO `{{ params.my_project }}.{{ params.my_dataset }}.{{ params.my_table_name }}`
SELECT * FROM table_stg;