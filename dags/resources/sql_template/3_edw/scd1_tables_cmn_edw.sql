DECLARE max_timestamp TIMESTAMP;

CREATE TABLE IF NOT EXISTS `{{ params.my_project }}.{{ params.my_dataset }}.{{ params.my_table_name }}` (
    row_wid STRING,
    -- Create columns with (which?) type for each column
    {{ params.create_typed_cols }},
    -- Audit cols base on SCD2
    rundate STRING,
    insert_dt TIMESTAMP,
    update_dt TIMESTAMP
);

-- Fetch the maximum timestamp from the XCom variable
SET max_timestamp = TIMESTAMP('{{ task_instance.xcom_pull(task_ids="edw_layer.get_max_timestamp", key="max_timestamp") }}');

-- Truncate the table if the max timestamp is the default value
IF max_timestamp = TIMESTAMP('1900-01-01 00:00:00') THEN
    TRUNCATE TABLE `{{ params.my_project }}.{{ params.my_dataset }}.{{ params.my_table_name }}`;
END IF;

-- Create a temporary table with staged data
CREATE TEMP TABLE table_stg AS
SELECT 
    GENERATE_UUID() AS row_wid,
    {{ params.select_cols }},
    rundate,
    insert_dt,
    update_dt
FROM 
    `{{ params.my_project }}.{{ params.my_serv_dataset }}.{{ params.serv_table_name }}`;

-- Merge the staged data into the target table
MERGE `{{ params.my_project }}.{{ params.my_dataset }}.{{ params.my_table_name }}` AS tgt
USING table_stg AS src
ON 
    {{ params.nk_4_condition }}
WHEN MATCHED THEN 
    UPDATE SET
        {{ params.lnk_4_update }},
        update_dt = src.update_dt
WHEN NOT MATCHED THEN
    INSERT (row_wid, {{ params.ncols_4_insert }}, rundate, insert_dt, update_dt)
    VALUES (src.row_wid, {{ params.vcols_4_insert }}, src.rundate, src.insert_dt, src.update_dt);
