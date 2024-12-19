-- Drop the table if it exists
DROP TABLE IF EXISTS `{{ params.my_project }}.{{ params.my_dataset }}.{{ params.my_table_name }}`;

-- Create the final table with deduplicated and transformed data
CREATE TABLE `{{ params.my_project }}.{{ params.my_dataset }}.{{ params.my_table_name }}` AS
WITH table_ld AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY {{ params.nk }} ORDER BY insert_dt DESC) AS _rnk
    FROM 
        `{{ params.my_project }}.{{ params.my_serv_dataset }}.{{ params.serv_table_name }}`
    WHERE 
        insert_dt > TIMESTAMP('{{ task_instance.xcom_pull(task_ids="staging_layer.get_max_timestamp", key="max_timestamp") }}')
)
SELECT 
    {{ params.cast_typed_cols }},
    CURRENT_TIMESTAMP() AS effective_start_dt,
    TIMESTAMP('9999-12-31 23:59:59') AS effective_end_dt,
    1 AS active_flg,
    rundate,
    CURRENT_TIMESTAMP() AS insert_date,
    CURRENT_TIMESTAMP() AS update_date
FROM 
    table_ld
WHERE 
    _rnk = 1;
