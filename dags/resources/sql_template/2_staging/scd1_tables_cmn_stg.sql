-- Technique: SCD1
-- Mode: Over write
-- NK: {{ params.nk }}

DROP TABLE IF EXISTS `{{ params.my_project }}.{{ params.my_dataset }}.{{ params.my_table_name }}`;

CREATE TABLE `{{ params.my_project }}.{{ params.my_dataset }}.{{ params.my_table_name }}` AS
WITH table_ld AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY {{ params.nk }} ORDER BY insert_dt DESC) AS _rnk        -- Natural key for pationtion
    FROM
        `{{ params.my_project }}.{{ params.my_serv_dataset }}.{{ params.serv_table_name }}`
    WHERE
        insert_dt > CAST('{{ task_instance.xcom_pull(task_ids="staging_layer.get_max_timestamp", key="max_timestamp") }}' AS TIMESTAMP)

)
SELECT 
    -- Casting string columns -> typed columns base on each table
    {{ params.cast_typed_cols }},
    -- Audit cols base on SCD1
    rundate,
    CURRENT_TIMESTAMP() AS insert_dt,
    CURRENT_TIMESTAMP() AS update_dt
FROM table_ld
WHERE _rnk = 1;
