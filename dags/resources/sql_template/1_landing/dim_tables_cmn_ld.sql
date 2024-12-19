DECLARE max_ts TIMESTAMP;
SET max_ts = TIMESTAMP('{{ task_instance.xcom_pull(task_ids="landing_layer.get_max_timestamp", key="max_timestamp") }}');

-- Tạo bảng nếu chưa tồn tại
CREATE TABLE IF NOT EXISTS `{{ params.my_project }}.{{ params.my_dataset }}.{{ params.my_table_name }}` (
    {{ params.schema_columns }},
    rundate STRING,
    insert_dt TIMESTAMP
);

-- Xóa dữ liệu nếu max_timestamp là giá trị mặc định
IF max_ts = TIMESTAMP('1900-01-01 00:00:00') THEN
    TRUNCATE TABLE `{{ params.my_project }}.{{ params.my_dataset }}.{{ params.my_table_name }}`;
END IF;

-- Chèn dữ liệu mới
INSERT INTO `{{ params.my_project }}.{{ params.my_dataset }}.{{ params.my_table_name }}`
SELECT 
    *,
    '{{ task_instance.xcom_pull(task_ids="get_rundate", key="rundate") }}' AS rundate,
    CURRENT_TIMESTAMP() AS insert_dt
FROM 
    `{{ params.my_project }}.{{ params.my_dataset }}.{{ params.source_table }}`;

DROP TABLE IF EXISTS `{{ params.my_project }}.{{ params.my_dataset }}.{{ params.source_table }}`;
