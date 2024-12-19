DECLARE max_ts_orders TIMESTAMP;
DECLARE max_ts_order_items TIMESTAMP;

SET max_ts_orders = TIMESTAMP('{{ task_instance.xcom_pull(task_ids="landing_layer.get_max_timestamp_orders", key="max_timestamp_orders") }}');
SET max_ts_order_items = TIMESTAMP('{{ task_instance.xcom_pull(task_ids="landing_layer.get_max_timestamp_order_items", key="max_timestamp_order_items") }}');

-- Create orders landing table if not exists
CREATE TABLE IF NOT EXISTS `{{ params.my_project }}.{{ params.my_dataset }}.orders_ld` (
    order_id STRING,
    order_date STRING,
    order_customer_id STRING,
    order_status STRING,

    rundate STRING,
    insert_dt TIMESTAMP
);

-- Create order_items landing table if not exists
CREATE TABLE IF NOT EXISTS `{{ params.my_project }}.{{ params.my_dataset }}.order_items_ld` (
    order_item_id STRING,
    order_item_order_id STRING,
    order_item_product_id STRING,
    order_item_quantity STRING,
    order_item_subtotal STRING,
    order_item_product_price STRING,

    rundate STRING,
    insert_dt TIMESTAMP
);

-- Handle orders data
IF max_ts_orders = TIMESTAMP('1900-01-01 00:00:00') THEN
    TRUNCATE TABLE `{{ params.my_project }}.{{ params.my_dataset }}.orders_ld`;
END IF;

INSERT INTO `{{ params.my_project }}.{{ params.my_dataset }}.orders_ld`
SELECT
    *,
    '{{ task_instance.xcom_pull(task_ids="get_rundate", key="rundate") }}' AS rundate,
    CURRENT_TIMESTAMP() AS insert_dt
FROM 
    `{{ params.my_project }}.{{ params.my_dataset }}.orders_ld_temp`;

DROP TABLE IF EXISTS `{{ params.my_project }}.{{ params.my_dataset }}.orders_ld_temp`;

-- Handle order_items data
IF max_ts_order_items = TIMESTAMP('1900-01-01 00:00:00') THEN
    TRUNCATE TABLE `{{ params.my_project }}.{{ params.my_dataset }}.order_items_ld`;
END IF;

INSERT INTO `{{ params.my_project }}.{{ params.my_dataset }}.order_items_ld`
SELECT
    *,
    '{{ task_instance.xcom_pull(task_ids="get_rundate", key="rundate") }}' AS rundate,
    CURRENT_TIMESTAMP() AS insert_dt
FROM 
    `{{ params.my_project }}.{{ params.my_dataset }}.order_items_ld_temp`;

DROP TABLE IF EXISTS `{{ params.my_project }}.{{ params.my_dataset }}.order_items_ld_temp`;


