DECLARE max_ts TIMESTAMP;
SET max_ts = TIMESTAMP('{{ task_instance.xcom_pull(task_ids="staging_layer.get_max_timestamp", key="max_timestamp") }}');

-- Drop and recreate staging table
DROP TABLE IF EXISTS `{{ params.my_project }}.{{ params.my_dataset }}.fact_sales_stg`;

CREATE TABLE `{{ params.my_project }}.{{ params.my_dataset }}.fact_sales_stg` AS
WITH order_infor AS (
    SELECT 
        CAST(o.order_id AS INT64) AS order_id,
        CAST(o.order_date AS TIMESTAMP) AS order_date,
        o.order_status,
        CAST(o.order_customer_id AS INT64) AS order_customer_id,
        CAST(oi.order_item_product_id AS INT64) AS order_item_product_id,
        CAST(oi.order_item_quantity AS FLOAT64) AS order_item_quantity,
        CAST(oi.order_item_product_price AS FLOAT64) AS item_price,
        o.rundate,
        -- Thêm rank để xử lý duplicate
        ROW_NUMBER() OVER (
            PARTITION BY 
                o.order_id,
                o.order_customer_id,
                oi.order_item_product_id,
                o.order_date
            ORDER BY
                -- Ưu tiên theo status
                CASE o.order_status 
                    WHEN 'COMPLETE' THEN 1
                    WHEN 'CLOSED' THEN 2
                    WHEN 'PROCESSING' THEN 3
                    WHEN 'PENDING' THEN 4
                    WHEN 'PENDING_PAYMENT' THEN 5
                    WHEN 'PAYMENT_REVIEW' THEN 6
                    WHEN 'ON_HOLD' THEN 7
                    WHEN 'CANCELED' THEN 8
                    WHEN 'SUSPECTED_FRAUD' THEN 9
                    ELSE 10
                END,
                -- Nếu cùng status thì lấy số lượng lớn nhất
                oi.order_item_quantity DESC,
                -- Nếu vẫn bằng nhau thì lấy bản ghi mới nhất
                o.insert_dt DESC
        ) as row_num
    FROM
        `{{ params.my_project }}.{{ params.my_serv_dataset }}.orders_ld` o 
    JOIN 
        `{{ params.my_project }}.{{ params.my_serv_dataset }}.order_items_ld` oi 
    ON 
        o.order_id = oi.order_item_order_id
    WHERE
        o.insert_dt > max_ts
    AND    
        oi.insert_dt > max_ts
),
mer_prd AS (
    SELECT 
        oin.*,
        COALESCE(
            oin.order_item_quantity * CAST(dp.product_price AS FLOAT64),
            oin.order_item_quantity * oin.item_price
        ) AS revenue,
        CASE
            WHEN oin.order_status IN ('CLOSED', 'COMPLETE') THEN 1
            ELSE 0
        END AS is_confirm_revenue,
        dp.product_category_id AS category_id,
        CONCAT(
            CAST(oin.order_id AS STRING), '||~||',
            CAST(oin.order_item_product_id AS STRING), '||~||',
            CAST(oin.order_customer_id AS STRING), '||~||',
            CAST(oin.order_date AS STRING)
        ) as integration_key,
        dp.row_wid AS product_wid
    FROM 
        order_infor oin
    LEFT JOIN 
        `{{ params.my_project }}.edw.dim_products` dp 
    ON
        dp.product_id = oin.order_item_product_id
    AND 
        dp.active_flg = 1
    WHERE
        oin.row_num = 1  -- Chỉ lấy bản ghi được chọn
)
SELECT
    order_id,
    order_date,
    category_id,
    order_customer_id AS customer_id,
    order_item_product_id AS product_id,
    product_wid,
    order_status,
    order_item_quantity AS qty,
    revenue,
    is_confirm_revenue,
    integration_key,
    
    rundate,
    CURRENT_TIMESTAMP() AS insert_dt,
    CURRENT_TIMESTAMP() AS update_dt
FROM
    mer_prd;