DECLARE max_timestamp TIMESTAMP;

-- Create fact table if not exists
CREATE TABLE IF NOT EXISTS `{{ params.my_project }}.{{ params.my_dataset }}.fact_sales_edw` (
    order_date TIMESTAMP,
    product_wid STRING,
    category_wid STRING,
    department_wid STRING,
    customer_wid STRING,
    order_id INT64,

    order_status STRING,
    qty FLOAT64,
    revenue FLOAT64,
    is_confirm_revenue INT64,
    integration_key STRING,
    version INT64,

    rundate STRING,
    insert_dt TIMESTAMP,
    update_dt TIMESTAMP
);

-- Get max timestamp from job control
SET max_timestamp = TIMESTAMP('{{ task_instance.xcom_pull(task_ids="edw_layer.get_max_timestamp", key="max_timestamp") }}');

-- Truncate if full load
IF max_timestamp = TIMESTAMP('1900-01-01 00:00:00') THEN
    TRUNCATE TABLE `{{ params.my_project }}.{{ params.my_dataset }}.fact_sales_edw`;
END IF;

-- Create temp staging table
CREATE OR REPLACE TEMP TABLE fact_sale_tmp AS
WITH fact_sale_with_dprd AS (
    SELECT * 
    FROM `{{ params.my_project }}.{{ params.my_serv_dataset }}.fact_sales_stg`
),
-- Join with SCD1 dimension tables
dim_cat AS (
    SELECT 
        dc.category_id,
        dc.row_wid AS category_wid,
        dc.category_department_id AS department_id
    FROM 
        `{{ params.my_project }}.{{ params.my_dataset }}.dim_categories` dc 
),
dim_dep AS (
    SELECT 
        dd.department_id,
        dd.row_wid AS department_wid
    FROM 
        `{{ params.my_project }}.{{ params.my_dataset }}.dim_departments` dd  
),
-- Join with SCD2 dimension table
dim_cus AS (
    SELECT 
        dc.customer_id,
        dc.row_wid AS customer_wid
    FROM 
        `{{ params.my_project }}.{{ params.my_dataset }}.dim_customers` dc
    WHERE
        dc.active_flg = 1
)
SELECT
    sale_prd.order_date,
    sale_prd.product_wid,
    dc.category_wid,
    dd.department_wid,
    dcu.customer_wid,
    sale_prd.order_id,
    sale_prd.order_status,
    sale_prd.qty,
    sale_prd.revenue,
    sale_prd.is_confirm_revenue,
    sale_prd.integration_key,
    COALESCE(
        (SELECT MAX(version) 
            FROM `{{ params.my_project }}.{{ params.my_dataset }}.fact_sales_edw` f 
            WHERE f.integration_key = sale_prd.integration_key
        ), 0) + 1 AS version,
    sale_prd.rundate,
    sale_prd.insert_dt,
    sale_prd.update_dt
FROM
    fact_sale_with_dprd sale_prd
LEFT JOIN dim_cat dc ON sale_prd.category_id = dc.category_id
LEFT JOIN dim_dep dd ON dd.department_id = dc.department_id
LEFT JOIN dim_cus dcu ON sale_prd.customer_id = dcu.customer_id;

-- Insert data from temp table to fact table
INSERT INTO `{{ params.my_project }}.{{ params.my_dataset }}.fact_sales_edw`
SELECT * FROM fact_sale_tmp;