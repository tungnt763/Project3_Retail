CREATE TABLE IF NOT EXISTS {{ params.my_project }}.{{ params.my_dataset }}.{{ params.my_table_name }} (
    {{ params.schema_columns }},
    -- rundate VARCHAR,
    insert_dt 
);

TRUNCATE TABLE {{ params.my_project }}.{{ params.my_dataset }}.{{ params.my_table_name }};

INSERT INTO {{ params.my_project }}.{{ params.my_dataset }}.{{ params.my_table_name }} 
({{ params.columns }})
VALUES ({{ params.values }});
    