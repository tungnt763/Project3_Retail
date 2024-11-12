CREATE TABLE IF NOT EXISTS {my_dataset}.{my_table_name} (
    {schema_columns},
    -- rundate VARCHAR,
    insert_dt TIMESTAMP
);

-- TRUNCATE TABLE {my_dataset}.{my_table_name};

INSERT INTO {my_dataset}.{my_table_name} ({columns})
VALUES %s;
    