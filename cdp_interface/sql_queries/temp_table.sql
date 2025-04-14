CREATE TABLE IF NOT EXISTS @schema.@temp_table (
    product_number STRING,
    formula_code STRING,
    product_name STRING,
    product_form STRING,
    unit_weight STRING,
    pallet_quantity DOUBLE,
    stocking_status STRING,
    min_order_quantity DOUBLE,
    days_lead_time DOUBLE,
    fob_or_dlv STRING,
    price_change DOUBLE,
    list_price DOUBLE,
    full_pallet_price DOUBLE,
    half_load_full_pallet_price DOUBLE,
    full_load_full_pallet_price DOUBLE,
    full_load_best_price DOUBLE,
    plant_location STRING,
    date_inserted STRING,
    source STRING
)
STORED AS PARQUET
LOCATION "@hdfs_root_folder/@temp_table"