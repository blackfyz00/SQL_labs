-- sql/dds/s_sql_dds/table/t_sql_source_unstructured.sql
CREATE SCHEMA IF NOT EXISTS s_sql_dds;

CREATE TABLE IF NOT EXISTS s_sql_dds.t_sql_source_unstructured (
    order_id VARCHAR(100),
    customer_id VARCHAR(100),
    order_date VARCHAR(100)), -- Храним как VARCHAR, так как даты могут быть некорректными
    product_category VARCHAR(100),
    product_name VARCHAR(100),
    quantity VARCHAR(100), -- Используем VARCHAR(100), чтобы учесть возможные NULL и некорректные значения
    price VARCHAR(100),
    total_amount VARCHAR(100),
    status VARCHAR(100),
    region VARCHAR(100)
);