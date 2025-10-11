CREATE TABLE IF NOT EXISTS s_sql_dds.t_sql_source_structured (
    order_id BIGINT,
    customer_id VARCHAR(100),
    order_date DATE, -- Храним как VARCHAR, так как даты могут быть некорректными
    product_category VARCHAR(50),
    product_name VARCHAR(50),
    quantity INT,
    price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    status VARCHAR(50),
    region VARCHAR(50)
);