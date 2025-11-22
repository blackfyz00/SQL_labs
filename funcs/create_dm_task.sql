CREATE TABLE IF NOT EXISTS s_sql_dds.t_dm_task (
    order_id BIGINT,
    customer_id TEXT,
    order_date DATE,
    product_category TEXT,
    product_name TEXT,
    quantity INT,
    price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    status TEXT,
    region TEXT,

    -- ID-поля из справочников (тип INT или BIGINT — выбираем INT, если данных не миллионы)
    product_category_id INT,
    product_name_id INT,
    quantity_id INT,
    region_id INT,
    status_id INT
);