CREATE OR REPLACE VIEW s_sql_dds.v_dm_task AS
SELECT
    order_id,
    customer_id,
    order_date,
    product_category,
    product_name,
    quantity,
    price,
    total_amount,
    status,
    region,
    product_category_id,
    product_name_id,
    quantity_id,
    region_id,
    status_id
FROM s_sql_dds.t_dm_task;