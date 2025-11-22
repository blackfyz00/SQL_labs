CREATE OR REPLACE FUNCTION s_sql_dds.load_clean_orders()
RETURNS VOID AS $$
DECLARE
    c_proc_name TEXT := 'load_clean_orders';
BEGIN
    TRUNCATE s_sql_dds.t_sql_source_structured;

    INSERT INTO s_sql_dds.t_sql_source_structured (
        order_id,
        customer_id,
        order_date,
        product_category,
        product_name,
        quantity,
        price,
        total_amount,
        status,
        region
    )
    WITH cleaned AS (
        SELECT DISTINCT ON (SUBSTRING(order_id FROM 4)::BIGINT)
    		SUBSTRING(order_id FROM 4)::BIGINT AS order_id,
            customer_id,
            order_date::DATE AS order_date,
            LEFT(product_category, 50) AS product_category,
            LEFT(product_name, 50) AS product_name,
            quantity::NUMERIC::INT AS quantity,
            price::DECIMAL(10,2) AS price,
            (quantity::NUMERIC::INT * price::DECIMAL(10,2))::DECIMAL(10,2) AS total_amount,
            LEFT(status, 50) AS status,
            LEFT(region, 50) AS region
        FROM s_sql_dds.t_sql_source_unstructured
        WHERE
            -- Обязательные поля не NULL
            customer_id IS NOT NULL
            AND product_name IS NOT NULL
            AND region IS NOT NULL
            AND order_id IS NOT NULL
            AND order_date IS NOT NULL
            AND quantity IS NOT NULL
            AND price IS NOT NULL
            AND status IS NOT NULL
            AND product_category IS NOT NULL

            -- Валидация и преобразуемость order_date
			AND order_date ~ '^\d{4}-\d{2}-\d{2}$'
			AND SUBSTRING(order_date, 1, 4)::INT BETWEEN 2020 AND 2025
			AND SUBSTRING(order_date, 6, 2)::INT BETWEEN 1 AND 12
			AND SUBSTRING(order_date, 9, 2)::INT BETWEEN 1 AND 31

            -- Валидация quantity
            AND quantity ~ '^\d+(\.\d*)?$'
			AND quantity::NUMERIC > 0
			AND quantity::NUMERIC = TRUNC(quantity::NUMERIC)

            -- Валидация price
            AND price ~ '^\d+(\.\d+)?$'
            AND price::NUMERIC > 0

            -- Валидация статуса и категории
            AND product_category IN ('Электроника', 'Одежда', 'Книги', 'Бытовая техника', 'Спорт')
        ORDER BY SUBSTRING(order_id FROM 4)::BIGINT, ctid
    )
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
        region
    FROM cleaned;

    RAISE NOTICE 'Загружено % строк в t_sql_source_structured', 
        (SELECT COUNT(*) FROM s_sql_dds.t_sql_source_structured);
END;
$$ LANGUAGE plpgsql;