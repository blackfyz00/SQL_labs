DECLARE
    c_proc_name TEXT := 'fn_dm_data_load';
    v_rows_inserted BIGINT;
BEGIN
    RAISE NOTICE 'Начало загрузки DM-данных за период % - %', start_dt, end_dt;

    -- Удаляем старые данные за указанный период (если нужно перезагружать)
    -- Если нужно только добавлять — закомментировать
    -- DELETE FROM s_sql_dds.t_dm_task WHERE order_date BETWEEN start_dt AND end_dt;

    INSERT INTO s_sql_dds.t_dm_task (
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
    )
    SELECT
        s.order_id,
        s.customer_id,
        s.order_date,
        s.product_category,
        s.product_name,
        s.quantity,
        s.price,
        s.total_amount,
        s.status,
        s.region,

        -- Джойним справочники
        pc.id AS product_category_id,
        pn.id AS product_name_id,
        q.id AS quantity_id,
        r.id AS region_id,
        st.id AS status_id

    FROM s_sql_dds.t_sql_source_structured s

    -- LEFT JOIN на справочники (чтобы не потерять строки, если нет соответствия)
    LEFT JOIN s_sql_dds.dim_product_category pc ON pc.name = s.product_category
    LEFT JOIN s_sql_dds.dim_product_name pn ON pn.name = s.product_name
    LEFT JOIN s_sql_dds.dim_quantity q ON q.name = s.quantity::TEXT  -- преобразуем INT в TEXT для джойна
    LEFT JOIN s_sql_dds.dim_region r ON r.name = s.region
    LEFT JOIN s_sql_dds.dim_status st ON st.name = s.status

    WHERE s.order_date BETWEEN start_dt AND end_dt;

    -- Получаем количество вставленных строк
    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

    RAISE NOTICE 'Загружено % строк в t_dm_task за период % - %', 
        v_rows_inserted, start_dt, end_dt;

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Ошибка при выполнении %: %', c_proc_name, SQLERRM;
END;
