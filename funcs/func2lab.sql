CREATE OR REPLACE PROCEDURE create_category_dictionaries()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Справочник product_category
    CREATE TABLE IF NOT EXISTS dim_product_category (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL
    );

    INSERT INTO dim_product_category (name)
    SELECT DISTINCT product_category
    FROM s_sql_dds.t_sql_source_structured
    WHERE product_category IS NOT NULL
      AND TRIM(product_category) != ''
    ON CONFLICT (name) DO NOTHING;

    -- Справочник status
    CREATE TABLE IF NOT EXISTS dim_status (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL
    );

    INSERT INTO dim_status (name)
    SELECT DISTINCT status
    FROM s_sql_dds.t_sql_source_structured
    WHERE status IS NOT NULL
      AND TRIM(status) != ''
    ON CONFLICT (name) DO NOTHING;

    -- Справочник region
    CREATE TABLE IF NOT EXISTS dim_region (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL
    );

    INSERT INTO dim_region (name)
    SELECT DISTINCT region
    FROM s_sql_dds.t_sql_source_structured
    WHERE region IS NOT NULL
      AND TRIM(region) != ''
    ON CONFLICT (name) DO NOTHING;

    RAISE NOTICE 'Справочники успешно созданы и заполнены.';
END;
$$;