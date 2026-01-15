--
-- PostgreSQL database dump
--
\restrict WZHwvb0KRQ1SS2qdqVMCXQw9K2Z8bun2HUNPDNOadRdfRaGpFDFwbzKbtbHDAu9
-- Dumped from database version 18.1
-- Dumped by pg_dump version 18.1
SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;
--
-- Name: s_sql_dds; Type: SCHEMA; Schema: -; Owner: postgres
--
CREATE SCHEMA s_sql_dds;

ALTER SCHEMA s_sql_dds OWNER TO postgres;
--
-- Name: create_category_dictionaries(); Type: FUNCTION; Schema: s_sql_dds; Owner: postgres
--
CREATE FUNCTION s_sql_dds.create_category_dictionaries() RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Справочник product_category
    CREATE TABLE IF NOT EXISTS s_sql_dds.dim_product_category (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL
    );

    INSERT INTO s_sql_dds.dim_product_category (name)
    SELECT DISTINCT product_category
    FROM s_sql_dds.t_sql_source_structured
    WHERE product_category IS NOT NULL
      AND TRIM(product_category) != ''
    ON CONFLICT (name) DO NOTHING;

    -- Справочник status
    CREATE TABLE IF NOT EXISTS s_sql_dds.dim_status (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL
    );

    INSERT INTO s_sql_dds.dim_status (name)
    SELECT DISTINCT status
    FROM s_sql_dds.t_sql_source_structured
    WHERE status IS NOT NULL
      AND TRIM(status) != ''
    ON CONFLICT (name) DO NOTHING;

    -- Справочник region
    CREATE TABLE IF NOT EXISTS s_sql_dds.dim_region (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL
    );

    INSERT INTO s_sql_dds.dim_region (name)
    SELECT DISTINCT region
    FROM s_sql_dds.t_sql_source_structured
    WHERE region IS NOT NULL
      AND TRIM(region) != ''
    ON CONFLICT (name) DO NOTHING;

	-- Справочник name
    CREATE TABLE IF NOT EXISTS s_sql_dds.dim_product_name (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL
    );

    INSERT INTO s_sql_dds.dim_product_name (name)
    SELECT DISTINCT product_name
    FROM s_sql_dds.t_sql_source_structured
    WHERE product_name IS NOT NULL
      AND TRIM(region) != ''
    ON CONFLICT (name) DO NOTHING;

	CREATE TABLE IF NOT EXISTS s_sql_dds.dim_quantity (
    id SERIAL PRIMARY KEY,
    quantity NUMERIC UNIQUE NOT NULL
	);
	
	INSERT INTO s_sql_dds.dim_quantity (quantity)
	SELECT DISTINCT quantity
	FROM s_sql_dds.t_sql_source_structured
	WHERE quantity IS NOT NULL;  -- ← обязательно, чтобы избежать нарушения NOT NULL

	-- Таблица связи между продуктом и категорией
	CREATE TABLE IF NOT EXISTS s_sql_dds.t_name_category (
    id SERIAL PRIMARY KEY,
    product_name_id INT NOT NULL REFERENCES s_sql_dds.dim_product_name(id),
    product_category_id INT NOT NULL REFERENCES s_sql_dds.dim_product_category(id),
    UNIQUE (product_name_id, product_category_id)
	);

	INSERT INTO s_sql_dds.t_name_category (product_name_id, product_category_id)
	SELECT DISTINCT
	    n.id AS product_name_id,
	    c.id AS product_category_id
	FROM s_sql_dds.t_sql_source_structured src
	JOIN s_sql_dds.dim_product_name n ON src.product_name = n.name
	JOIN s_sql_dds.dim_product_category c ON src.product_category = c.name
	WHERE src.product_name IS NOT NULL
	  AND TRIM(src.product_name) != ''
	  AND src.product_category IS NOT NULL
	  AND TRIM(src.product_category) != ''
	ON CONFLICT (product_name_id, product_category_id) DO NOTHING;

    RAISE NOTICE 'Справочники успешно созданы и заполнены.';
END;
$$;

ALTER FUNCTION s_sql_dds.create_category_dictionaries() OWNER TO postgres;
--
-- Name: fn_dm_data_load(date, date); Type: FUNCTION; Schema: s_sql_dds; Owner: postgres
--
CREATE FUNCTION s_sql_dds.fn_dm_data_load(start_dt date, end_dt date) RETURNS void
    LANGUAGE plpgsql
    AS $$DECLARE
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
    LEFT JOIN s_sql_dds.dim_quantity q ON q.quantity = s.quantity
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
$$;

ALTER FUNCTION s_sql_dds.fn_dm_data_load(start_dt date, end_dt date) OWNER TO postgres;
--
-- Name: fn_dq_checks_load(date, date); Type: FUNCTION; Schema: s_sql_dds; Owner: postgres
--
CREATE FUNCTION s_sql_dds.fn_dq_checks_load(start_dt date, end_dt date) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
  v_count INT;
BEGIN
  -- Проверка 1: Уникальность (отсутствие дубликатов order_id)
  BEGIN
    SELECT COUNT(*) INTO v_count
    FROM (
      SELECT order_id
      FROM s_sql_dds.v_dm_task
      WHERE order_date BETWEEN start_dt AND end_dt
      GROUP BY order_id
      HAVING COUNT(*) > 1
    ) AS duplicates;

    IF v_count > 0 THEN
      INSERT INTO s_sql_dds.t_dq_check_results (check_type, table_name, status, error_message)
      VALUES ('уникальность', 's_sql_dds.v_dm_task', 'failed', 'Обнаружено ' || v_count || ' дубликатов order_id');
    ELSE
      INSERT INTO s_sql_dds.t_dq_check_results (check_type, table_name, status)
      VALUES ('уникальность', 's_sql_dds.v_dm_task', 'passed');
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      INSERT INTO s_sql_dds.t_dq_check_results (check_type, table_name, status, error_message)
      VALUES ('уникальность', 's_sql_dds.v_dm_task', 'error', 'Ошибка выполнения: ' || SQLERRM);
  END;

  -- Проверка 2: Полнота (отсутствие пропусков в критических полях)
  BEGIN
    SELECT COUNT(*) INTO v_count
    FROM s_sql_dds.v_dm_task
    WHERE (order_id IS NULL OR total_amount IS NULL)
      AND order_date BETWEEN start_dt AND end_dt;

    IF v_count > 0 THEN
      INSERT INTO s_sql_dds.t_dq_check_results (check_type, table_name, status, error_message)
      VALUES ('полнота', 's_sql_dds.v_dm_task', 'failed', 'Обнаружено ' || v_count || ' записей с пропущенными критическими полями');
    ELSE
      INSERT INTO s_sql_dds.t_dq_check_results (check_type, table_name, status)
      VALUES ('полнота', 's_sql_dds.v_dm_task', 'passed');
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      INSERT INTO s_sql_dds.t_dq_check_results (check_type, table_name, status, error_message)
      VALUES ('полнота', 's_sql_dds.v_dm_task', 'error', 'Ошибка выполнения: ' || SQLERRM);
  END;

  -- Проверка 3: Валидность (статус заказа)
  BEGIN
    SELECT COUNT(*) INTO v_count
    FROM s_sql_dds.v_dm_task
    WHERE status NOT IN ('new', 'processed', 'shipped', 'cancelled', 'отменен')
      AND order_date BETWEEN start_dt AND end_dt;

    IF v_count > 0 THEN
      INSERT INTO s_sql_dds.t_dq_check_results (check_type, table_name, status, error_message)
      VALUES ('валидность', 's_sql_dds.v_dm_task', 'failed', 'Обнаружено ' || v_count || ' недопустимых статусов');
    ELSE
      INSERT INTO s_sql_dds.t_dq_check_results (check_type, table_name, status)
      VALUES ('валидность', 's_sql_dds.v_dm_task', 'passed');
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      INSERT INTO s_sql_dds.t_dq_check_results (check_type, table_name, status, error_message)
      VALUES ('валидность', 's_sql_dds.v_dm_task', 'error', 'Ошибка выполнения: ' || SQLERRM);
  END;

  -- Проверка 4: Правильность (соответствие total_amount = price * quantity)
  BEGIN
    SELECT COUNT(*) INTO v_count
    FROM s_sql_dds.v_dm_task
    WHERE total_amount != price * quantity
      AND order_date BETWEEN start_dt AND end_dt;

    IF v_count > 0 THEN
      INSERT INTO s_sql_dds.t_dq_check_results (check_type, table_name, status, error_message)
      VALUES ('правильность', 's_sql_dds.v_dm_task', 'failed', 'Обнаружено ' || v_count || ' несоответствий total_amount = price * quantity');
    ELSE
      INSERT INTO s_sql_dds.t_dq_check_results (check_type, table_name, status)
      VALUES ('правильность', 's_sql_dds.v_dm_task', 'passed');
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      INSERT INTO s_sql_dds.t_dq_check_results (check_type, table_name, status, error_message)
      VALUES ('правильность', 's_sql_dds.v_dm_task', 'error', 'Ошибка выполнения: ' || SQLERRM);
  END;

  -- Проверка 5: Непротиворечивость (для отмененных заказов total_amount = 0)
  BEGIN
    SELECT COUNT(*) INTO v_count
    FROM s_sql_dds.v_dm_task
    WHERE status = 'отменен' AND total_amount != 0
      AND order_date BETWEEN start_dt AND end_dt;

    IF v_count > 0 THEN
      INSERT INTO s_sql_dds.t_dq_check_results (check_type, table_name, status, error_message)
      VALUES ('непротиворечивость', 's_sql_dds.v_dm_task', 'failed', 'Обнаружено ' || v_count || ' отмененных заказов с ненулевой суммой');
    ELSE
      INSERT INTO s_sql_dds.t_dq_check_results (check_type, table_name, status)
      VALUES ('непротиворечивость', 's_sql_dds.v_dm_task', 'passed');
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      INSERT INTO s_sql_dds.t_dq_check_results (check_type, table_name, status, error_message)
      VALUES ('непротиворечивость', 's_sql_dds.v_dm_task', 'error', 'Ошибка выполнения: ' || SQLERRM);
  END;
END;
$$;

ALTER FUNCTION s_sql_dds.fn_dq_checks_load(start_dt date, end_dt date) OWNER TO postgres;
--
-- Name: fn_etl_data_load(date, date); Type: FUNCTION; Schema: s_sql_dds; Owner: postgres
--
CREATE FUNCTION s_sql_dds.fn_etl_data_load(start_date date, end_date date) RETURNS void
    LANGUAGE plpgsql
    AS $_$DECLARE
    c_proc_name TEXT := 'load_clean_orders';
    v_rows_loaded BIGINT;
BEGIN
    -- Удаляем данные за период (order_date — DATE, поэтому используем BETWEEN)
    DELETE FROM s_sql_dds.t_sql_source_structured
    WHERE order_date BETWEEN start_date AND end_date;

    -- Вставляем очищенные данные
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
            -- 1. Защита от NULL и пустых строк
            NULLIF(TRIM(order_id), '') IS NOT NULL
            AND NULLIF(TRIM(customer_id), '') IS NOT NULL
            AND NULLIF(TRIM(product_name), '') IS NOT NULL
            AND NULLIF(TRIM(region), '') IS NOT NULL
            AND NULLIF(TRIM(quantity), '') IS NOT NULL
            AND NULLIF(TRIM(price), '') IS NOT NULL
            AND NULLIF(TRIM(status), '') IS NOT NULL
            AND NULLIF(TRIM(product_category), '') IS NOT NULL
            AND NULLIF(TRIM(order_date), '') IS NOT NULL

            -- 2. Валидация формата даты как строки
            AND order_date ~ '^\d{4}-\d{2}-\d{2}$'
            AND SUBSTRING(order_date, 1, 4)::INT BETWEEN 2020 AND 2025
            AND SUBSTRING(order_date, 6, 2)::INT BETWEEN 1 AND 12
            AND SUBSTRING(order_date, 9, 2)::INT BETWEEN 1 AND 31

            -- 3. Валидация чисел
            AND quantity ~ '^\d+(\.\d*)?$'
            AND price ~ '^\d+(\.\d+)?$'
            AND quantity::NUMERIC > 0
            AND price::NUMERIC > 0
            AND quantity::NUMERIC = TRUNC(quantity::NUMERIC)

            -- 4. Валидация категории
            AND product_category IN ('Электроника', 'Одежда', 'Книги', 'Бытовая техника', 'Спорт')

            -- 5. Фильтр по периоду — БЕЗ приведения к DATE в WHERE!
            AND order_date >= start_date::TEXT
            AND order_date <= end_date::TEXT

        ORDER BY SUBSTRING(order_id FROM 4)::BIGINT, ctid
    )
    INSERT INTO s_sql_dds.t_sql_source_structured (
        order_id, customer_id, order_date, product_category,
        product_name, quantity, price, total_amount, status, region
    )
    SELECT * FROM cleaned;

    GET DIAGNOSTICS v_rows_loaded = ROW_COUNT;
    RAISE NOTICE 'Загружено % строк в t_sql_source_structured за период [% - %]', 
        v_rows_loaded, start_date, end_date;
END;$_$;

ALTER FUNCTION s_sql_dds.fn_etl_data_load(start_date date, end_date date) OWNER TO postgres;
SET default_tablespace = '';
SET default_table_access_method = heap;
--
-- Name: dim_product_category; Type: TABLE; Schema: s_sql_dds; Owner: postgres
--
CREATE TABLE s_sql_dds.dim_product_category (
    id integer NOT NULL,
    name text NOT NULL
);

ALTER TABLE s_sql_dds.dim_product_category OWNER TO postgres;
--
-- Name: dim_product_category_id_seq; Type: SEQUENCE; Schema: s_sql_dds; Owner: postgres
--
CREATE SEQUENCE s_sql_dds.dim_product_category_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE s_sql_dds.dim_product_category_id_seq OWNER TO postgres;
--
-- Name: dim_product_category_id_seq; Type: SEQUENCE OWNED BY; Schema: s_sql_dds; Owner: postgres
--
ALTER SEQUENCE s_sql_dds.dim_product_category_id_seq OWNED BY s_sql_dds.dim_product_category.id;

--
-- Name: dim_product_name; Type: TABLE; Schema: s_sql_dds; Owner: postgres
--
CREATE TABLE s_sql_dds.dim_product_name (
    id integer NOT NULL,
    name text NOT NULL
);

ALTER TABLE s_sql_dds.dim_product_name OWNER TO postgres;
--
-- Name: dim_product_name_id_seq; Type: SEQUENCE; Schema: s_sql_dds; Owner: postgres
--
CREATE SEQUENCE s_sql_dds.dim_product_name_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE s_sql_dds.dim_product_name_id_seq OWNER TO postgres;
--
-- Name: dim_product_name_id_seq; Type: SEQUENCE OWNED BY; Schema: s_sql_dds; Owner: postgres
--
ALTER SEQUENCE s_sql_dds.dim_product_name_id_seq OWNED BY s_sql_dds.dim_product_name.id;

--
-- Name: dim_quantity; Type: TABLE; Schema: s_sql_dds; Owner: postgres
--
CREATE TABLE s_sql_dds.dim_quantity (
    id integer NOT NULL,
    quantity integer NOT NULL
);

ALTER TABLE s_sql_dds.dim_quantity OWNER TO postgres;
--
-- Name: dim_quantity_id_seq; Type: SEQUENCE; Schema: s_sql_dds; Owner: postgres
--
CREATE SEQUENCE s_sql_dds.dim_quantity_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE s_sql_dds.dim_quantity_id_seq OWNER TO postgres;
--
-- Name: dim_quantity_id_seq; Type: SEQUENCE OWNED BY; Schema: s_sql_dds; Owner: postgres
--
ALTER SEQUENCE s_sql_dds.dim_quantity_id_seq OWNED BY s_sql_dds.dim_quantity.id;

--
-- Name: dim_region; Type: TABLE; Schema: s_sql_dds; Owner: postgres
--
CREATE TABLE s_sql_dds.dim_region (
    id integer NOT NULL,
    name text NOT NULL
);

ALTER TABLE s_sql_dds.dim_region OWNER TO postgres;
--
-- Name: dim_region_id_seq; Type: SEQUENCE; Schema: s_sql_dds; Owner: postgres
--
CREATE SEQUENCE s_sql_dds.dim_region_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE s_sql_dds.dim_region_id_seq OWNER TO postgres;
--
-- Name: dim_region_id_seq; Type: SEQUENCE OWNED BY; Schema: s_sql_dds; Owner: postgres
--
ALTER SEQUENCE s_sql_dds.dim_region_id_seq OWNED BY s_sql_dds.dim_region.id;

--
-- Name: dim_status; Type: TABLE; Schema: s_sql_dds; Owner: postgres
--
CREATE TABLE s_sql_dds.dim_status (
    id integer NOT NULL,
    name text NOT NULL
);

ALTER TABLE s_sql_dds.dim_status OWNER TO postgres;
--
-- Name: dim_status_id_seq; Type: SEQUENCE; Schema: s_sql_dds; Owner: postgres
--
CREATE SEQUENCE s_sql_dds.dim_status_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE s_sql_dds.dim_status_id_seq OWNER TO postgres;
--
-- Name: dim_status_id_seq; Type: SEQUENCE OWNED BY; Schema: s_sql_dds; Owner: postgres
--
ALTER SEQUENCE s_sql_dds.dim_status_id_seq OWNED BY s_sql_dds.dim_status.id;

--
-- Name: t_dm_task; Type: TABLE; Schema: s_sql_dds; Owner: postgres
--
CREATE TABLE s_sql_dds.t_dm_task (
    order_id bigint,
    customer_id text,
    order_date date,
    product_category text,
    product_name text,
    quantity integer,
    price numeric(10,2),
    total_amount numeric(10,2),
    status text,
    region text,
    product_category_id integer,
    product_name_id integer,
    quantity_id integer,
    region_id integer,
    status_id integer
);

ALTER TABLE s_sql_dds.t_dm_task OWNER TO postgres;
--
-- Name: t_dq_check_results; Type: TABLE; Schema: s_sql_dds; Owner: postgres
--
CREATE TABLE s_sql_dds.t_dq_check_results (
    check_id integer NOT NULL,
    check_type character varying,
    table_name character varying,
    execution_date timestamp(6) without time zone DEFAULT CURRENT_TIMESTAMP,
    status character varying,
    error_message character varying
);

ALTER TABLE s_sql_dds.t_dq_check_results OWNER TO postgres;
--
-- Name: t_dq_check_results_check_id_seq; Type: SEQUENCE; Schema: s_sql_dds; Owner: postgres
--
CREATE SEQUENCE s_sql_dds.t_dq_check_results_check_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE s_sql_dds.t_dq_check_results_check_id_seq OWNER TO postgres;
--
-- Name: t_dq_check_results_check_id_seq; Type: SEQUENCE OWNED BY; Schema: s_sql_dds; Owner: postgres
--
ALTER SEQUENCE s_sql_dds.t_dq_check_results_check_id_seq OWNED BY s_sql_dds.t_dq_check_results.check_id;

--
-- Name: t_sql_source_structured; Type: TABLE; Schema: s_sql_dds; Owner: postgres
--
CREATE TABLE s_sql_dds.t_sql_source_structured (
    order_id bigint,
    customer_id character varying(100),
    order_date date,
    product_category character varying(50),
    product_name character varying(50),
    quantity integer,
    price numeric(10,2),
    total_amount numeric(10,2),
    status character varying(50),
    region character varying(50)
);

ALTER TABLE s_sql_dds.t_sql_source_structured OWNER TO postgres;
--
-- Name: t_sql_source_unstructured; Type: TABLE; Schema: s_sql_dds; Owner: postgres
--
CREATE TABLE s_sql_dds.t_sql_source_unstructured (
    order_id character varying(100),
    customer_id character varying(100),
    order_date character varying(100),
    product_category character varying(100),
    product_name character varying(100),
    quantity character varying(100),
    price character varying(100),
    total_amount character varying(100),
    status character varying(100),
    region character varying(100)
);

ALTER TABLE s_sql_dds.t_sql_source_unstructured OWNER TO postgres;
--
-- Name: v_dm_task; Type: VIEW; Schema: s_sql_dds; Owner: postgres
--
CREATE VIEW s_sql_dds.v_dm_task AS
 SELECT order_id,
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

ALTER VIEW s_sql_dds.v_dm_task OWNER TO postgres;
--
-- Name: dim_product_category id; Type: DEFAULT; Schema: s_sql_dds; Owner: postgres
--
ALTER TABLE ONLY s_sql_dds.dim_product_category ALTER COLUMN id SET DEFAULT nextval('s_sql_dds.dim_product_category_id_seq'::regclass);

--
-- Name: dim_product_name id; Type: DEFAULT; Schema: s_sql_dds; Owner: postgres
--
ALTER TABLE ONLY s_sql_dds.dim_product_name ALTER COLUMN id SET DEFAULT nextval('s_sql_dds.dim_product_name_id_seq'::regclass);

--
-- Name: dim_quantity id; Type: DEFAULT; Schema: s_sql_dds; Owner: postgres
--
ALTER TABLE ONLY s_sql_dds.dim_quantity ALTER COLUMN id SET DEFAULT nextval('s_sql_dds.dim_quantity_id_seq'::regclass);

--
-- Name: dim_region id; Type: DEFAULT; Schema: s_sql_dds; Owner: postgres
--
ALTER TABLE ONLY s_sql_dds.dim_region ALTER COLUMN id SET DEFAULT nextval('s_sql_dds.dim_region_id_seq'::regclass);

--
-- Name: dim_status id; Type: DEFAULT; Schema: s_sql_dds; Owner: postgres
--
ALTER TABLE ONLY s_sql_dds.dim_status ALTER COLUMN id SET DEFAULT nextval('s_sql_dds.dim_status_id_seq'::regclass);

--
-- Name: t_dq_check_results check_id; Type: DEFAULT; Schema: s_sql_dds; Owner: postgres
--
ALTER TABLE ONLY s_sql_dds.t_dq_check_results ALTER COLUMN check_id SET DEFAULT nextval('s_sql_dds.t_dq_check_results_check_id_seq'::regclass);

--
-- Name: dim_product_category dim_product_category_name_key; Type: CONSTRAINT; Schema: s_sql_dds; Owner: postgres
--
ALTER TABLE ONLY s_sql_dds.dim_product_category
    ADD CONSTRAINT dim_product_category_name_key UNIQUE (name);

--
-- Name: dim_product_category dim_product_category_pkey; Type: CONSTRAINT; Schema: s_sql_dds; Owner: postgres
--
ALTER TABLE ONLY s_sql_dds.dim_product_category
    ADD CONSTRAINT dim_product_category_pkey PRIMARY KEY (id);

--
-- Name: dim_product_name dim_product_name_name_key; Type: CONSTRAINT; Schema: s_sql_dds; Owner: postgres
--
ALTER TABLE ONLY s_sql_dds.dim_product_name
    ADD CONSTRAINT dim_product_name_name_key UNIQUE (name);

--
-- Name: dim_product_name dim_product_name_pkey; Type: CONSTRAINT; Schema: s_sql_dds; Owner: postgres
--
ALTER TABLE ONLY s_sql_dds.dim_product_name
    ADD CONSTRAINT dim_product_name_pkey PRIMARY KEY (id);

--
-- Name: dim_quantity dim_quantity_pkey; Type: CONSTRAINT; Schema: s_sql_dds; Owner: postgres
--
ALTER TABLE ONLY s_sql_dds.dim_quantity
    ADD CONSTRAINT dim_quantity_pkey PRIMARY KEY (id);

--
-- Name: dim_quantity dim_quantity_quantity_key; Type: CONSTRAINT; Schema: s_sql_dds; Owner: postgres
--
ALTER TABLE ONLY s_sql_dds.dim_quantity
    ADD CONSTRAINT dim_quantity_quantity_key UNIQUE (quantity);

--
-- Name: dim_region dim_region_name_key; Type: CONSTRAINT; Schema: s_sql_dds; Owner: postgres
--
ALTER TABLE ONLY s_sql_dds.dim_region
    ADD CONSTRAINT dim_region_name_key UNIQUE (name);

--
-- Name: dim_region dim_region_pkey; Type: CONSTRAINT; Schema: s_sql_dds; Owner: postgres
--
ALTER TABLE ONLY s_sql_dds.dim_region
    ADD CONSTRAINT dim_region_pkey PRIMARY KEY (id);

--
-- Name: dim_status dim_status_name_key; Type: CONSTRAINT; Schema: s_sql_dds; Owner: postgres
--
ALTER TABLE ONLY s_sql_dds.dim_status
    ADD CONSTRAINT dim_status_name_key UNIQUE (name);

--
-- Name: dim_status dim_status_pkey; Type: CONSTRAINT; Schema: s_sql_dds; Owner: postgres
--
ALTER TABLE ONLY s_sql_dds.dim_status
    ADD CONSTRAINT dim_status_pkey PRIMARY KEY (id);

--
-- Name: t_dq_check_results t_dq_check_results_pkey; Type: CONSTRAINT; Schema: s_sql_dds; Owner: postgres
--
ALTER TABLE ONLY s_sql_dds.t_dq_check_results
    ADD CONSTRAINT t_dq_check_results_pkey PRIMARY KEY (check_id);

--
-- Name: t_dm_task fk_product_category_id; Type: FK CONSTRAINT; Schema: s_sql_dds; Owner: postgres
--
ALTER TABLE ONLY s_sql_dds.t_dm_task
    ADD CONSTRAINT fk_product_category_id FOREIGN KEY (product_category_id) REFERENCES s_sql_dds.dim_product_category(id);

--
-- Name: t_dm_task fk_product_name_id; Type: FK CONSTRAINT; Schema: s_sql_dds; Owner: postgres
--
ALTER TABLE ONLY s_sql_dds.t_dm_task
    ADD CONSTRAINT fk_product_name_id FOREIGN KEY (product_name_id) REFERENCES s_sql_dds.dim_product_name(id);

--
-- Name: t_dm_task fk_quantity_id; Type: FK CONSTRAINT; Schema: s_sql_dds; Owner: postgres
--
ALTER TABLE ONLY s_sql_dds.t_dm_task
    ADD CONSTRAINT fk_quantity_id FOREIGN KEY (quantity_id) REFERENCES s_sql_dds.dim_quantity(id);

--
-- Name: t_dm_task fk_region_id; Type: FK CONSTRAINT; Schema: s_sql_dds; Owner: postgres
--
ALTER TABLE ONLY s_sql_dds.t_dm_task
    ADD CONSTRAINT fk_region_id FOREIGN KEY (region_id) REFERENCES s_sql_dds.dim_region(id);

--
-- Name: t_dm_task fk_status_id; Type: FK CONSTRAINT; Schema: s_sql_dds; Owner: postgres
--
ALTER TABLE ONLY s_sql_dds.t_dm_task
    ADD CONSTRAINT fk_status_id FOREIGN KEY (status_id) REFERENCES s_sql_dds.dim_status(id);

--
-- PostgreSQL database dump complete
--
\unrestrict WZHwvb0KRQ1SS2qdqVMCXQw9K2Z8bun2HUNPDNOadRdfRaGpFDFwbzKbtbHDAu9
