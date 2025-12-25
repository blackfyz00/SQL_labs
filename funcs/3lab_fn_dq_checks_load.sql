CREATE OR REPLACE FUNCTION s_sql_dds.fn_dq_checks_load(start_dt DATE, end_dt DATE)
RETURNS VOID AS $$
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
$$ LANGUAGE plpgsql;

-- Пример вызова для проверки данных за текущий месяц
SELECT s_sql_dds.fn_dq_checks_load(
  DATE_TRUNC('month', CURRENT_DATE)::DATE,
  (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month - 1 day')::DATE
);