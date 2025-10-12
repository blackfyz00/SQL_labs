import psycopg2
from psycopg2 import sql
from datetime import date
from config import db_params

def fill_structured_table(start_date: date, end_date: date, conn_params: dict):
    """
    Вызывает процедуру load_clean_orders в PostgreSQL для загрузки очищенных данных.
    
    :param start_date: начальная дата периода (datetime.date)
    :param end_date: конечная дата периода (datetime.date)
    :param conn_params: параметры подключения к БД (словарь для psycopg2.connect)
    """
    conn = None
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        # Вызываем процедуру
        cur.execute(
            "SELECT s_sql_dds.fn_etl_data_load(%s, %s)",
            (start_date, end_date)
        )

        # Если нужно получить NOTICE-сообщения:
        conn.commit()  # CALL не требует коммита, но если были изменения — лучше явно
        print(f"Процедура успешно выполнена для периода {start_date} - {end_date}")

    except Exception as e:
        print(f"Ошибка при выполнении процедуры: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
   fill_structured_table(date(2022, 1, 1), date(2025, 1, 31), db_params)