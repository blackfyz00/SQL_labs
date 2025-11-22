import pandas as pd
from sqlalchemy import create_engine
from src.config import db_params as pg_params, mysql_params

if __name__ == "__main__":
    # --- Настройки подключения ---
    # Имя таблицы
    table_name = "t_dm_task"
    schema_name = "s_sql_dds"  # только для PostgreSQL (схема)

    # --- Подключение к PostgreSQL ---
    pg_engine = create_engine(
        f"postgresql+psycopg2://{pg_params['user']}:{pg_params['password']}@"
        f"{pg_params['host']}:{pg_params['port']}/{pg_params['database']}"
    )

    # --- Чтение данных ---
    df = pd.read_sql_table(
        table_name=table_name,
        schema=schema_name,
        con=pg_engine
    )

    # --- Подключение к MySQL ---
    mysql_engine = create_engine(
        f"mysql+pymysql://{mysql_params['user']}:{mysql_params['password']}@"
        f"{mysql_params['host']}:{mysql_params['port']}/{mysql_params['database']}"
    )

    # --- Запись в MySQL ---
    df.to_sql(
        name=table_name,
        con=mysql_engine,
        if_exists="replace",  # или "append", если нужно добавить к существующей таблице
        index=False,          # не сохранять индекс как колонку
        chunksize=10000       # опционально: для больших таблиц
    )

    print(f"Таблица {schema_name}.{table_name} успешно перенесена в MySQL.")