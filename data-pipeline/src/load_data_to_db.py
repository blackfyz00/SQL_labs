# src/load_data_to_db.py
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import logging

def load_data_to_db(df, db_params):
    """
    Загружает данные из DataFrame в таблицу t_sql_source_unstructured.
    :param df: pandas DataFrame с данными.
    :param db_params: Параметры подключения к базе данных.
    """
    try:
        # Настройка логирования
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

        # Создание подключения через SQLAlchemy
        engine = create_engine(
            f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@"
            f"{db_params['host']}:{db_params['port']}/{db_params['database']}"
        )


        logger.info("Очистка таблицы t_sql_source_unstructured")

        #Очистка таблицы
        with engine.connect() as conn:
            with conn.begin():  # Явная транзакция
                conn.execute(text("TRUNCATE TABLE s_sql_dds.t_sql_source_unstructured"))

        # Загрузка данных в таблицу
        logger.info("Загрузка данных в таблицу t_sql_source_unstructured")
        df.to_sql('t_sql_source_unstructured', engine, schema='s_sql_dds', if_exists='append', index=False)
        logger.info("Данные успешно загружены")

    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {e}")
        raise