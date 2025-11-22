# main.py
from src.get_dataset import get_dataset
from src.load_data_to_db import load_data_to_db
from src.config import db_params
import logging

def main():
    # Настройка логирования
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        # Генерация датасета
        logger.info("Генерация датасета")
        df = get_dataset()
        logger.info(f"Сгенерировано {len(df)} строк")

        # Загрузка в базу данных
        load_data_to_db(df, db_params)

    except Exception as e:
        logger.error(f"Ошибка в процессе выполнения: {e}")
        raise

if __name__ == "__main__":
    main()