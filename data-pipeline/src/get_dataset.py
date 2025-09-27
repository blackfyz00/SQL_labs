# src/get_dataset.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from faker import Faker

def get_dataset(n_rows=100000):
    """
    Генерирует синтетический датасет с "сломанными" данными.
    :param n_rows: Количество строк в датасете.
    :return: pandas DataFrame
    """
    fake = Faker('ru_RU')
    categories = ['Электроника', 'Одежда', 'Книги', 'Бытовая техника', 'Спорт']
    statuses = ['завершен', 'отменен', 'в ожидании', 'отправлен']
    regions = ['Москва', 'Санкт-Петербург', 'Новосибирск', 'Екатеринбург', None]

    data = {
        'order_id': [],
        'customer_id': [],
        'order_date': [],
        'product_category': [],
        'product_name': [],
        'quantity': [],
        'price': [],
        'total_amount': [],
        'status': [],
        'region': []
    }

    for i in range(n_rows):
        # order_id (добавляем дубликаты с вероятностью 5%)
        order_id = f'ORD{i:06d}' if random.random() > 0.05 else f'ORD{random.randint(0, i):06d}'
        data['order_id'].append(order_id)

        # customer_id (10% пропусков)
        data['customer_id'].append(fake.uuid4() if random.random() > 0.1 else None)

        # order_date (10% некорректных или пропущенных)
        if random.random() < 0.1:
            order_date = random.choice(['2023-13-01', 'не дата', None])
        else:
            start_date = datetime(2023, 1, 1)
            order_date = (start_date + timedelta(days=random.randint(0, 730))).strftime('%Y-%m-%d')
        data['order_date'].append(order_date)

        # product_category (5% ошибок в написании)
        category = random.choice(categories)
        if random.random() < 0.05:
            category = category + 'а'  # Например, "Электроникаа"
        data['product_category'].append(category)

        # product_name (10% пропусков)
        data['product_name'].append(fake.word() if random.random() > 0.1 else None)

        # quantity (10% отрицательных или пропусков)
        if random.random() < 0.1:
            quantity = random.choice([-1, None])
        else:
            quantity = random.randint(1, 10)
        data['quantity'].append(quantity)

        # price (10% отрицательных или пропусков)
        if random.random() < 0.1:
            price = random.choice([-100.0, None])
        else:
            price = round(random.uniform(10.0, 1000.0), 2)
        data['price'].append(price)

        # total_amount (20% несоответствий quantity * price)
        if quantity is not None and price is not None and random.random() > 0.2:
            total_amount = round(quantity * price, 2)
        else:
            total_amount = round(random.uniform(-100.0, 10000.0), 2)
        data['total_amount'].append(total_amount)

        # status (5% ошибок в написании)
        status = random.choice(statuses)
        if random.random() < 0.05:
            status = status + '_error'
        data['status'].append(status)

        # region (10% пропусков)
        data['region'].append(random.choice(regions))

    return pd.DataFrame(data)