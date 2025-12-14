import psycopg2
import logging
from psycopg2.extras import execute_values
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
import datetime
import pandas as pd
import requests
from rich import print
import time
from loguru import logger

host = "localhost"
username = "postgres"
password = "141312"
db_name = "postgres"
port = 5432

logger.add(
    "LOG/log {time}.log",
    level='INFO',
    format = "{time:[DD.MM.YYYY]-[HH:mm:ss]} [{level}] {message}",
    enqueue=True #асинхронная запись
    )

@dataclass
class db_tablenames:
    megapolys: str = 'megapolys'
    konturterm: str = 'konturterm'
    tdstroitel: str = 'tdstroitel'
    stv39: str = 'stv39'

@dataclass
class sku:
    shop: str
    date: datetime
    article: str
    catalog_path: str
    title: str
    normalized_title: str
    url: str
    pic_url: str
    price: float
    stock: float

@dataclass
class db_tablenames:
    megapolys: str = 'megapolys'
    konturterm: str = 'konturterm'
    baucenter: str = 'baucenter'
    lemanapro: str = 'lemanapro'
    tdstroitel: str = 'tdstroitel'
    stv39: str = 'stv39'

@dataclass
class shops:
    megapolys: str = 'https://www.megapolys.com/'
    konturterm: str = 'https://www.konturterm.ru'
    baucenter: str = 'https://baucenter.ru/'
    lemanapro: str = 'https://kaliningrad.lemanapro.ru'
    tdstroitel: str = 'https://tdstroitel.ru/'
    stv39: str = "https://stv39.ru/"


async def write_to_db(tablename:str, input_data:list[tuple]):
    try:
        connection = psycopg2.connect(
                host=host,
                user=username,
                password=password,
                database=db_name,
        )
        connection.autocommit = True
        
        with connection.cursor() as cursor:
            cursor.execute(
                f"""CREATE TABLE IF NOT EXISTS {tablename}(
                id SERIAL PRIMARY KEY NOT NULL,
                shop VARCHAR(100),
                date TIMESTAMP NOT NULL,
                catalog_path VARCHAR,
                article VARCHAR, 
                title VARCHAR,
                normalized_title VARCHAR,
                url VARCHAR,
                pic_url VARCHAR,
                price REAL,
                stock REAL
                );"""
            )
        
        with connection.cursor() as cursor:
            logger.info(f"Запись в базу данных в количестве: {len(input_data)}")
            execute_values(cursor, f"INSERT INTO {tablename} (shop, date, catalog_path, article, title, normalized_title, url, pic_url, price, stock) VALUES %s", input_data)

    except Exception as ex:
        logger.info(f"Ошибка работы с базой данных: {ex}" )


def export_from_postgresql(tablename:str, days_ago:int) -> pd.DataFrame:
    connection = psycopg2.connect(
                host=host,
                user=username,
                password=password,
                database=db_name,
        )
    connection.autocommit = True
    
        
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {tablename} WHERE date >= now() - interval '{days_ago} day'")
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['index', 'shop', 'date', 'article', 'title', 'url', 'pic_url', 'price', 'stock'])
        logger.info(f"Количествро строк датафрэйма {len(df)}")
    return df

