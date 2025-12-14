import requests
import asyncio
import sys
from bs4 import BeautifulSoup
from rich import print
from dataclasses import dataclass
import aiohttp
from datetime import datetime, timedelta
import datetime
import time
import pandas as pd
from typing import NamedTuple
import requests
from loguru import logger
from config import db_tablenames, shops, sku, write_to_db
import string


logger.add(
    "LOG/log {time}.log",
    level='INFO',
    retention="10 days",
    format = "{time:[DD.MM.YYYY]-[HH:mm:ss]} [{level}] {line} {message}",
    enqueue=True #асинхронная запись,
    )

semaphore = asyncio.Semaphore(5)


ROOT_URL = 'https://www.konturterm.ru/catalog/'
TABLENAME = db_tablenames.konturterm
TIMEOUT = 20
MAX_ATTEMPTS = 5
CURRENT_PAGE = 0
FINAL_URL_LIST = []
SKU_COUNT = 0

headers = {
    'accept': '*/*',
    'accept-language': 'ru,en;q=0.9',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 YaBrowser/25.2.0.0 Safari/537.36',
}

cookies = {
    'is_gdpr': '0',
    'yuidss': '8697576231763142844',
    'ymex': '2078502845.yrts.1763142845',
    'my': 'YwA=',
    'gdpr': '0',
    '_ym_uid': '1763142985172199021',
    'yandex_login': 'k.ufimcev',
    '_ym_d': '1763143105',
    'amcuid': '1797130091763144971',
    'skid': '3700026931763145006',
    'yashr': '4393905941763811030',
    'yandexuid': '8697576231763142844',
    'is_gdpr_b': 'CMfFKhCu5AIoAg==',
    'yabs-vdrf': 'A0',
    'alice_uuid': '0EA3B3DA-F959-408A-93CD-8FB8FF5F88E3',
    'Session_id': '3:1764961635.5.0.1763143056204:_wztbQ:6e8d.1.2:1|179023899.-1.2.3:1763143056|3:11468222.422356.miojOQi6LTAycmLg3R6f-1hjUc8',
    'sessar': '1.1396519.CiBH_Y36H_Jzsd1tz8gaFGVXY9sPIzS6viiuiL9p8Kt5dg.5VBhBA7aCU-h7v9NBPdOeS0LgWb9D3wpHBDZgz1Vydw',
    'sessionid2': '3:1764961635.5.0.1763143056204:_wztbQ:6e8d.1.2:1|179023899.-1.2.3:1763143056|3:11468222.422356.fakesign0000000000000000000',
    'coockoos': '1',
    'i': 'WEU7WvDNczFQ9q7ubX1muJbEcnhJkmWoyNJXcgB/L1pAq7vpeOsqsC2tQ9s79Mup5eowGYhSXqP74V30gjS4nkb4hKE=',
    '_ym_isad': '2',
    'sae': '0:0EA3B3DA-F959-408A-93CD-8FB8FF5F88E3:p:25.10.2.1176:w:d:RU:20240814',
    'isa': 'TH/b9AZaT7v+ktXwRxR43g8duJdSH9Rfv8zdSxHxU6aC5p/xrq/3S3FNWykX1cXlLxSKVmieDFh+A80cMznvarQ3+gU=',
    'yabs-sid': '486780531765096691',
    'yclid_src': 'export-base.ru:17867503823716876287:8697576231763142844',
    'cycada': 'LICLwqAtGZHFg1eBXGW0pW4SvoLEdEastVVYVWGqeHU=',
    'ys': 'svt.1#def_bro.1#wprid.1765127729918098-7466794192977053924-balancer-l7leveler-kubr-yp-klg-294-BAL#ybzcc.ru#newsca.native_cache',
    'yp': '1765182793.uc.ru#1765182793.duc.ru#1795347031.cld.2270452#1795347031.brd.6301000000#1780216032.szm.1:1920x1080:1904x960:15#1765821392.csc.1#2078503056.udn.cDprLnVmaW1jZXY%3D#2080488152.pcs.0#1796664152.swntab.0#1765821469.hdrc.1#2078503105.skin.s#1794834279.dc_neuro.10#1794686616.bk-map.1#1765785321.dlp.2#1765135993.gpauto.54_710159:20_510138:100000:3:1765128793',
    'gpauto': '54_710159:20_510138:100000:3:1765128793',
    'bh': 'ElEiQ2hyb21pdW0iO3Y9IjE0MCIsICJOb3Q9QT9CcmFuZCI7dj0iMjQiLCAiWWFCcm93c2VyIjt2PSIyNS4xMCIsICJZb3dzZXIiO3Y9IjIuNSIaBSJ4ODYiIgwyNS4xMC4yLjExNzYqAj8wMgIiIjoJIldpbmRvd3MiQgciNy4wLjAiSgQiNjQiUmoiQ2hyb21pdW0iO3Y9IjE0MC4wLjczMzkuMTE3NiIsICJOb3Q9QT9CcmFuZCI7dj0iMjQuMC4wLjAiLCAiWWFCcm93c2VyIjt2PSIyNS4xMC4yLjExNzYiLCAiWW93c2VyIjt2PSIyLjUiWgI/MGC799bJBmoh3Mrh/wiS2KGxA5/P4eoD+/rw5w3r//32D/6899YF84EC',
}

def get_category_urls(root_url: str)-> list[str]:
    """Функция собирает корневые url"""
    response = requests.get(url=root_url, headers=headers, cookies=cookies)
    html = BeautifulSoup(response.text, 'lxml')
    container = html.find('div', {'class':'main-catalog-wrapper'})
    item_blocks = container.find_all('div', {'class' : 'item_block sm col-md-4 col-xs-6'})
    result = []
    for item in item_blocks:
        lis = item.find_all('li', {'class':'sect font_xs'})
        for li in lis:
            href = li.find('a').get('href')
            result.append(root_url.replace('/catalog/', '') + href)
        hiden_lis = item.find_all('li', {'class': 'sect collapsed font_xs'})
        for li in hiden_lis:
            href = li.find('a').get('href')
            result.append(root_url.replace('/catalog/', '') + href)
    logger.info(f"Собрано [{len(result)}] ссылок категорий для парсинга")
    return result

async def generate_urls(session:aiohttp.ClientSession, url:str):
    """
    Функция генерирует url на основе количеств апгаинации на странице
    используется
    """
    global FINAL_URL_LIST
    async with semaphore:
        try:
            response = await session.get(url=url, headers=headers, timeout=TIMEOUT)
        except (asyncio.TimeoutError, aiohttp.ClientConnectionError, aiohttp.ClientConnectorDNSError) as ex:
            logger.info(f"Ошибка {ex} для страницы [{url}]")
            return
        else:
            logger.info(f'Статус код: [{response.status}] для страницы [{url}]')
            html = BeautifulSoup(await response.text(), 'lxml')
            try:
                pagin_modul = html.find('div', {'class':'nums'}).find_all('a', {'class':'dark_link'})
            except Exception as ex:
                logger.info(f"Пагинация на странице [{url}] отсутствует, одна страница")
                FINAL_URL_LIST.append(url + f"?PAGEN_1={1}")
            else:
                pages = int(pagin_modul[-1].text)
                logger.info(f'Для [{url}] количество страниц для пагинации [{pages}]')
                for p in range(1, pages + 1):
                    FINAL_URL_LIST.append(url + f"?PAGEN_1={p}")

def _normalization(string: str) -> str:
    """
    Функция делает нормализацию строки
    """
    #table = str.maketrans("", "", "!@#$%^\"&*(){}[]|._-`/?:;'\,~")   
    table = str.maketrans("!@#$%^\"&*(){}[]|_-`/?:;',~", "                          ") #количество пробелов должно соответствовать количествек символов замены  
    result = string.translate(table)

    return result

async def get_response(session:aiohttp.ClientSession, url:str):
    """
    Функция контроля повторных подключений
    """
    logger.info(f"Выполняю парсинг старницы {url}")
    attempt = 0
    while True:
        async with semaphore:
            try:
                async with session.get(url=url, headers=headers, cookies=cookies, timeout=TIMEOUT) as response:
                    if response.status == 200:
                        await parse_page(response=response, url=url)
                        break
                    else:
                        attempt +=1
                        logger.info(f'Повторное подключение [{attempt}] из [{MAX_ATTEMPTS}]')
                        await asyncio.sleep(2)
                        if attempt > MAX_ATTEMPTS:
                            logger.info(f"Количество переподключений превысило [{MAX_ATTEMPTS}]")
                            break
            except (asyncio.TimeoutError, aiohttp.ClientConnectionError, aiohttp.ClientConnectorDNSError):
                logger.info(f'Ошибка времени ожидания или подключения сети для ссылки [{url}]')
                attempt +=1
                await asyncio.sleep(2)
                if attempt > MAX_ATTEMPTS:
                    logger.info(f"Количество переподключений превысило [{MAX_ATTEMPTS}]")
                    break


async def parse_page(response:aiohttp.ClientResponse, url:str) -> None:
    """
    Функция парсинга одной страницы без захода в карточки

    """
    global CURRENT_PAGE
    global SKU_COUNT
    html = BeautifulSoup(await response.text(), 'lxml')
    print(f"Обработано [{CURRENT_PAGE}] страниц из [{len(FINAL_URL_LIST)}]")
    try:
        cards = html.find('div', {'class':'section-content-wrapper with-leftblock'}).find_all('div', class_='col-lg-3 col-md-4 col-sm-6 col-xs-6 col-xxs-12 item item-parent catalog-block-view__item item_block')
    except Exception as ex:
        logger.info(f"Ошибка считывания краточек [{ex}] [{url}]")
    else:
        result = []
        shop = shops.konturterm
        date = datetime.datetime.now()
        for card in cards:
            try:
                article = card.find('div', {'class':'article_block'}).get('data-value').strip()
            except Exception as ex:
                logger.info(f"Ошибка считывания article [{ex}] [{url}]")
                article = None
            try:
                cat_path = html.find('div', id='navigation').find('div', {'class':'breadcrumbs swipeignore'}).find_all('span', {'class':'breadcrumbs__item-name font_xs'})
                res = []
                for cat in cat_path:
                    res.append(cat.text)
                cat_path = '/'.join(res)
            except Exception as ex:
                logger.info(f"Ошибка считывания cat_path [{ex}] [{url}]")
                cat_path = None  
            try:
                title = card.find('div', {'class':'item-title'}).text.strip()
            except Exception as ex:
                logger.info(f"Ошибка считывания product_name [{ex}] [{url}]")
                title = None
            try:
                normal_title = _normalization(string=title)
            except Exception as ex:
                logger.info(f"Ошибка создания нормализованной строки normal_title [{ex}] [{url}]")
                normal_title = None
            try:
                product_url = card.find('div', {'class':'item-title'}).find('a').get('href')
                product_url = ROOT_URL.replace('/catalog/', '') + product_url
            except Exception as ex:
                logger.info(f"Ошибка считывания product_url [{ex}] [{url}]")
                product_url = None
            try:
                price = float(card.find('div', {'class':'price font-bold font_mxs'}).get('data-value'))
            except Exception as ex:
                logger.info(f"Ошибка считывания price [{ex}] [{url}]")
                price = 0
            try:
                stock = float(card.find('span', {'class':'value font_sxs'}).text.replace('Есть в наличии:', '').strip())
                #stock = int(card.find('span', {'class':'value font_sxs'}).text.replace('Есть в наличии: ', '').strip())
            except Exception as ex:
                logger.info(f"Ошибка считывания stock [{ex}] [{url}]")
                stock = 0
            try:
                pic_url = card.find('a').find('img').get('data-src')
                pic_url = ROOT_URL.replace('/catalog/', '') + pic_url
            except Exception as ex:
                logger.info(f"Ошибка считывания url_pic [{ex}] [{url}]")
                pic_url = None
            
            item = sku(
                shop=shop,
                date=date,
                catalog_path=cat_path,
                article=article,
                title=title,
                normalized_title=normal_title,
                url=product_url,
                pic_url=pic_url,
                price=price,
                stock=stock
            )
            #print(item)
            if (article == None) or (title == None):
                pass
            else:
                result.append((item.shop, 
                            item.date, 
                            item.catalog_path, 
                            item.article, 
                            item.title, 
                            item.normalized_title, 
                            item.url, 
                            item.pic_url, 
                            item.price, 
                            item.stock))
        if len(result) != 0:
            await write_to_db(tablename=TABLENAME, input_data=result)
            CURRENT_PAGE +=1
            SKU_COUNT += len(result) #счетчки количества товаров
        else:
            logger.info("Не собран результат - не записываю в базу")
            return

async def main():
    categoty_urls = get_category_urls(root_url=ROOT_URL)
    
    async with aiohttp.ClientSession() as session:
        async with asyncio.TaskGroup() as group:
            tasks = [group.create_task(generate_urls(session=session, url=url)) for url in categoty_urls[0:5]]
    
    logger.info(f"Всего собрано [{len(FINAL_URL_LIST)}] ссылок для парсинга")

    async with aiohttp.ClientSession() as session:
       async with asyncio.TaskGroup() as group:
           tasks = [group.create_task(get_response(session=session, url=url)) for url in FINAL_URL_LIST]

if __name__ == "__main__":
    start_time = time.perf_counter()

    asyncio.run(main())

    end_time = time.perf_counter()
    duration = round((float(end_time - start_time) / 60), 1)
    logger.info(f'Время выполнения парсинга KONTURTERM: {duration} минут!')
    logger.info(f"Собрано товаров [{SKU_COUNT:_}]")


    
    
    