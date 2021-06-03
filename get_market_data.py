import csv
import re
import sqlite3
from loguru import logger
from sqlite3 import Cursor
from typing import Union, Any

import requests
from config import MARKETS_LIST, DB_NAME


def get_market_history(market: str) -> bytes:
    """
    Get historic data from finance.yahoo
    :param market: market name from list of available in finance.yahoo
    :return: file as binary string
    """
    market = re.sub('^A-Za-z0-9', '', market)
    error_message = b'404 Not Found: No data found, symbol may be delisted'
    request = requests.get(f'https://query1.finance.yahoo.com/v7/finance/download/{market}'
                           f'?period1=0&period2=9999999999'
                           f'&interval=1d&events=history&includeAdjustedClose=true').content
    if not market.isalpha() or error_message in request:
        logger.error(f'Invalid Symbol {market}')
    return request


def save_market_data_to_file(market_data: bytes, market_name: str) -> None:
    """
    Saving data to csv file
    :param market_data: binary string of data
    :param market_name: market name from list of available in finance.yahoo
    :return: None
    """
    filename = f'market-data/{market_name}.csv'
    with open(filename, 'w') as file:
        file.writelines(market_data.decode('utf-8'))
    logger.info(f'Processing data. Saving to file {filename}')


def get_db_connection(db_name=DB_NAME) -> Cursor:
    """
    Create and return connection to database
    :param db_name: (str) database name, default DB_NAME
    :return: cursor for db
    """
    cursor = sqlite3.connect(db_name).cursor()
    logger.info(f"Connecting to database {db_name}")
    return cursor


def check_table_exist(market_name: str) -> bool:
    """
    Checks does table exist in database
    :param market_name: str
    :return: bool
    """
    cursor = get_db_connection()
    tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    cursor.close()
    return tuple(market_name) in tables


def create_market_table(market_name: str) -> None:
    """
    Create table with market name in database
    :param market_name: str
    :return: None
    """
    cursor = get_db_connection()
    if not check_table_exist(market_name):
        cursor.execute(f"""CREATE TABLE {market_name}(date text, open real, high real, low real,
                                                        close real, adj_close real, volume integer)""")
        cursor.close()
        logger.info(f'Table {market_name} created in database')
    else:
        return


def save_market_data_to_db(market_name: str):
    """
    Saving data from csv file to database
    :param market_name:
    :return:
    """
    cursor = get_db_connection()
    create_market_table(market_name)
    filename = f'market-data/{market_name}.csv'
    with open(filename, 'r') as fin:
        dr = csv.DictReader(fin)
        to_db = [(i['Date'], i['Open'], i['High'], i['Low'], i['Close'], i['Adj Close'], i['Volume']) for i in dr]
    cursor.executemany(f"""INSERT INTO {market_name}(date, open, high, low, close, adj_close, volume) 
                                                                            VALUES (?, ?, ?, ?, ?, ?, ?);""", to_db)
    cursor.connection.commit()
    cursor.close()
    logger.info(f'Data from {filename} saved in database table {market_name}')


def get_market_jsonified_data(market_name: str) -> Union[list[dict[str, Any]], str]:
    """
    Get data from database in dict format
    :param market_name: str
    :return: list of dicts with market data
    """
    try:
        cursor = get_db_connection()
        cursor.execute(f"SELECT * FROM {market_name};")
        data = cursor.fetchall()
        cursor.close()
        data_list = [{'date': line[0],
                      'open': line[1],
                      'high': line[2],
                      'low': line[3],
                      'close': line[4],
                      'adj_close': line[5],
                      'volume': line[6]} for line in data]
        logger.info(f'Market data for {market_name} serialized successfully')
        return data_list
    except sqlite3.OperationalError:
        logger.error(f"There is no data in database with market {market_name}")


def load_market_data_to_db(market_list=MARKETS_LIST) -> None:
    """
    Load market data to database with given list of names
    :param market_list: List of given markets
    :return: None
    """
    for market_name in MARKETS_LIST:
        logger.info(f"Processing data to database")
        market_data = get_market_history(market_name)
        save_market_data_to_file(market_data, market_name)
        save_market_data_to_db(market_name)
