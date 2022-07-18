# TODO: Better way to pass data between tasks
# cf. https://www.astronomer.io/guides/airflow-passing-data-between-tasks
# TODO: USE Mongo DB container or Package apache-airflow-providers-mongo
# cf. https://airflow.apache.org/docs/apache-airflow-providers-mongo/stable/index.html
# TODO: change python operator to bash operator, for dependency on database and etc.
# ex. https://github.com/vineeths96/Data-Engineering-Nanodegree/blob/master/Project%206%20Capstone%20Project/airflow/dag.py

# https://airflow.apache.org/docs/apache-airflow/stable/timezone.html
# TODO: fix scheduling

# Templates reference
# - https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html#
# - https://stackoverflow.com/questions/71915396/what-is-the-prefered-way-to-write-next-ds-on-an-airflow-template
# MongoDB index
# - https://stackoverflow.com/questions/36939482/pymongo-create-index-only-if-it-does-not-exist

import os
import json
import logging
import requests
import pendulum
import datetime as dt
from typing import Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator

from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv("/tmp/.env")

SCHEDULE_INTERVAL = "*/10 * * * *"
MINUTE_INTERVAL = 1
GET_CNT = 10
KST = pendulum.timezone("Asia/Seoul")
UTC_TIMEZONE = pendulum.timezone('UTC')
FILE_BASE_DIR = "/data/upbit"

def _get_mongo_client():
    user = os.getenv("MONGODB_USER") 
    pwd = os.getenv("MONGODB_PWD") 
    host = os.getenv("MONGODB_HOST")
    port = os.getenv("MONGODB_PORT")
    client = MongoClient(f'mongodb://{user}:{pwd}@{host}:{port}')
    return client


def _get_minutes_ohlcvs(interval: int, ticker: str, to: pendulum.datetime, count: int) -> List[Dict]:
    """Get ohlcvs until datetime 'to'."""
    to = UTC_TIMEZONE.convert(to).strftime('%Y-%m-%d %H:%M:%S')
    url = f"https://api.upbit.com/v1/candles/minutes/{interval}?market={ticker}&to={to}&count={count}"
    headers = {"Accept": "application/json"}
    response = requests.get(url, headers=headers)
    # Error happens randomly
    # TODO: json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0) 
    response = json.loads(response.text)
    return response


def _json_strptime(json_dicts: List[Dict] , dict_keys: List[str]= ['candle_date_time_utc', 'candle_date_time_kst']):
    for key in dict_keys:
        for ohlcv in json_dicts:
            ohlcv.update({key:dt.datetime.strptime(ohlcv[key], '%Y-%m-%dT%H:%M:%S')})
    return json_dicts


def _ts_2_pendulum_datetime(start_time: str):
    start_time = start_time.split('.')[0]
    start_time = pendulum.from_format(start_time, 'YYYYMMDDTHHmmss')
    return start_time


with DAG(
    dag_id="api2db",
    description="Get ohlcv data using upbit API",
    start_date=dt.datetime(2022, 7, 18, 9, 0, tzinfo=KST),
    end_date=dt.datetime(2022, 7, 23, 0, 0, tzinfo=KST),
    schedule_interval=SCHEDULE_INTERVAL,
) as dag:
    def _fetch_ohlcvs(templates_dict, **kwargs):
        logger = logging.getLogger(__name__)

        start_time = templates_dict["start_time"] # "2022-07-18T07:43:15.165980+00:00"
        datetime_start_time = _ts_2_pendulum_datetime(start_time)        

        coin_ticker = templates_dict["coin_ticker"]
        output_path =  f"{FILE_BASE_DIR}/{coin_ticker}/{start_time}.json"
        logger.info(f"Fetching ohlcvs til {start_time}")

        ohlcvs = _get_minutes_ohlcvs(MINUTE_INTERVAL, coin_ticker, datetime_start_time, GET_CNT)
        logger.info(f"Fetched {len(ohlcvs)} ohlcvs")
        logger.info(f"Writing ohlcvs to {output_path}")

        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)

        with open(output_path, "w") as file_:
            json.dump(ohlcvs, fp=file_)
    
    fetch_usdt_btc = PythonOperator(
        task_id="fetch_usdt_btc",
        python_callable=_fetch_ohlcvs,
        templates_dict={
            "start_time": "{{ ts_nodash }}",
            "end_time": "{{ data_interval_end | ts_nodash }}",
            "coin_ticker" : "USDT-BTC"
        },
    )
    fetch_krw_btc = PythonOperator(
        task_id="fetch_krw_btc",
        python_callable=_fetch_ohlcvs,
        templates_dict={
            "start_time": "{{ ts_nodash }}",
            "end_time": "{{ data_interval_end | ts_nodash }}",
            "coin_ticker" : "KRW-BTC"
        },
    )
    fetch_usdt_eth = PythonOperator(
        task_id="fetch_usdt_eth",
        python_callable=_fetch_ohlcvs,
        templates_dict={
            "start_time": "{{ ts_nodash }}",
            "end_time": "{{ data_interval_end | ts_nodash }}",
            "coin_ticker" : "USDT-ETH"
        },
    )
    fetch_krw_eth = PythonOperator(
        task_id="fetch_krw_eth",
        python_callable=_fetch_ohlcvs,
        templates_dict={
            "start_time": "{{ ts_nodash }}",
            "end_time": "{{ data_interval_end | ts_nodash }}",
            "coin_ticker" : "KRW-ETH"
        },
    )
    
    def _insert_ohlcvs(templates_dict, **kwargs):
        logger = logging.getLogger(__name__)
        mongo_client = _get_mongo_client()
        
        start_time = templates_dict['start_time']
        
        tickers = os.listdir(FILE_BASE_DIR)
        for ticker in tickers:
            file_path = f"{FILE_BASE_DIR}/{ticker}/{start_time}.json"
            
            with open(file_path, 'r') as file:
                json_dicts = json.load(file)
            
            logger.info(f"="*100)
            logger.info(f"{type(json_dicts)}")
            logger.info(f"{json_dicts}")
            logger.info(f"="*100)

            json_dicts = _json_strptime(json_dicts)
            db = mongo_client.test_db
            db[ticker].insert_many(json_dicts)
            db[ticker].create_index('candle_date_time_kst')
        mongo_client.close()

    insert_jsons = PythonOperator(
        task_id="insert_jsons",
        python_callable=_insert_ohlcvs,
        templates_dict={
            "start_time": "{{ ts_nodash }}",
        }
    )

[fetch_usdt_btc, fetch_krw_btc, fetch_usdt_eth, fetch_krw_eth] >> insert_jsons
