# TODO: https://github.com/astronomer/airflow-guide-passing-data-between-tasks
# cf. https://www.astronomer.io/guides/airflow-passing-data-between-tasks
# TODO: USE Mongo DB container or Package apache-airflow-providers-mongo
# cf. https://airflow.apache.org/docs/apache-airflow-providers-mongo/stable/index.html
# TODO: change python operator to bash operator, for dependency on database and etc.
# ex. https://github.com/vineeths96/Data-Engineering-Nanodegree/blob/master/Project%206%20Capstone%20Project/airflow/dag.py

import os
import json
import logging
import pyupbit
import datetime as dt
from typing import Dict

from airflow import DAG
from airflow.operators.python import PythonOperator

from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv('/tmp/.env')


def _get_mongo_client():
    user = os.getenv("MONGODB_USER") 
    pwd = os.getenv("MONGODB_PWD") 
    host = os.getenv("MONGODB_HOST")
    port = os.getenv("MONGODB_PORT")
    client = MongoClient(f'mongodb://{user}:{pwd}@{host}:{port}')
    return client


def _get_ohlcvs(ticker: str, interval: str, count: int) -> Dict:
    """Get ohlcv."""
    ohlcv = pyupbit.get_ohlcv(ticker, interval, count)
    ohlcv = ohlcv.to_dict()
    return ohlcv


def _dict_strftime(dict_keys):
    return [dict_key.strftime('%Y-%m-%d %H:%M:%S') for dict_key in dict_keys]


def _strftime_ohlcvs(ohlcvs):
    for k in ohlcvs.keys():
        ohlcvs[k] = dict(zip(_dict_strftime(ohlcvs[k].keys()), 
                            ohlcvs[k].values()))
    return ohlcvs

def _dict_strptime(dict_keys):
    return [dt.datetime.strptime(dict_key, '%Y-%m-%d %H:%M:%S') for dict_key in dict_keys]


def _strptime_ohlcvs(ohlcvs):
    for k in ohlcvs.keys():
        ohlcvs[k] = dict(zip(_dict_strptime(ohlcvs[k].keys()), 
                            ohlcvs[k].values()))
    return ohlcvs


with DAG(
    dag_id="api2db",
    description="Get ohlcv data using pyupbit",
    start_date=dt.datetime(2022, 7, 16),
    end_date=dt.datetime(2022, 7, 19),
    schedule_interval="@hourly",
) as dag:
    def _fetch_ohlcvs(templates_dict, **kwargs):
        logger = logging.getLogger(__name__)
        start_date = templates_dict["start_date"]
        end_date = templates_dict["end_date"]
        output_path = templates_dict["output_path"]

        coin_ticker = templates_dict["coin_ticker"]
        time_interval = templates_dict["time_interval"]
        cnt = templates_dict["cnt"]

        logger.info(f"Fetching ratings for {start_date} to {end_date}")

        ohlcvs = _get_ohlcvs(coin_ticker, time_interval, cnt)
        ohlcvs = _strftime_ohlcvs(ohlcvs)
        logger.info(f"Fetched {len(ohlcvs)} ratings")
        logger.info(f"Writing ratings to {output_path}")

        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)

        with open(output_path, "w") as file_:
            json.dump(ohlcvs, fp=file_)
    
    fetch_usdt_btc = PythonOperator(
        task_id="fetch_usdt_btc",
        python_callable=_fetch_ohlcvs,
        templates_dict={
            "start_date": "{{ds}}",
            "end_date": "{{next_ds}}",
            "output_path": "/data/python/usdt_btc/{{ds}}.json",
            "coin_ticker" : "USDT-BTC", # "KRW-BTC"
            "time_interval" : "minute1",
            "cnt" : 60
        },
    )
    fetch_krw_btc = PythonOperator(
        task_id="fetch_krw_btc",
        python_callable=_fetch_ohlcvs,
        templates_dict={
            "start_date": "{{ds}}",
            "end_date": "{{next_ds}}",
            "output_path": "/data/python/krw_btc/{{ds}}.json",
            "coin_ticker" : "KRW-BTC", # "KRW-BTC"
            "time_interval" : "minute1",
            "cnt" : 60
        },
    )
    fetch_usdt_eth = PythonOperator(
        task_id="fetch_usdt_eth",
        python_callable=_fetch_ohlcvs,
        templates_dict={
            "start_date": "{{ds}}",
            "end_date": "{{next_ds}}",
            "output_path": "/data/python/usdt_eth/{{ds}}.json",
            "coin_ticker" : "USDT-ETH", # "KRW-BTC"
            "time_interval" : "minute1",
            "cnt" : 60
        },
    )
    fetch_krw_eth = PythonOperator(
        task_id="fetch_krw_eth",
        python_callable=_fetch_ohlcvs,
        templates_dict={
            "start_date": "{{ds}}",
            "end_date": "{{next_ds}}",
            "output_path": "/data/python/krw_eth/{{ds}}.json",
            "coin_ticker" : "KRW-ETH", # "KRW-BTC"
            "time_interval" : "minute1",
            "cnt" : 60
        },
    )
    
    def _insert_ohlcvs(templates_dict, **kwargs):
        mongo_client = _get_mongo_client()
        file_path = templates_dict['input_path']
        collection_name = templates_dict['collection_name']
        with open(file_path, 'r') as file:
            data = json.load(file)

        ohlcvs = _strptime_ohlcvs(data)
        time_idx = ohlcvs[list(ohlcvs.keys())[0]].keys()
        ohlcvs = {k:list(v.values()) for k,v in ohlcvs.items()}
        ohlcvs.update(time_idx=time_idx)
        ohlcvs = [dict(zip(ohlcvs,t)) for t in zip(*ohlcvs.values())]
        db = mongo_client.test_db
        db[collection_name].insert_many(ohlcvs)
        mongo_client.close()

    insert_usdt_btc = PythonOperator(
        task_id="insert_usdt_btc",
        python_callable=_insert_ohlcvs,
        templates_dict={
            "input_path": "/data/python/usdt_btc/{{ds}}.json",
            "collection_name": "usdt_btc"
        }
    )
    insert_krw_btc = PythonOperator(
        task_id="insert_krw_btc",
        python_callable=_insert_ohlcvs,
        templates_dict={
            "input_path": "/data/python/krw_btc/{{ds}}.json",
            "collection_name": "krw_btc"
        }
    )
    insert_usdt_eth = PythonOperator(
        task_id="insert_usdt_eth",
        python_callable=_insert_ohlcvs,
        templates_dict={
            "input_path": "/data/python/usdt_eth/{{ds}}.json",
            "collection_name": "usdt_eth"
        }
    )
    insert_krw_eth = PythonOperator(
        task_id="insert_krw_eth",
        python_callable=_insert_ohlcvs,
        templates_dict={
            "input_path": "/data/python/krw_eth/{{ds}}.json",
            "collection_name": "krw_eth"
        }
    )

fetch_usdt_btc >> insert_usdt_btc
fetch_krw_btc >> insert_krw_btc
fetch_usdt_eth >> insert_usdt_eth
fetch_krw_eth >> insert_krw_eth

