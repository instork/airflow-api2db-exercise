import datetime as dt
from typing import Dict, List

import pendulum
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from mongodb.json2mongo import insert_ohlcvs
from pymongo import MongoClient
from upbit.api2json import fetch_ohlcvs

load_dotenv("/tmp/.env")

# Set Configs
KST = pendulum.timezone("Asia/Seoul")
SCHEDULE_INTERVAL = "*/10 * * * *"
MINUTE_INTERVAL = 1
GET_CNT = 10

UTC_TIMEZONE = pendulum.timezone("UTC")
FILE_BASE_DIR = "/data/upbit"

fetch_base_template_dict = {
    "minute_interval": MINUTE_INTERVAL,
    "get_cnt": GET_CNT,
    "start_time": "{{ ts_nodash }}",
    "file_base_dir": FILE_BASE_DIR,
}
tickers = ["USDT-BTC", "KRW-BTC", "USDT-ETH", "KRW-ETH"]
req_time_intervals = [float(i + 1) for i in range(len(tickers))]
fetch_template_dicts = {}
for ticker, req_time_interval in zip(tickers, req_time_intervals):
    fetch_base_template_dict.update(
        coin_ticker=ticker, req_time_interval=req_time_interval
    )
    fetch_template_dicts[ticker] = fetch_base_template_dict.copy()

dag = DAG(
    dag_id="api2db_splitted",
    description="Get ohlcv data using upbit API",
    start_date=dt.datetime(2022, 7, 19, 16, 0, tzinfo=KST),
    end_date=dt.datetime(2022, 7, 23, 0, 0, tzinfo=KST),
    schedule_interval=SCHEDULE_INTERVAL,
)

fetch_usdt_btc = PythonOperator(
    task_id="fetch_usdt_btc",
    python_callable=fetch_ohlcvs,
    templates_dict=fetch_template_dicts["USDT-BTC"],
    dag=dag,
)
fetch_krw_btc = PythonOperator(
    task_id="fetch_krw_btc",
    python_callable=fetch_ohlcvs,
    templates_dict=fetch_template_dicts["KRW-BTC"],
    dag=dag,
)
fetch_usdt_eth = PythonOperator(
    task_id="fetch_usdt_eth",
    python_callable=fetch_ohlcvs,
    templates_dict=fetch_template_dicts["USDT-ETH"],
    dag=dag,
)
fetch_krw_eth = PythonOperator(
    task_id="fetch_krw_eth",
    python_callable=fetch_ohlcvs,
    templates_dict=fetch_template_dicts["KRW-ETH"],
    dag=dag,
)

insert_jsons = PythonOperator(
    task_id="insert_jsons", python_callable=insert_ohlcvs, dag=dag
)

fetch_usdt_btc >> insert_jsons
fetch_krw_btc >> insert_jsons
fetch_usdt_eth >> insert_jsons
fetch_krw_eth >> insert_jsons

# logger.info(f"="*100)
# logger.info(f"{type(json_dicts)}")
# logger.info(f"{json_dicts}")
# logger.info(f"="*100)
