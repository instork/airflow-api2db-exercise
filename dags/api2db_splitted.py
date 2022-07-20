import datetime as dt
from typing import Dict, List

import pendulum
from airflow.models import DAG
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

dag = None
with DAG(
    dag_id="api2db_splitted",
    description="Get ohlcv data using upbit API",
    start_date=dt.datetime(2022, 7, 20, 0, 0, tzinfo=KST),
    end_date=dt.datetime(2022, 7, 23, 0, 0, tzinfo=KST),
    schedule_interval=SCHEDULE_INTERVAL,
):

    fetch_cmd = "python /home/airflow/airflow/dags/upbit/api2json.py \
                --minute_interval params.minute_interval \
                --get_cnt params.get_cnt \
                --start_time params.start_time \
                --file_base_dir params.file_base_dir \
                --coin_ticker params.coin_ticker \
                --req_time_interval params.req_time_interval"

    fetch_usdt_btc = BashOperator(
        task_id="fetch_usdt_btc",
        bash_command=fetch_cmd,
        params=fetch_template_dicts["USDT-BTC"],
        dag = dag
    )
    fetch_krw_btc = BashOperator(
        task_id="fetch_krw_btc",
        bash_command=fetch_cmd,
        params=fetch_template_dicts["KRW-BTC"],
        dag = dag
    )
    fetch_usdt_eth = BashOperator(
        task_id="fetch_usdt_eth",
        bash_command=fetch_cmd,
        params=fetch_template_dicts["USDT-ETH"],
        dag = dag
    )
    fetch_krw_eth = BashOperator(
        task_id="fetch_krw_eth",
        bash_command=fetch_cmd,
        params=fetch_template_dicts["KRW-ETH"],
        dag = dag
    )

    insert_jsons = BashOperator(
        task_id="insert_jsons",
        bash_command="python mongodb/json2mongo.py --start_time {{ ts_nodash }} --file_base_dir {{ params.file_base_dir }} ",
        params = {
            "file_base_dir": FILE_BASE_DIR,
        },
        dag = dag
    )


[fetch_usdt_btc, fetch_krw_btc, fetch_usdt_eth, fetch_krw_eth] >> insert_jsons

# logger.info(f"="*100)
# logger.info(f"{type(json_dicts)}")
# logger.info(f"{json_dicts}")
# logger.info(f"="*100)
