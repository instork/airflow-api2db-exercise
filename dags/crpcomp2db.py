import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator
from mongodb.data2mongo import insert_ohlcvs
from crypcomp.request import fetch_minute_ohlcvs
from utils.timeutils import UTC

########################### Set Configs ###########################
SCHEDULE_INTERVAL = "0 * * * *"  # every hour
## mongodb
mongo_templates_dict = {"db_name": "test_db", "start_time": "{{ ts_nodash }}"}
## upbit
MINUTE_INTERVAL = 60
GET_CNT = 1
timestamp = ""

parameters = {'fsym': "BTC",
              'tsym': "USDT",
              'limit': 1,
              'aggregate': 1,
              'toTs': timestamp}


fetch_base_template_dict = {
    "limit": GET_CNT,
    "aggregate": 1,
    "start_time": "{{ ts_nodash }}",
}

tickers = ["USDT-BTC", "USDT-ETH"]

fetch_template_dicts = {}
# ticker = tickers[0]
for ticker in tickers:
    tsym, fsym = ticker.split("-")
    fetch_base_template_dict.update({"tsym": tsym, "fsym": fsym})
    fetch_template_dicts[ticker] = fetch_base_template_dict.copy()
################################################################

dag = DAG(
    dag_id="crpcomp2db",
    description="Get ohlcv data using cryptocompare API",
    start_date=dt.datetime(2020, 1, 1, 0, 0, tzinfo=UTC),
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=4,
    default_args={
        "depends_on_past": True,
        "retries": 3,
        "retry_delay": dt.timedelta(minutes=2),
    },
)

fetch_usdt_btc = PythonOperator(
    task_id="fetch_usdt_btc",
    python_callable=fetch_hour_ohlcvs,
    templates_dict=fetch_template_dicts["USDT-BTC"],
    dag=dag,
)

fetch_usdt_eth = PythonOperator(
    task_id="fetch_usdt_eth",
    python_callable=fetch_hour_ohlcvs,
    templates_dict=fetch_template_dicts["USDT-ETH"],
    dag=dag,
)

insert_btc_ohlcvs_task = PythonOperator(
    task_id="insert_ohlcvs_btc",
    python_callable=insert_ohlcvs,
    dag=dag,
    templates_dict=mongo_templates_dict,
)

insert_eth_ohlcvs_task = PythonOperator(
    task_id="insert_ohlcvs_eth",
    python_callable=insert_ohlcvs,
    dag=dag,
    templates_dict=mongo_templates_dict,
)

fetch_usdt_btc >> insert_btc_ohlcvs_task
fetch_usdt_eth >> insert_eth_ohlcvs_task
