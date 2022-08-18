import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator
from de.mongodb.crpcomp2mongo import insert_ohlcvs
from de.crypcomp.request import fetch_hour_ohlcvs
from de.utils.timeutils import UTC

from de.utils.config import config

########################### Set Configs ###########################
SCHEDULE_INTERVAL = config.APP_PROPERTIES.crpcomp_schedule_interval  # every hour

## mongodb
mongo_templates_dict = {"db_name": config.APP_PROPERTIES.mongodb_name,
                        "start_time": "{{ data_interval_end }}"}
GET_CNT = 1

fetch_base_template_dict = {
    "limit": GET_CNT,
    "aggregate": 1,
    "toTs": "{{ data_interval_end }}",
}

tickers = ["USDT-BTC", "USDT-ETH"]

fetch_template_dicts = {}
mongo_templates_dicts = {}

# ticker = tickers[0]
for ticker in tickers:
    tsym, fsym = ticker.split("-")
    fetch_base_template_dict.update({"tsym": tsym, "fsym": fsym})
    mongo_templates_dict.update({"collection_name": ticker})

    mongo_templates_dicts[ticker] = mongo_templates_dict.copy()
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
        "retries": 0,
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
    templates_dict=mongo_templates_dicts["USDT-BTC"],
)

insert_eth_ohlcvs_task = PythonOperator(
    task_id="insert_ohlcvs_eth",
    python_callable=insert_ohlcvs,
    dag=dag,
    templates_dict=mongo_templates_dicts["USDT-ETH"],
)

fetch_usdt_btc >> insert_btc_ohlcvs_task
fetch_usdt_eth >> insert_eth_ohlcvs_task
