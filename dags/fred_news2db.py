import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator
from fred.request import fetch_fred
from googlenews.request import fetch_news
from mongodb.data2mongo import insert_single
from utils.timeutils import ETZ

########################### Set Configs ###########################
SCHEDULE_INTERVAL = "0 0 * * *"  # At 00:00
## mongodb
news_mongo_templates_dict = {
    "db_name": "test_db",
    "collection_name": "news",
    "start_time": "{{ ts_nodash }}",
}
fred_mongo_templates_dict = {
    "db_name": "test_db",
    "collection_name": "fred",
    "start_time": "{{ ts_nodash }}",
}
## FRED
fred_series_tickers = ["T5YIE", "T5YIFR", "T10YIE", "T10Y2Y", "SP500", "DJIA"]
fred_templates_dict = {
    "fred_series_tickers": fred_series_tickers,
    "start_time": "{{ ts_nodash }}",
}
## Google
queries = {
    "BTC-News": ["BTC", "bitcoin", "bitcoin will"],
    "ETH-News": ["ETH", "ethereum", "ethereum will"],
}
news_templates_dict = {"queries": queries, "start_time": "{{ ts_nodash }}"}
################################################################


dag = DAG(
    dag_id="fred_news2db",
    description="Get fred data and news and load on MongoDB",
    start_date=dt.datetime(2019, 1, 1, 0, 0, tzinfo=ETZ),
    end_date=dt.datetime(2022, 7, 27, 0, 0, tzinfo=ETZ),
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=4,
    default_args={
        "depends_on_past": True,
        "retries": 3,
        "retry_delay": dt.timedelta(minutes=2),
    },
)

fetch_fred_task = PythonOperator(
    task_id="fetch_fred_task",
    python_callable=fetch_fred,
    templates_dict=fred_templates_dict,
    dag=dag,
)

fetch_google_news_task = PythonOperator(
    task_id="fetch_google_news_task",
    python_callable=fetch_news,
    templates_dict=news_templates_dict,
    dag=dag,
)

insert_news_task = PythonOperator(
    task_id="insert_news_to_mongodb",
    python_callable=insert_single,
    templates_dict=news_mongo_templates_dict,
    dag=dag,
)
insert_fred_task = PythonOperator(
    task_id="insert_fred_to_mongodb",
    python_callable=insert_single,
    templates_dict=fred_mongo_templates_dict,
    dag=dag,
)

fetch_fred_task >> insert_fred_task
fetch_google_news_task >> insert_news_task
