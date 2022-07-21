import json
import logging
import os

from airflow.decorators import task
from dotenv import load_dotenv
from pymongo import MongoClient
from utils.timeutils import json_strptime

load_dotenv("/tmp/.env")


def _get_mongo_client():
    """Get mongo client."""
    user = os.getenv("MONGODB_USER")
    pwd = os.getenv("MONGODB_PWD")
    host = os.getenv("MONGODB_HOST")
    port = os.getenv("MONGODB_PORT")
    client = MongoClient(f"mongodb://{user}:{pwd}@{host}:{port}")
    return client


def insert_ohlcvs(**context):
    logger = logging.getLogger(__name__)
    prev_task_id = next(iter(context["task"].upstream_task_ids))

    mongo_client = _get_mongo_client()

    json_dicts = context["task_instance"].xcom_pull(task_ids=prev_task_id)
    json_dicts = json_strptime(json_dicts)
    ticker = json_dicts[0]["market"]
    db = mongo_client.test_db
    db[ticker].insert_many(json_dicts)
    db[ticker].create_index("candle_date_time_kst")

    mongo_client.close()
