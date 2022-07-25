import json
import logging
import os
from typing import Dict, List

from airflow.decorators import task
from dotenv import load_dotenv
from pymongo import MongoClient
from utils.timeutils import get_datetime_from_ts, json_strptime

load_dotenv("/tmp/mongo.env")

# turn this on when test is done
INDEX_UNIQUE = False


def _get_mongo_client():
    """Get mongo client."""
    user = os.getenv("MONGODB_USER")
    pwd = os.getenv("MONGODB_PWD")
    host = os.getenv("MONGODB_HOST")
    port = os.getenv("MONGODB_PORT")
    client = MongoClient(f"mongodb://{user}:{pwd}@{host}:{port}")
    return client


def insert_ohlcvs(templates_dict, **context):
    logger = logging.getLogger(__name__)
    # 이미 UTC로 변환됨, 20220601T040000 <- dt.datetime(2022, 6, 1, 0, 0, tzinfo=ETZ)
    start_time = templates_dict["start_time"]
    db_name = templates_dict["db_name"]
    etz_time = get_datetime_from_ts(start_time, get_day_before=False)

    prev_task_id = next(iter(context["task"].upstream_task_ids))
    json_dicts = context["task_instance"].xcom_pull(task_ids=prev_task_id)
    json_dicts = json_strptime(json_dicts)
    ticker = json_dicts[0]["market"]

    for d in json_dicts:
        d.update({"etz_time": etz_time})

    # Get database
    mongo_client = _get_mongo_client()
    db = mongo_client[db_name]

    # Make collection
    if ticker not in db.list_collection_names():
        try:
            db.create_collection(ticker)
            db[ticker].create_index([("etz_time", 1)], unique=INDEX_UNIQUE)
        except Exception as e:
            logger.info(e)

    db[ticker].insert_many(json_dicts)

    mongo_client.close()


def insert_single(templates_dict, **context):
    logger = logging.getLogger(__name__)
    start_time = templates_dict["start_time"]
    db_name = templates_dict["db_name"]
    collection_name = templates_dict["collection_name"]
    etz_time = get_datetime_from_ts(start_time, get_day_before=True)

    prev_task_id = next(iter(context["task"].upstream_task_ids))
    single_dict = context["task_instance"].xcom_pull(task_ids=prev_task_id)
    single_dict.update(etz_time=etz_time)
    # Get database
    mongo_client = _get_mongo_client()
    db = mongo_client[db_name]
    if collection_name not in db.list_collection_names():
        try:
            db.create_collection(collection_name)
            db[collection_name].create_index([("etz_time", 1)], unique=INDEX_UNIQUE)
        except Exception as e:
            logger.info(e)

    db[collection_name].insert_one(single_dict)
