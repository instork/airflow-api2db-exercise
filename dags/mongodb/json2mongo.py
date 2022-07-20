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


# @task
def insert_ohlcvs(templates_dict, **kwargs):
    logger = logging.getLogger(__name__)

    file_base_dir = templates_dict["file_base_dir"]
    start_time = templates_dict["start_time"]
    tickers = os.listdir(file_base_dir)

    mongo_client = _get_mongo_client()

    for ticker in tickers:
        file_path = f"{file_base_dir}/{ticker}/{start_time}.json"

        with open(file_path, "r") as file:
            json_dicts = json.load(file)

        json_dicts = json_strptime(json_dicts)
        db = mongo_client.test_db
        db[ticker].insert_many(json_dicts)
        db[ticker].create_index("candle_date_time_kst")
    mongo_client.close()
