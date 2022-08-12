import logging
import os

from dotenv import load_dotenv
from pymongo import MongoClient
from de.utils.timeutils import get_datetime_from_ts, UTC

load_dotenv("/tmp/mongo.env")

# turn this on when test is done
INDEX_UNIQUE = False


def _get_mongo_client():
    user = os.getenv("MONGODB_USER")
    pwd = os.getenv("MONGODB_PWD")
    host = os.getenv("MONGODB_HOST")
    port = os.getenv("MONGODB_PORT")
    client = MongoClient(f"mongodb://{user}:{pwd}@{host}:{port}")
    return client


def insert_ohlcvs(templates_dict, **context):
    logger = logging.getLogger(__name__)

    # 이미 UTC 로 변환됨, 20220601T040000 <- dt.datetime(2022, 6, 1, 0, 0, tzinfo=ETZ)
    # start_time = "20220601T040000"
    start_time = templates_dict["start_time"]
    db_name = templates_dict["db_name"]
    collection_name = templates_dict["collection_name"]

    utc_time = get_datetime_from_ts(start_time, get_day_before=False, tz=UTC)

    prev_task_id = next(iter(context["task"].upstream_task_ids))
    json_dicts = context["task_instance"].xcom_pull(task_ids=prev_task_id)
    logger.info(json_dicts)

    # 동부시간의 서머타임으로 인해 서머타임 해제 시, 겹치기 때문에 etz_time 은 인덱스가 될 수 없음
    # 업비트 서버점검으로 인해 candle_date_time_utc 는 인덱스가 될 수 없음(겹침)
    for d in json_dicts:
        d.update({"utc_time": utc_time})

    # Get database
    mongo_client = _get_mongo_client()
    db = mongo_client[db_name]

    # Make collection
    if collection_name not in db.list_collection_names():
        try:
            db.create_collection(collection_name)
            db[collection_name].create_index([("utc_time", 1)], unique=INDEX_UNIQUE)
        except Exception as e:
            logger.info(e)

    db[collection_name].insert_many(json_dicts)
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