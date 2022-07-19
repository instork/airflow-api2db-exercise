import argparse
import json
import logging
import os
import time
from typing import Dict, List

import pendulum
import requests
from airflow.decorators import task
from utils.timeutils import ts_2_pendulum_datetime

def get_params():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--minute_interval",
        type=int,
    )
    parser.add_argument(
        "--get_cnt",
        type=int,
    )
    parser.add_argument(
        "--start_time",
        type=str,
    )
    parser.add_argument(
        "--file_base_dir",
        type=str,
    )
    parser.add_argument(
        "--coin_ticker",
        type=str,
    )
    parser.add_argument(
        "--req_time_interval",
        type=float,
    )
    args = parser.parse_args()
    return args


def _get_minutes_ohlcvs(
    interval: int,
    ticker: str,
    to: pendulum.datetime,
    count: int,
    req_time_interval: float = 0,
    logger: logging.RootLogger = None,
    utc_timezone=pendulum.timezone("UTC"),
) -> List[Dict]:
    """Get ohlcvs until datetime 'to'."""
    to = utc_timezone.convert(to).strftime("%Y-%m-%d %H:%M:%S")
    url = f"https://api.upbit.com/v1/candles/minutes/{interval}?market={ticker}&to={to}&count={count}"
    headers = {"Accept": "application/json"}
    time.sleep(req_time_interval)
    response = requests.get(url, headers=headers)
    response = json.loads(response.text)
    return response


def _fetch_ohlcvs(templates_dict, **kwarg):
    """Get ohlcvs and save."""
    logger = logging.getLogger(__name__)
    start_time = templates_dict["start_time"]  # "2022-07-18T07:43:15.165980+00:00"
    req_time_interval = templates_dict["req_time_interval"]
    file_base_dir = templates_dict["file_base_dir"]
    minute_interval = templates_dict["minute_interval"]
    get_cnt = templates_dict["get_cnt"]
    coin_ticker = templates_dict["coin_ticker"]

    datetime_start_time = ts_2_pendulum_datetime(start_time)

    output_path = f"{file_base_dir}/{coin_ticker}/{start_time}.json"
    logger.info(f"Fetching ohlcvs til {start_time}")

    ohlcvs = _get_minutes_ohlcvs(
        minute_interval,
        coin_ticker,
        datetime_start_time,
        get_cnt,
        req_time_interval,
        logger,
    )

    logger.info(f"Fetched {len(ohlcvs)} ohlcvs")
    logger.info(f"Writing ohlcvs to {output_path}")

    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)

    with open(output_path, "w") as file_:
        json.dump(ohlcvs, fp=file_)

def main(args):
    _fetch_ohlcvs(args)

if __name__ == "__main__":
    args = get_params()
    main(args)
