import datetime as dt
from typing import Dict, List

import pendulum


def ts_2_pendulum_datetime(start_time: str):
    """Timestamp string to Pendulum date time."""
    start_time = start_time.split(".")[0]
    start_time = pendulum.from_format(start_time, "YYYYMMDDTHHmmss")
    return start_time


def json_strptime(
    json_dicts: List[Dict],
    dict_keys: List[str] = ["candle_date_time_utc", "candle_date_time_kst"],
):
    for key in dict_keys:
        for data in json_dicts:
            data.update({key: dt.datetime.strptime(data[key], "%Y-%m-%dT%H:%M:%S")})
    return json_dicts
