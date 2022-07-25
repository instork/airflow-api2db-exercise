import datetime as dt
from time import timezone
from typing import Dict, List

import pendulum

KST = pendulum.timezone("Asia/Seoul")
ETZ = pendulum.timezone("US/Eastern")
UTC = pendulum.timezone("UTC")


def str2pend_datetime(start_time: str, format: str, tz_code="UTC"):
    """string to pendulum datetime."""
    start_time = pendulum.from_format(start_time, format, tz=pendulum.timezone(tz_code))
    return start_time


def str2datetime(s: str, format: str):
    """string to datetime."""
    return dt.datetime.strptime(s, format)


def json_strptime(
    json_dicts: List[Dict],
    dict_keys: List[str] = ["candle_date_time_utc", "candle_date_time_kst"],
):
    """dictionary's string datetime to datetime datetime."""
    for key in dict_keys:
        for data in json_dicts:
            new_time = str2datetime(data[key], "%Y-%m-%dT%H:%M:%S")
            data.update({key: new_time})
    return json_dicts


def pend2datetime(p_time: pendulum.datetime):
    """pedulum datetime to datetime datetime."""
    datetime_string = p_time.to_datetime_string()
    dt_datetime = dt.datetime.fromisoformat(datetime_string)
    return dt_datetime


def get_str_date_before_from_ts(
    ts: str, date_format: str = "%Y-%m-%d", tz: pendulum.timezone = ETZ
) -> str:
    """
    Get string datetime from ts(start_time). start time automatically converted to UCT.
    Chagege to tz(ETZ, default) and returns to string date_format
    """
    start_time = str2pend_datetime(ts, "YYYYMMDDTHHmmss", "UCT")
    etz_time = tz.convert(start_time).subtract(minutes=1)  # to get data day before
    start_date = etz_time.strftime(date_format)
    return start_date


def get_datetime_from_ts(
    ts: str, get_day_before=False, tz: pendulum.timezone = ETZ
) -> dt.datetime:
    """
    Get dt.datetime form ts(start_time).
    """
    start_time = str2pend_datetime(ts, "YYYYMMDDTHHmmss", "UCT")
    etz_time = tz.convert(start_time)
    if get_day_before:
        etz_time = etz_time.subtract(minutes=1)
    etz_time = pend2datetime(etz_time)
    return etz_time
