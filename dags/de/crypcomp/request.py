from typing import Dict, List

import requests


def _get_crpcomp_ohlcvs(
        templates_dict: dict,
        time_interval: str = "hour"
) -> List[Dict]:
    """Get ohlcvs until datetime 'to'."""

    baseurl = f"https://min-api.cryptocompare.com/data/v2/histo" + time_interval
    response = requests.get(baseurl, params=templates_dict)
    data = response.json()['Data']["Data"]
    return data[:-1]


def fetch_hour_ohlcvs(templates_dict):
    """Get ohlcvs and save."""
    import pendulum

    # to_Ts 의 값 YYYYMMDDTHHmmss(UTC) -> timestamp 으로 변경
    # templates_dict["toTs"] = "20220601T040000"
    to_ts = templates_dict["toTs"]
    to_ts_dt = pendulum.from_format(to_ts, "YYYY-MM-DDTHH:mm:ssZ", tz=pendulum.timezone("UTC"))
    to_ts_timestamp = int(round(to_ts_dt.timestamp()))
    templates_dict.update({"toTs": to_ts_timestamp})

    ohlcvs = _get_crpcomp_ohlcvs(templates_dict)
    return ohlcvs
