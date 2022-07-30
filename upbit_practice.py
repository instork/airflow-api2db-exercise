from
from pprint import pprint

MINUTE_INTERVAL = 60
GET_CNT = 1
fetch_base_template_dict = {
    "minute_interval": MINUTE_INTERVAL,
    "get_cnt": GET_CNT,
    "start_time": "20220730T000000",
    "coin_ticker": "USDT-BTC"
}


result_dict = fetch_minute_ohlcvs(fetch_base_template_dict)
pprint(result_dict)

