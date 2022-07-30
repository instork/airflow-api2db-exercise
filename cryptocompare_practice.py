import pandas as pd
import matplotlib.pyplot as plt
import requests
import pendulum

import datetime
from pprint import pprint

pd.set_option('expand_frame_repr', False)

url = 'https://min-api.cryptocompare.com/data/v2/histo'
baseurl = url + "hour"

# datetime to timestamp
UTC = pendulum.timezone("UTC")
curr_dt = datetime.datetime(year=2022, month=3, day=10, hour=3, minute=0, tzinfo=UTC)


timestamp = int(round(curr_dt.timestamp()))

parameters = {'fsym': "BTC",
              'tsym': "USDT",
              'limit': 1,
              'aggregate': 1,
              'toTs': timestamp}

response = requests.get(baseurl, params=parameters)

data = response.json()['Data']["Data"]
df = pd.DataFrame.from_dict(data)

df["time"] = pd.to_datetime(df["time"], unit="s")






