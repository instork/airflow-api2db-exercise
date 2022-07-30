import requests

url = "https://api.bithumb.com/public/candlestick/BTC_USDT/24h"

headers = {"Accept": "application/json"}

response = requests.get(url, headers=headers)

print(response.text)