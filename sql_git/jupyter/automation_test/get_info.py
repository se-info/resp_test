# import requests
# from bs4 import BeautifulSoup
# import json

# a = requests.get('https://www.coingecko.com/')
# soup = BeautifulSoup(a.text,'lxml')
# # soup.find_all('body',{'data-exchange-rate-json'})
# element = soup.find(attrs={'data-exchange-rate-json': True})
# data_exchange_rate_json = element['data-exchange-rate-json']
# print(json.loads(data_exchange_rate_json))