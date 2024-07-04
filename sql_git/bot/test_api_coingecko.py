import telebot
import pandas as pd
import re
import random
from pycoingecko import CoinGeckoAPI
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd



def call_coin_price(crypto,currency='usd'):
	# global id_
	id_ = 'none'
	cg = CoinGeckoAPI()
	print(crypto)
	list_coin = cg.get_coins_list()
	symbol = crypto
	for i, list_sym in enumerate(list_coin):
		if(symbol == list_sym['symbol']) and i >0:
			print('i: ',i)
			id_ = list_coin[i]['id']
	currency = currency
	if id_ == 'none':
		return 'có cái loz'
	print(id_)
	price = cg.get_price(ids=id_, vs_currencies=currency)
	result = price[id_][currency]
	return f"$ {result:,}"

cg = CoinGeckoAPI()
# price = cg.get_price(ids='dia-data', vs_currencies='usd')
list_coin = cg.get_coins_list()
# chart = cg.get_coin_market_chart_by_id(id =['bitcoin','ethereum'],vs_currency='usd',days = 1)
price = cg.get_price(ids = ['usc','ethereum'],vs_currencies='usd')
print(price)