import telebot
import pandas as pd
import re
import random
from pycoingecko import CoinGeckoAPI
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import numpy as np
import time
# import time


def call_coin_price(crypto,currency='usd'):
	# global id_
	id_ = 'none'
	cg = CoinGeckoAPI()
	print(crypto)
	list_coin = cg.get_coins_list()
	symbol = crypto
	if crypto == 'iota':
		symbol = 'miota'
	else:
		symbol = crypto
	# symbol = crypto
	for i, list_sym in enumerate(list_coin):
		if(symbol == list_sym['symbol'].lower()) and i >0:
			print('i: ',i)
			id_ = list_coin[i]['id']
	currency = currency
	if id_ == 'none':
		return 'có cái loz'
	print(id_)
	price = cg.get_price(ids=id_, vs_currencies=currency)
	result = price[id_][currency]
	return result
def num_finance(number,crypto):
	if crypto == 'btc':
		num_digit = 6
	elif crypto == 'none':
		num_digit = 4
	else:
		num_digit = 2

	if number == '':
		result = '-'
	elif float(number) <0:
		result = str('({:,.{}f})').format(abs(float(number)),num_digit)
	elif float(number) == 0:
		result = '-'
	else:
		result = str('{:,.{}f}').format(float(number),num_digit)
	return result

scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope) # Your json file here

gc = gspread.authorize(credentials)

wks = gc.open_by_key('1W2_tdPIPJ8rNPMi4eYcN8NOzq2pEqM_83n85CT3Nqxo').worksheet("summary")

time.sleep(2)

data = wks.get_all_values()

headers = data.pop(0)

df = pd.DataFrame(data, columns=headers)

print(df)


# df['concat'] = (df['Name']+df['CCY']).str.lower()

# holder = 'K'
# holding_value_ = df[(df['Name'] == holder) & (df['Remaining_qty'].astype('float64') >0 )]['Holding_Value'].astype('float64').sum()
# df['Holding_Value_'] = '$ ' + df['Holding_Value']

# list_coin = df[(df['Name'] == holder) & (df['Remaining_qty'].astype('float64') >0 )][['CCY','Holding_Value_']].to_string(header=False,index_names =False)
# print(holding_value_)
# holding_value_ = num_finance(holding_value_,crypto = 'none')
# print(list_coin)

# text_ = '''<b>Shark {} - Portfolio</b>\n\n'''.format(holder.upper())+list_coin + '''\n<b>Total Value: $ {}</b>'''.format(holding_value_)
