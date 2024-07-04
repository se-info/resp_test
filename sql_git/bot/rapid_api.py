# from pycoingecko import CoinGeckoAPI
# cg = CoinGeckoAPI()

# list_id = cg.get_exchanges_id_name_list()
# name = 'btc'
# # print(list_id)
# chart = cg.get_coin_market_chart_by_id(id='qash', vs_currency='usd',days=1)
# list_coin = cg.get_coins_list()
# symbol = "btc"

# for i, list_sym in enumerate(list_coin):
# 	# print(list_sym['symbol'])
# 	# print
#     if(symbol == list_sym['symbol']):
#         print(i)
#         id_ = list_coin[i]['id']
# currency = 'usd'
# price = cg.get_price(ids=id_, vs_currencies=currency)
# print(price[id_][currency])
# # print(test)
# # print(list_id)
# # print(price)
# # print(chart)

from pycoingecko import CoinGeckoAPI

def call_coin_price(crypto,currency):
	cg = CoinGeckoAPI()
	name = crypto
	list_coin = cg.get_coins_list()
	symbol = "btc"
	for i, list_sym in enumerate(list_coin):
	    if(symbol == list_sym['symbol']):
	        # print(i)
	        id_ = list_coin[i]['id']
	currency = currency
	price = cg.get_price(ids=id_, vs_currencies=currency)
	return str(price[id_][currency])+str(' '+currency)
	
test = call_coin_price(crypto = 'btc',currency='usd')
print(test)