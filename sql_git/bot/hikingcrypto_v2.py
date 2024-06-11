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



# print(df[(df['Name'] == 'K') & (df['Remaining_qty'].astype('float64') >0 )][['CCY','Holding_Value']].to_string())
# print(df.index)
# print(df[df['concat'] == 'kienfilda'][' Avg_Purchasing_Price '].values[0])
# df.to_csv(r"test.csv")

# print(df_price = df[df['concat'] == str(message.text).split()[1].lower()]['Avg_Purchasing_Price'].values[0])
# print(df['concat'])



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



# df = pd.read_excel(r"https://docs.google.com/spreadsheets/d/e/2PACX-1vSlvIzYPhhAe7iPqtW_szdcxCIARxgnBBy_toKIeT6MM8gDSrth3M1zcVl50uGGLgmS5yhNjAD3rc6y/pub?output=xlsx",sheet_name = 'data')

bot = telebot.TeleBot("1550527239:AAGeWZiYIfVkoMXLL3yJBGD5RXfL3_9NLGI")


def handle_messages(messages):
	pic_list = ["https://i.imgur.com/meNzfAS.png","https://i.imgur.com/jWunnho.png","https://i.imgur.com/nb7Sv8b.png","https://i.imgur.com/zQtWB3c.png"
,"https://i.imgur.com/wpRt7Ge.png","https://i.imgur.com/mZDIETB.png","https://i.imgur.com/xKsFYLh.png",
"https://i.imgur.com/YWDBwhe.png"]
	scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

	credentials = ServiceAccountCredentials.from_json_keyfile_name(
	         'creds.json', scope) # Your json file here

	gc = gspread.authorize(credentials)

	# wks = gc.open_by_key('1W2_tdPIPJ8rNPMi4eYcN8NOzq2pEqM_83n85CT3Nqxo').worksheet("summary")
	wks = gc.open("HIKING Tracking Logs").sheet1

	data = wks.get_all_values()
	headers = data.pop(0)

	df = pd.DataFrame(data, columns=headers)
	df['concat'] = (df['Name']+df['CCY']).str.lower()


	num_rand = random.randint(0, len(pic_list)-1)
	pic = pic_list[num_rand]
	global search_key
	try:
		for message in messages:
			if len(str(message.text).split()) >= 2 and str(message.text).split()[0].lower() == '!p':
				price = call_coin_price(crypto = str(message.text).split()[1].lower())
				result = f"$ {price:,}"
				# text_ = 
				bot.send_message(chat_id=message.chat.id,text=str('---***---\ngiá {} nè sếp: '.format(str(message.text).split()[1].upper()))+result,parse_mode='HTML')

			if str(message.text).lower() == 'good job':
				bot.send_message(chat_id=message.chat.id,text=str('---***---\n')+str('cảm ơn sếp ạ'))
			if len(str(message.text).split()) == 3 and str(message.text).split()[0].lower() == '!c':

				holder = str(message.text).split()[1].lower()
				crypto = str(message.text).split()[2].lower()
				concat = holder+crypto
				cost = df[df['concat'] == concat][' Avg_Purchasing_Price '].values[0]
				buy_qty = df[df['concat'] == concat]['Buy_quantity'].values[0]
				remain_qty = df[df['concat'] == concat]['Remaining_qty'].values[0]
				total_cost = df[df['concat'] == concat][' Total_cost '].values[0]
				market = df[df['concat'] == concat]['Exchange'].values[0]
				sale_price = df[df['concat'] == concat][' Avg_Selling_Price '].values[0]
				print(sale_price)

					
				qty_sale = df[df['concat'] == concat]['Sell_quantity'].values[0]
				total_sale = df[df['concat'] == concat][' Total_sales '].values[0]
				pnl = df[df['concat'] == concat][' PnL '].values[0]
				total_value = df[df['concat'] == concat]['Holding_Value'].values[0]
				market_price = call_coin_price(crypto = crypto)
				market_value = float(remain_qty) * float(market_price)

				num_digit = 2

				print('done')
				net_position = market_value + float(pnl)
				percent_net_position = (net_position / abs(float(total_cost)))*100
				percent_net_position = num_finance(percent_net_position,crypto = crypto)
				net_position = num_finance(net_position,crypto = crypto )
				avg_sale_price = num_finance(sale_price,crypto = crypto)
				total_cost = num_finance(total_cost, crypto = crypto)
				cost = num_finance(cost, crypto = crypto)
				buy_qty = num_finance(buy_qty, crypto = crypto)
				qty_sale = num_finance(qty_sale, crypto= crypto)
				total_sale = num_finance(total_sale , crypto = crypto)
				pnl = num_finance(pnl, crypto = crypto)
				remain_qty = num_finance(remain_qty, crypto = crypto)
				total_value = num_finance(total_value, crypto=crypto)
				market_value = num_finance(market_value,crypto = crypto)
				market_price = num_finance(market_price,crypto = 'none')


				text = '''<b color = "red">Shark {} - {}</b>
				\nExchange: <b>{}</b> \n\
				\nCost: $ {} | Qty: {} \
				\nTotal Cost: $ {} \n\
				\nSale: $ {} | Qty: {}\
				\nTotal Sale: $ {} \n \
				\nPnL: $ {} \
				\nRemain Qty: {} \
				\nHolding Value: $ {}\

				\nMarket Price: $ {}\
				\nMarket Value: $ {}\
				\nNet Position: $ {} | <b>{} %</b>
				\nJoin @hikingcryptobot\
				'''.format(holder.upper(),crypto.upper(),market,cost,buy_qty,total_cost,avg_sale_price,qty_sale,total_sale,pnl,remain_qty,total_value,market_price,market_value,net_position,percent_net_position)

				# bot.reply_to(message,text)
				bot.send_message(chat_id=message.chat.id,text = text,parse_mode='HTML')
				# print(df_price)
			elif len(str(message.text).split()) == 3 and str(message.text).split()[0].lower() == '!r' and str(message.text).split()[2].lower() == 'all':
				holder = str(message.text).split()[1].upper()
				holding_value_ = df[(df['Name'] == holder) & (df['Remaining_qty'].astype('float64') >0 )]['Holding_Value'].astype('float64').sum()
				df['Holding_Value_'] = '$ ' + df['Holding_Value']
				list_coin = df[(df['Name'] == holder) & (df['Remaining_qty'].astype('float64') >0 )][['CCY','Holding_Value_']].to_string(header=False,index_names =False)
				print(holding_value_)
				holding_value_ = num_finance(holding_value_,crypto = 'none')
				print(list_coin)
				text_ = '''<b>Shark {} - Portfolio</b>\n\n'''.format(holder.upper())+list_coin + '''\n<b>Total Value: $ {}</b>'''.format(holding_value_)
				bot.send_message(chat_id=message.chat.id,text = text_,parse_mode='HTML')



			elif re.search(str(message.text).lower(),r"sua nao bot|bot sua di coi|sủa"):
				bot.send_message(chat_id=message.chat.id,text=str('---***---\n')+str('gâu gâu gâu'))
			elif re.search(str(message.text).lower(),r"đưa tay|dua tay|dance|nhay|nhảy"):
				bot.send_message(chat_id=message.chat.id,text=str('---***---\n')+str("https://www.youtube.com/watch?v=TpmVzBcP70U")+str('\nđưa tay đây nào. mãi bên nhau bạn nhớ'))
			elif re.search(str(message.text).lower(),r"rên|ren"):
				bot.send_message(chat_id=message.chat.id,text=str('---***---\n')+str('kimochi ư ư ư'))
				bot.send_message(chat_id=message.chat.id,text=pic)
			elif re.search(str(message.text).lower(),r"gáy|gay|ỉa"):
				bot.send_message(chat_id=message.chat.id,text=str('---***---\n')+ str('ò ó o o'))
			elif re.search(str(message.text).lower(),r"bot ra đây coi|bot ra day coi|bot dau|bot"):
				bot.send_message(chat_id=message.chat.id,text='---***---\nsếp cần gì ạ')
			elif re.search(str(message.text).lower(),r"bye|pp"):
				bot.send_message(chat_id=message.chat.id,text='---***---\nbye sếp ạ')
			elif re.search(str(message.text).lower(),r"moon|go to the moon"):
				bot.send_message(chat_id=message.chat.id,text='---***---\nsắp lên đỉnh rồi mấy sếp chờ đi')
			elif re.search(str(message.text).lower(),r"nghĩa loz|sharkn|shark nghĩa"):
				bot.send_message(chat_id=message.chat.id,text='---****---\nshark N đu đỉnh ghê vcl')
			elif re.search(str(message.text).lower(),r"chó n|chó nghĩa|cho n"):
				bot.send_message(chat_id=message.chat.id,text='---****---\nchó N dằn non ngu vcl\nhttps://i.imgur.com/wMfAZtx.png')
			elif re.search(str(message.text).lower(),r"chó h|chó hiếu|cho h|cho k|chó k"):
				bot.send_message(chat_id=message.chat.id,text='---****---\nmay ngu vc')
			elif re.search(str(message.text).lower(),r"chó"):
				bot.send_message(chat_id=message.chat.id,text='---****---\ntụi mày ngu vcc')
			elif re.search(str(message.text).lower(),r"giỏi|gioi|good job|good"):
				bot.send_message(chat_id=message.chat.id,text='---****---\ncảm ơn sếp ạ')
			elif re.search(str(message.text).lower(),r"=))"):
				bot.send_message(chat_id=message.chat.id,text='---****---\ncười cc nè')
			elif re.search(str(message.text).lower(),r"láo|lao|láo vãi|bot láo|bot lao"):
				bot.send_message(chat_id=message.chat.id,text='---****---\ncó mày láo ấy')
			elif re.search(str(message.text).lower(),r"gsheet link"):
				bot.send_message(chat_id=message.chat.id,text='---****---\n"Database:\nhttps://docs.google.com/spreadsheets/d/1W2_tdPIPJ8rNPMi4eYcN8NOzq2pEqM_83n85CT3Nqxo/edit?usp=sharing')
			elif re.search(str(message.text).lower(),r"goctienao|keo coin|kèo|gta"):
				bot.send_message(chat_id=message.chat.id,text='---****---\nAll kèo ở đây hết các sếp nhé\n"https://docs.google.com/spreadsheets/d/1qmOiwAOVUaf5rtzWd4Hz3y9bMUMcs5LOkm9EzK3yXLo/htmlview?pru=AAABeD_Dh_4*aM3VUxuXWl-mjQb2y7SPFA#gid=0"')

			else:
				pass
	except Exception as ex:
		print(ex)


bot.set_update_listener(handle_messages)
bot.polling()

# wakeup = time.time()

# print(wakeup)

# print('running')

# while True:
# 	print('start')
# 	wakeup +=  10
# 	for i in range(500):
# 		if time.time() > wakeup:
# 			break
# 		while time.time() < wakeup:
# 			print('start sleep')
# 			time.sleep(1)
			

# # print('running')
# # bot.polling()
