import telebot
import pandas as pd
import re
import random
from pycoingecko import CoinGeckoAPI
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd





# print(df[(df['Name'] == 'K') & (df['Remaining_qty'].astype('float64') >0 )][['CCY','Holding_Value']].to_string())
# print(df.index)
# print(df[df['concat'] == 'kienfilda'][' Avg_Purchasing_Price '].values[0])
# df.to_csv(r"test.csv")

# print(df_price = df[df['concat'] == str(message.text).split()[1].lower()]['Avg_Purchasing_Price'].values[0])
# print(df['concat'])

def rename_column(data,col_list):
    for col in col_list: 
        data.rename(columns={col: str(col).strip().lower()},inplace = True)
    return data

scope = ['https://spreadsheets.google.com/feeds',
     'https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive.file']

credentials = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope) # Your json file here


gc = gspread.authorize(credentials)
print(gc)
# gc.login()
# time.sleep(5)



def call_coin_price(crypto,currency='usd'):
	# global id_
	id_ = 'none'
	cg = CoinGeckoAPI()
	print(crypto)
	list_coin = cg.get_coins_list()
	symbol = crypto
	if crypto == 'iota':
		symbol = 'miota'
	elif crypto == 'uti':
		symbol = 'usc'
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

print('running')

def handle_messages(messages):
	pic_list = ["https://i.imgur.com/meNzfAS.png","https://i.imgur.com/jWunnho.png","https://i.imgur.com/nb7Sv8b.png","https://i.imgur.com/zQtWB3c.png"
,"https://i.imgur.com/wpRt7Ge.png","https://i.imgur.com/mZDIETB.png","https://i.imgur.com/xKsFYLh.png",
"https://i.imgur.com/YWDBwhe.png"]
	list_blame = ['con súc vật nghiệt súc','con loz ngu xuẩn','súc vật mọc sừng']
	num_rand_type1 = random.randint(0,len(list_blame)-1)
	list_blame_all = ['shark N đu đỉnh ghê vcl','shark N dằn non ngu vcl','shark H ăn loz','shark K ăn loz','mày ngu nên ra đảo chứ cc gì','call tao ra làm gì ăn loz ko?','chửi chửi cc']
	num_blame_all = random.randint(0,len(list_blame_all)-1)

	game_list = [':right_fist: :right_facing_fist:',':v:',':raised_hand_with_fingers_splayed: :hand_splayed:']
	num_rand_game = random.randint(0,len(game_list)-1)



	num_rand = random.randint(0, len(pic_list)-1)
	pic = pic_list[num_rand]
	global search_key
	try:
		for message in messages:
			if len(str(message.text).split()) >= 2 and str(message.text).split()[0].lower() == '!p':

				price = call_coin_price(crypto = str(message.text).split()[1].lower())
				if price != 'có cái loz':

					result = f"$ {price:,}"
					bot.send_message(chat_id=message.chat.id,text=str('---***---\ngiá {} nè sếp: '.format(str(message.text).split()[1].upper()))+result,parse_mode='HTML')
				else:
					bot.send_message(chat_id=message.chat.id,text=str('---***---\ngiá {} nè sếp: '.format(str(message.text).split()[1].upper()))+price,parse_mode='HTML')

			if str(message.text).lower() == 'good job':
				bot.send_message(chat_id=message.chat.id,text=str('---***---\n')+str('cảm ơn sếp ạ'))
			if len(str(message.text).split()) == 3 and str(message.text).split()[0].lower() == '!c':
				wks = gc.open_by_key('1W2_tdPIPJ8rNPMi4eYcN8NOzq2pEqM_83n85CT3Nqxo').worksheet("summary")
				data = wks.get_all_values()
				print('done-')
				headers = data.pop(0)
				df = pd.DataFrame(data, columns=headers)
				rename_column(data = df,col_list = df.columns)
				df['concat'] = (df['name']+df['ccy']).astype('str').str.lower()

				print('done')
				
				print (message.text)

				holder = str(message.text).split()[1].lower()
				crypto = str(message.text).split()[2].lower()
				concat = holder+crypto
				print(df.concat)
				cost = df[df['concat'] == concat]['avg_purchasing_price'].values[0]
				print(cost)
				buy_qty = df[df['concat'] == concat]['buy_quantity'].values[0]
				remain_qty = df[df['concat'] == concat]['remaining_qty'].values[0]
				total_cost = df[df['concat'] == concat]['total_cost'].values[0]
				market = df[df['concat'] == concat]['exchange'].values[0]
				sale_price = df[df['concat'] == concat]['avg_selling_price'].values[0]
				print(sale_price)

					
				qty_sale = df[df['concat'] == concat]['sell_quantity'].values[0]
				total_sale = df[df['concat'] == concat]['total_sales'].values[0]
				pnl = df[df['concat'] == concat]['pnl'].values[0]
				total_value = df[df['concat'] == concat]['holding_value'].values[0]
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
				\nTotal Sales: $ {} \n \
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
			elif len(str(message.text).split()) == 3 and str(message.text).split()[0].lower() == '!n' and str(message.text).split()[2].lower() == 'all':
				wks = gc.open_by_key('1W2_tdPIPJ8rNPMi4eYcN8NOzq2pEqM_83n85CT3Nqxo').worksheet("summary")
				data = wks.get_all_values()
				print('done-')
				headers = data.pop(0)
				df = pd.DataFrame(data, columns=headers)
				rename_column(data = df,col_list = df.columns)
				df['concat'] = (df['name']+df['ccy']).astype('str').str.lower()

				print('done')
				
				holder = str(message.text).split()[1].upper()
				print('running-all')

				list_price = []
				list_net_position = []
				total_net_position = 0
				list_coin = df[(df['name'] == holder) & (df['remaining_qty'].astype('float64') >=0 )]['ccy'].tolist()
				pnl = df[(df['name'] == holder) & (df['remaining_qty'].astype('float64') >=0 )]['pnl'].tolist()
				remain_qty = df[(df['name'] == holder) & (df['remaining_qty'].astype('float64') >=0 )]['remaining_qty'].astype('float64').tolist()
				content = ''''''
				for i in range(0,len(list_coin)):
					crypto = list_coin[i]
					print(crypto)
					if remain_qty[i] == 0:
						print('true')
						net_position = float(pnl[i])
						list_net_position.append(net_position)
						total_net_position += net_position
						print('running-step2')
						content += '''{} --- {}   --- $ {} \n'''.format(crypto,num_finance(remain_qty[i],crypto=crypto),num_finance(net_position,crypto = crypto))
						print('end if')
					elif remain_qty[i] > 0:
						print('start else')
						market_price = call_coin_price(str(crypto).lower())
						print('----',i,'----')
						print(market_price)
						print(remain_qty[i])
						market_value = float(remain_qty[i])*market_price
						net_position = market_value+float(pnl[i])

						print(market_value,'---',net_position)
						list_net_position.append(net_position)
						total_net_position += net_position
						print('running-step2')
						content += '''{} --- {}   --- $ {} \n'''.format(crypto,num_finance(remain_qty[i],crypto=crypto),num_finance(net_position,crypto = crypto))
				text = '''<b>Shark {} - Portfolio</b>\n\
						\n<b>CCY     Remain Qty     Net_Position</b>\n'''.format(holder)

				content2 =  '''<b>Total Net Position: $ {}</b> '''.format(num_finance(total_net_position,crypto = 'none'))

				text_ = text+content+content2

				bot.send_message(chat_id=message.chat.id,text = text_,parse_mode='HTML')

			elif len(str(message.text).split()) == 3 and str(message.text).split()[0].lower() == '!r' and str(message.text).split()[2].lower() == 'all':
				wks = gc.open_by_key('1W2_tdPIPJ8rNPMi4eYcN8NOzq2pEqM_83n85CT3Nqxo').worksheet("summary")
				data = wks.get_all_values()
				print('done-')
				headers = data.pop(0)
				df = pd.DataFrame(data, columns=headers)
				rename_column(data = df,col_list = df.columns)
				df['concat'] = (df['name']+df['ccy']).astype('str').str.lower()

				print('done')
				holder = str(message.text).split()[1].upper()
				print('running-all')

				list_price = []
				list_market_value = []
				total_net_position = 0
				list_coin = df[(df['name'] == holder) & (df['remaining_qty'].astype('float64') >0 )]['ccy'].tolist()
				pnl = df[(df['name'] == holder) & (df['remaining_qty'].astype('float64') >0 )]['pnl'].tolist()
				remain_qty = df[(df['name'] == holder) & (df['remaining_qty'].astype('float64') >0 )]['remaining_qty'].astype('float64').tolist()
				content = ''''''
				total_market_value = 0
				for i in range(0,len(list_coin)):
				    crypto = list_coin[i]
				    market_price = call_coin_price(str(crypto).lower())
				#     list_price.append(price)
				    print('----',i,'----')
				    print(market_price)
				    print(remain_qty[i])
				    market_value = float(remain_qty[i])*market_price
				    net_position = market_value+float(pnl[i])
				    
				    print(market_value,'---',net_position)
				    list_market_value.append(market_value)
				    total_net_position += net_position
				    total_market_value += market_value
				    print('running-step2')
				    content += '''{} --- {}   --- $ {} \n'''.format(crypto,num_finance(remain_qty[i],crypto=crypto),num_finance(market_value,crypto = crypto))
				text = '''<b>Shark {} - Portfolio</b>\n\
						\n<b>CCY     Remain Qty     Market Value</b>\n'''.format(holder)

				content2 =  '''<b>Total Values: $ {}</b> '''.format(num_finance(total_market_value,crypto = 'none'))

				text_ = text+content+content2

				bot.send_message(chat_id=message.chat.id,text = text_,parse_mode='HTML')

			elif len(str(message.text).split()) == 2 and str(message.text).split()[0].lower() == '!k' and str(message.text).split()[1].lower() == 'all':
				wks = gc.open_by_key('1W2_tdPIPJ8rNPMi4eYcN8NOzq2pEqM_83n85CT3Nqxo').worksheet("db_keo")
				data = wks.get_all_values()
				print('done-db_keo')
				headers = data.pop(0)
				df = pd.DataFrame(data, columns=headers)
				rename_column(data = df,col_list = df.columns)
				keo_all = df[(df['status'] == 'Valid')][['ccy','entry','tp1','tp2','tp3']].to_string()
				# print(df[(df['status'] == 'valid')].to_string())
				print(keo_all)

				bot.send_message(chat_id=message.chat.id,text = keo_all)

			elif len(str(message.text).split()) == 2 and str(message.text).split()[0].lower() == '!k':
				wks = gc.open_by_key('1W2_tdPIPJ8rNPMi4eYcN8NOzq2pEqM_83n85CT3Nqxo').worksheet("db_keo")
				data = wks.get_all_values()
				ccy = str(message.text).split()[1].lower()
				print('done-db_keo')
				headers = data.pop(0)
				df = pd.DataFrame(data, columns=headers)
				rename_column(data = df,col_list = df.columns)
				keo_all = df[((df['status'] == 'Valid') & (df['ccy'].astype('str').str.lower() == ccy ))][['ccy','entry','tp1','tp2','tp3']].to_string()
				# print(df[(df['status'] == 'valid')].to_string())
				print(keo_all)

				bot.send_message(chat_id=message.chat.id,text = keo_all)





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
			elif re.search(str(message.text).lower(),r"chó n|chó nghĩa|cho n|cho h|cho k"):
				bot.send_message(chat_id=message.chat.id,text= str("---****---\n")+list_blame_all[num_blame_all]+str("\nhttps://i.imgur.com/wMfAZtx.png"))
			elif re.search(str(message.text).lower(),r"chó h|chó hiếu|cho h|cho k|chó k"):
				bot.send_message(chat_id=message.chat.id,text='---****---\nmay ngu vc')
			elif re.search(str(message.text).lower(),r"chó"):
				bot.send_message(chat_id=message.chat.id,text='---****---\ntụi mày ngu vcc')
			elif re.search(str(message.text).lower(),r"giỏi|gioi|good job|good"):
				bot.send_message(chat_id=message.chat.id,text='---****---\ncảm ơn sếp ạ')
			elif re.search(str(message.text).lower(),r"=))"):
				bot.send_message(chat_id=message.chat.id,text='---****---\ncười cc nè')
			elif re.search(str(message.text).lower(),r"bot láo|lao|láo vãi|bot lao"):
				bot.send_message(chat_id=message.chat.id,text='---****---\ncó mày láo ấy')
			elif re.search(str(message.text).lower(),r"juld|filda|mdx"):
				bot.send_message(chat_id=message.chat.id,text=str('---****---\n')+list_blame[num_rand_type1])
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
