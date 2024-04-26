from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
import requests
from bs4 import BeautifulSoup
import json
def call_fx():
    a = requests.get('https://www.coingecko.com/')
    soup = BeautifulSoup(a.text,'lxml')
    # soup.find_all('body',{'data-exchange-rate-json'})
    element = soup.find(attrs={'data-exchange-rate-json': True})
    data_exchange_rate_json = element['data-exchange-rate-json']
    return json.loads(data_exchange_rate_json)

async def hello(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f'Hello {update.effective_user.first_name}')

async def fx_info(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    data = list(call_fx().items())
    await update.message.reply_text(f'Below is fx_info \n{data[0:10]}')

app = ApplicationBuilder().token("6965701995:AAG8uo262IAsVLcCOPdSntrVSSgzXho0DyU").build()

app.add_handler(CommandHandler("hello", hello))
app.add_handler(CommandHandler("current_fx", fx_info))


app.run_polling()