import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# headers = gspread.httpsession.HTTPSession(headers={'Connection':'Keep-Alive'})

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope) # Your json file here

gc = gspread.authorize(credentials)

wks = gc.open_by_key('1W2_tdPIPJ8rNPMi4eYcN8NOzq2pEqM_83n85CT3Nqxo').worksheet("summary")
# wks = gc.open("HIKING Tracking Logs").sheet1

data = wks.get_all_values()
# data = wks.get_all_records()
# print(data)
headers = data.pop(0)

df = pd.DataFrame(data, columns=headers)
df['concat'] = (df['Name']+df['CCY']).str.lower()
# df.to_csv(r"test.csv")

print(df['concat'])