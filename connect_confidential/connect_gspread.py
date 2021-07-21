import gspread
from oauth2client.service_account import ServiceAccountCredentials

scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name('C:/Users/9480/Documents/VIBBIDI/2021/Python_project/VIBBIDI_marketing_f8f3d0002af3.json', scope)

client_gspread = gspread.authorize(creds)
# urls = client.open_by_url('https://docs.google.com/spreadsheets/d/1gVtIMGFllY8ebemFQRCehFVyhn2-5qKUwHnQStQBp4o/edit#gid=0')

# worksheet = urls.add_worksheet(title="MP3", rows="1000", cols="26")

# sheet_metadata = client_gspread.open_by_url('https://docs.google.com/spreadsheets/d/1qxoNvkSvDfmIhoJgs9XFucN01kCZsd-1TkVJSzRj-CA/edit#gid=0').title
# print(type(sheet_metadata))

# import pandas as pd

# open_urls = client_gspread.open_by_url('https://docs.google.com/spreadsheets/d/1TGNFci-sn9vyCKD4zU2My-R20EzFDtbLMZivohfbim0/edit#gid=5410219')
# open_sheet = open_urls.worksheet('Justin_update')
# data = open_sheet.get_all_values()
# df1 = pd.DataFrame(data)
# new_header = df1.iloc[0]
# df1.columns = new_header
# #df_sg1 = df1[['Category','TrackId','Sub Genre 1']].drop_duplicates(subset=None,keep='first',ignore_index=True)
# #df_sg1.columns = ['Category','TrackId','Sub_Genre']
# df_sg2 = df1[['Category','TrackId','Sub Genre 2']].drop_duplicates(subset=None,keep='first',ignore_index=True)
# df_sg1 = df1[['Category','TrackId','Sub Genre 1']].drop_duplicates(subset=None,keep='first',ignore_index=True)
# df_sg2.columns = ['Category','TrackId','Sub_Genre']
# df_sg1.columns = ['Category','TrackId','Sub_Genre']
# df = df_sg2.append(df_sg1,ignore_index=True).drop_duplicates(subset=None,keep='first',ignore_index=True)
# df = df[1:]
# df = df[(df['TrackId'] != 'TrackId')]
# df = df[df['Sub_Genre'] != '']
# print(df)
# df.to_html('index_ta.html')

