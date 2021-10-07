from connect_confidential.connect_gspread import client_gspread

from gspread_dataframe import set_with_dataframe
from datetime import date
import calendar
import pandas as pd

day_list = {'Sunday':0,'Monday':1,'Tuesday':2,'Wednesday':3,'Thursday':4,'Friday':5,'Saturday':6}
myweekday = calendar.day_name[date.today().weekday()]

open_urls = client_gspread.open_by_url("https://docs.google.com/spreadsheets/d/1MHDksbs-RKXhZZ-LRgRhVy_ldAxK8lSzyoJK4sA_Uyo/edit#gid=141704097")

# sheet = open_urls.worksheet('user_contribute')

# data = sheet.get_all_values()
# df = pd.DataFrame(data, columns= [i for i in data[0]])
# df = pd.DataFrame(data)
# print(df)

def update_data_gsheet(sheet_name,date,action_type,description,count_id):
    sheet = open_urls.worksheet(sheet_name)
    data = sheet.get_all_values()
    df = pd.DataFrame(data, columns= [i for i in data[0]])
    sheet.update_cell(len(df['Date'])+1,df.columns.get_loc('Date')+1,date)
    sheet.update_cell(len(df['Date'])+1,df.columns.get_loc('action_type')+1,action_type)
    sheet.update_cell(len(df['Date'])+1,df.columns.get_loc('description')+1,description)
    sheet.update_cell(len(df['Date'])+1,df.columns.get_loc('count_id')+1,count_id)     


class report_invalid_ids:
    def __init__(self,work_sheet_name,report_url,df):
        self.work_sheet_name = work_sheet_name
        self.report_url = report_url
        self.df = df

    def create_and_update(self):
        open_urls = client_gspread.open_by_url(self.report_url)
        try:
            worksheet = open_urls.worksheet(self.work_sheet_name)
            open_urls.del_worksheet(worksheet)
        except:
            pass
        open_urls.add_worksheet(self.work_sheet_name, rows="1000", cols="26")
        work_sheet = open_urls.worksheet(self.work_sheet_name)
        set_with_dataframe(work_sheet,self.df)
        return self.report_url