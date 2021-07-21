from connect_confidential.connect_gspread import client_gspread

from datetime import date
import calendar
import pandas as pd

day_list = {
    "Sunday": 0,
    "Monday": 1,
    "Tuesday": 2,
    "Wednesday": 3,
    "Thursday": 4,
    "Friday": 5,
    "Saturday": 6,
}
myweekday = calendar.day_name[date.today().weekday()]

open_urls = client_gspread.open_by_url(
    "https://docs.google.com/spreadsheets/d/1MHDksbs-RKXhZZ-LRgRhVy_ldAxK8lSzyoJK4sA_Uyo/edit#gid=141704097"
)

sheet = open_urls.worksheet("delacct_chgpwd_updpwd")

data = sheet.get_all_values()
df = pd.DataFrame(data, columns=[i for i in data[0]])
# df = pd.DataFrame(data)
# print(df)


def update_data_gsheet(date, option, userid, old_email, old_username, new_username):
    sheet.update_cell(len(df["Date"]) + 1, df.columns.get_loc("Date") + 1, date)
    sheet.update_cell(len(df["Date"]) + 1, df.columns.get_loc("Option") + 1, option)
    sheet.update_cell(len(df["Date"]) + 1, df.columns.get_loc("Userid") + 1, userid)
    sheet.update_cell(
        len(df["Date"]) + 1, df.columns.get_loc("Old_email") + 1, old_email
    )
    sheet.update_cell(
        len(df["Date"]) + 1, df.columns.get_loc("Old_username") + 1, old_username
    )
    sheet.update_cell(
        len(df["Date"]) + 1, df.columns.get_loc("New_username") + 1, new_username
    )
