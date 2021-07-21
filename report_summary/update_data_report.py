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

# sheet = open_urls.worksheet("Minchan")
sheet = open_urls.worksheet('top_ab_sg_cs')

data = sheet.get_all_values()
df = pd.DataFrame(data, columns=[i for i in data[0]])


class column_index:
    def __init__(self, value, df):
        self.value = value
        self.df = df

    def colum_index_value(self):
        return self.df.columns.get_loc(self.value) + 1


class row_index:
    def __init__(self, report_type, gsheet_url, df):
        self.report_type = report_type
        self.gsheet_url = gsheet_url
        self.df = df

    def row_index_value(self):
        row_index = self.df.index[
            (self.df["gsheet_url"] == str(self.gsheet_url))
            & (self.df["report"] == self.report_type)
        ].to_numpy()
        return int(str(row_index).strip("[]")) + 1


class update_data:
    def __init__(self, row, column, value):
        self.row = row
        self.column = column
        self.value = value

    def update_data_gspread(self):
        sheet.update_cell(self.row, self.column, self.value)


class report_to_db:
    def __init__(
        self,
        preprare_date,
        missing_youtube,
        missing_spotify,
        report_type,
        gsheet_url,
        df,
    ):
        self.preprare_date = preprare_date
        self.missing_youtube = missing_youtube
        self.missing_spotify = missing_spotify
        self.report_type = report_type
        self.gsheet_url = gsheet_url
        self.df = df

    def update_gsh_db(self):
        row_update = row_index(
            self.report_type, self.gsheet_url, self.df
        ).row_index_value()
        column_update_preprare_date = column_index(
            "preprare_date", self.df
        ).colum_index_value()
        column_update_missing_youtube = column_index(
            "missing_youtube", self.df
        ).colum_index_value()
        column_update_missing_spotify = column_index(
            "missing_spotify", self.df
        ).colum_index_value()
        update_data(
            row_update, column_update_preprare_date, self.preprare_date
        ).update_data_gspread()
        update_data(
            row_update, column_update_missing_youtube, self.missing_youtube
        ).update_data_gspread()
        update_data(
            row_update, column_update_missing_spotify, self.missing_spotify
        ).update_data_gspread()


def get_gsheet_id_from_url(url: str):
    try:
        url_list = url.split("/")
        gsheet_id = url_list[5]
        return "https://docs.google.com/spreadsheets/d/" + str(gsheet_id)
    except IndexError:
        return None
