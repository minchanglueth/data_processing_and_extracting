from connect_confidential.connect_gspread import client_gspread

from datetime import date
import calendar
import pandas as pd
from gspread_dataframe import set_with_dataframe

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
# df = pd.DataFrame(data)
# print(df)


class column_index:
    def __init__(self, value, df):
        self.value = value
        self.df = df

    def colum_index_value(self):
        return self.df.columns.get_loc(self.value) + 1


class row_index:
    def __init__(self, report_type, report_day, df):
        self.report_type = report_type
        self.report_day = report_day
        self.df = df

    def row_index_value(self):
        row_index = self.df.index[
            (self.df["week"] == str(self.report_day))
            & (self.df["report"] == self.report_type)
        ].to_numpy()
        return int(str(row_index).strip("[]")) + 1


class url_extract:
    def __init__(self, report_type, report_day, df):
        self.report_type = report_type
        self.report_day = report_day
        self.df = df

    def value_done(self):
        find_value_url = self.df.loc[
            (self.df["week"] == str(self.report_day))
            & (self.df["report"] == self.report_type)
        ]["gsheet_url"].to_numpy()
        return str(find_value_url).strip("[]").strip("'")


class update_data:
    def __init__(self, row, column, value):
        self.row = row
        self.column = column
        self.value = value

    def update_data_gspread(self):
        sheet.update_cell(self.row, self.column, self.value)


class if_exist_report:
    def __init__(self, report_day, report_type):
        self.report_type = report_type
        self.report_day = report_day

    def create_data(self):
        row_index = df.index[
            (df["week"] == str(self.report_day)) & (df["report"] == self.report_type)
        ].to_numpy()
        row_index_value = str(row_index).strip("[]")
        if row_index_value == "":
            if myweekday == "Monday" or self.report_type in ["top_album", "top_single"]:
                update_data(
                    len(df["week"]) + 1, df.columns.get_loc("week") + 1, self.report_day
                ).update_data_gspread()
                update_data(
                    len(df["week"]) + 2, df.columns.get_loc("week") + 1, self.report_day
                ).update_data_gspread()
                update_data(
                    len(df["week"]) + 3, df.columns.get_loc("week") + 1, self.report_day
                ).update_data_gspread()
                update_data(
                    len(df["week"]) + 4, df.columns.get_loc("week") + 1, self.report_day
                ).update_data_gspread()
                update_data(
                    len(df["week"]) + 1, df.columns.get_loc("report") + 1, "top_album"
                ).update_data_gspread()
                update_data(
                    len(df["week"]) + 2, df.columns.get_loc("report") + 1, "top_single"
                ).update_data_gspread()
                update_data(
                    len(df["week"]) + 3,
                    df.columns.get_loc("report") + 1,
                    "new_classic_s11",
                ).update_data_gspread()
                update_data(
                    len(df["week"]) + 4, df.columns.get_loc("report") + 1, "new_classic"
                ).update_data_gspread()
            else:
                update_data(
                    len(df["week"]) + 1, df.columns.get_loc("week") + 1, self.report_day
                ).update_data_gspread()
                update_data(
                    len(df["week"]) + 2, df.columns.get_loc("week") + 1, self.report_day
                ).update_data_gspread()
                update_data(
                    len(df["week"]) + 1,
                    df.columns.get_loc("report") + 1,
                    "new_classic_s11",
                ).update_data_gspread()
                update_data(
                    len(df["week"]) + 2, df.columns.get_loc("report") + 1, "new_classic"
                ).update_data_gspread()


class report_to_db:
    def __init__(
        self,
        update_date,
        update_gsheet_url,
        compare_file,
        update_status,
        report_type,
        report_day,
        nc_dupbanner,
        nc_dupbanner_column,
        df,
    ):
        self.update_date = update_date
        self.update_gsheet_url = update_gsheet_url
        self.compare_file = compare_file
        self.update_status = update_status
        self.report_type = report_type
        self.report_day = report_day
        self.nc_dupbanner = nc_dupbanner
        self.nc_dupbanner_column = nc_dupbanner_column
        self.df = df

    def update_gsh_db(self):
        row_update = row_index(
            self.report_type, self.report_day, self.df
        ).row_index_value()
        column_update_update_date = column_index(
            "update_date", self.df
        ).colum_index_value()
        column_update_update_gsheet_url = column_index(
            "update_gsheet_url", self.df
        ).colum_index_value()
        column_update_compare_file = column_index(
            "compare_file", self.df
        ).colum_index_value()
        column_update_update_status = column_index(
            "update_status", self.df
        ).colum_index_value()
        column_update_nc_dupbanner = column_index(
            self.nc_dupbanner_column, self.df
        ).colum_index_value()
        update_data(
            row_update, column_update_update_date, self.update_date
        ).update_data_gspread()
        update_data(
            row_update, column_update_update_gsheet_url, self.update_gsheet_url
        ).update_data_gspread()
        update_data(
            row_update, column_update_compare_file, self.compare_file
        ).update_data_gspread()
        update_data(
            row_update, column_update_update_status, self.update_status
        ).update_data_gspread()
        update_data(
            row_update, column_update_nc_dupbanner, self.nc_dupbanner
        ).update_data_gspread()


class report_invalid_ids:
    def __init__(self, work_sheet_name, report_url, df):
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
        # open_urls_2 = client_gspread.open_by_url(self.report_url)
        # open_urls_2.add_worksheet(self.work_sheet_name, rows="1000", cols="26")
        open_urls.add_worksheet(self.work_sheet_name, rows="1000", cols="26")
        work_sheet = open_urls.worksheet(self.work_sheet_name)
        set_with_dataframe(work_sheet, self.df)
        return self.report_url

    #'invalid_albumsuuid',report_url,df_none
