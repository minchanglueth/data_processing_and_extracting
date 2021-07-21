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
sheet = open_urls.worksheet("top_ab_sg_cs")

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


class done_extract:
    def __init__(self, report_type, report_day):
        self.report_type = report_type
        self.report_day = report_day

    def value_done(self):
        find_value_done = df.loc[
            (df["week"] == str(self.report_day)) & (df["report"] == self.report_type)
        ]["extract_status"].to_numpy()
        return str(find_value_done).strip("[]").strip("'")


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


class update_report:
    def __init__(
        self,
        crl_sts,
        id_list,
        gsh_url,
        gsh_name,
        etr_sts,
        report_type,
        report_day,
        df,
        process_date,
    ):
        self.crl_sts = crl_sts
        self.id_list = id_list
        self.gsh_name = gsh_name
        self.gsh_url = gsh_url
        self.etr_sts = etr_sts
        self.report_type = report_type
        self.report_day = report_day
        self.df = df
        self.process_date = process_date

    def update_gsh_noidlist(self):
        row_update = row_index(
            self.report_type, self.report_day, self.df
        ).row_index_value()
        column_update_crawler_status = column_index(
            "crawler_status", self.df
        ).colum_index_value()
        column_update_gsheet_url = column_index(
            "gsheet_url", self.df
        ).colum_index_value()
        column_update_gsheet_name = column_index(
            "gsheet_name", self.df
        ).colum_index_value()
        column_update_extract_status = column_index(
            "extract_status", self.df
        ).colum_index_value()
        column_update_processing_date = column_index(
            "processing_date", self.df
        ).colum_index_value()
        update_data(
            row_update, column_update_crawler_status, self.crl_sts
        ).update_data_gspread()
        update_data(
            row_update, column_update_gsheet_url, self.gsh_url
        ).update_data_gspread()
        update_data(
            row_update, column_update_gsheet_name, self.gsh_name
        ).update_data_gspread()
        update_data(
            row_update, column_update_extract_status, self.etr_sts
        ).update_data_gspread()
        update_data(
            row_update, column_update_processing_date, self.process_date
        ).update_data_gspread()

    def update_gsh(self):
        row_update = row_index(
            self.report_type, self.report_day, self.df
        ).row_index_value()
        column_update_crawler_status = column_index(
            "crawler_status", self.df
        ).colum_index_value()
        column_update_reset_id_list = column_index(
            "reset_id_list", self.df
        ).colum_index_value()
        column_update_gsheet_url = column_index(
            "gsheet_url", self.df
        ).colum_index_value()
        column_update_gsheet_name = column_index(
            "gsheet_name", self.df
        ).colum_index_value()
        column_update_extract_status = column_index(
            "extract_status", self.df
        ).colum_index_value()
        column_update_processing_date = column_index(
            "processing_date", self.df
        ).colum_index_value()
        update_data(
            row_update, column_update_crawler_status, self.crl_sts
        ).update_data_gspread()
        update_data(
            row_update, column_update_reset_id_list, self.id_list
        ).update_data_gspread()
        update_data(
            row_update, column_update_gsheet_url, self.gsh_url
        ).update_data_gspread()
        update_data(
            row_update, column_update_gsheet_name, self.gsh_name
        ).update_data_gspread()
        update_data(
            row_update, column_update_extract_status, self.etr_sts
        ).update_data_gspread()
        update_data(
            row_update, column_update_processing_date, self.process_date
        ).update_data_gspread()


class update_gsheet_name:
    def __init__(self, gsh_name, df, report_type, report_day):
        self.gsh_name = gsh_name
        self.df = df
        self.report_type = report_type
        self.report_day = report_day

    def update_gsh(self):
        row_update = row_index(
            self.report_type, self.report_day, self.df
        ).row_index_value()
        column_update_gsheet_name = column_index(
            "gsheet_name", self.df
        ).colum_index_value()
        update_data(
            row_update, column_update_gsheet_name, self.gsh_name
        ).update_data_gspread()
