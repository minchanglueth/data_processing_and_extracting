from gspread_dataframe import set_with_dataframe

from connect_confidential.connect_db_final import conn, cursor
from connect_confidential.connect_gspread import client_gspread

from datetime import date, timedelta
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
report_date = date.today() - timedelta(day_list[myweekday])


class create_update_spreadsheet:
    def __init__(
        self,
        value_report_name,
        work_sheet_name,
        sheet_url,
        report_day,
        report_mp3,
        report_mp4,
    ):
        self.value_report_name = value_report_name
        self.work_sheet_name = work_sheet_name
        self.sheet_url = sheet_url
        self.report_day = report_day
        self.report_mp3 = report_mp3
        self.report_mp4 = report_mp4

    def create_gsread_s11(self):
        spreadsheet_name = input("Please input the spreadsheet name: ")
        new_sheet_report = client_gspread.create(spreadsheet_name)
        new_sheet_report.share(None, perm_type="anyone", role="writer", with_link=True)
        new_sheet_report.share(
            "albumpagejane@gmail.com", perm_type="user", role="writer"
        )
        new_sheet_report.share(
            "singlepagejanezz@gmail.com", perm_type="user", role="writer"
        )
        new_sheet_report_url = (
            "https://docs.google.com/spreadsheets/d/%s" % new_sheet_report.id
        )

        open_urls = client_gspread.open_by_url(new_sheet_report_url)

        open_urls.worksheet("Sheet1").update_title(self.work_sheet_name)
        open_urls.add_worksheet("MP_3", rows="1000", cols="26")
        open_urls.add_worksheet("MP_4", rows="1000", cols="26")

        work_sheet = open_urls.worksheet(self.work_sheet_name)
        value_report = self.value_report_name.format(self.report_day)

        cursor.execute(value_report)
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        set_with_dataframe(work_sheet, df)

        return new_sheet_report_url

    def update_gsread(self):
        open_urls = client_gspread.open_by_url(self.sheet_url)
        work_sheet = open_urls.worksheet(self.work_sheet_name)
        value_report = self.value_report_name.format(self.report_day)

        cursor.execute(value_report)
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        set_with_dataframe(work_sheet, df)

        return self.sheet_url

    def create_gsread_top(self):
        spreadsheet_name = input("Please input the spreadsheet name: ")
        new_sheet_report = client_gspread.create(spreadsheet_name)
        new_sheet_report.share(None, perm_type="anyone", role="writer", with_link=True)
        new_sheet_report.share(
            "albumpagejane@gmail.com", perm_type="user", role="writer"
        )
        new_sheet_report.share(
            "singlepagejanezz@gmail.com", perm_type="user", role="writer"
        )
        new_sheet_report_url = (
            "https://docs.google.com/spreadsheets/d/%s" % new_sheet_report.id
        )

        open_urls = client_gspread.open_by_url(new_sheet_report_url)
        open_urls.worksheet("Sheet1").update_title("MP_3")
        open_urls.add_worksheet("MP_4", rows="1000", cols="26")
        work_sheet_mp3 = open_urls.worksheet("MP_3")
        work_sheet_mp4 = open_urls.worksheet("MP_4")

        value_report_mp3 = self.report_mp3.format(self.report_day)
        value_report_mp4 = self.report_mp4.format(self.report_day)
        value_report = [value_report_mp3, value_report_mp4]
        report = []
        for i in value_report:
            cursor.execute(i)
            result = cursor.fetchall()
            df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
            report.append(df)
        set_with_dataframe(work_sheet_mp3, report[0])
        set_with_dataframe(work_sheet_mp4, report[1])

        return new_sheet_report_url
