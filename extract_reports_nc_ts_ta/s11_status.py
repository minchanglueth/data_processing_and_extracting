import pandas as pd

from datetime import date, timedelta, datetime
import calendar

from colorama import Fore, Style

from connect_confidential.connect_gspread import client_gspread

# from connect_confidential.connect_db_stg import conn, cursor
from connect_confidential.connect_db_final import conn, cursor

from raw_sql import report_s11, crawler_report_s11, crawlingtask
from slack_report import send_message_slack
from gspread_report import create_update_spreadsheet
from update_data_report import (
    done_extract,
    update_report,
    if_exist_report,
    sheet,
    url_extract,
    update_gsheet_name,
)

day_list = {
    "Saturday": 0,
    "Sunday": 1,
    "Monday": 2,
    "Tuesday": 3,
    "Wednesday": 4,
    "Thursday": 5,
    "Friday": 6,
}
myweekday = calendar.day_name[date.today().weekday()]
report_date = date.today() - timedelta(day_list[myweekday])
daily_nc_date = date.today() - timedelta(1)
daily_nc_date = datetime.strptime(
    str(date.today() - timedelta(1)) + " 16:00:00", "%Y-%m-%d %H:%M:%S"
)
topalbum_actionid = "90ECECF350D94F8C8A16B209CADF9B9E"
topsingle_actionid = "988CE4D571B4455EA4B9B1904BA92916"
newclassic_actionid = "E4B85D0A993146EEB84426C2246EFCA0"


class check_s11_crawler:
    def __init__(self, report_day):
        self.report_day = report_day

    def status(self):
        report_crawler = crawler_report_s11.format(self.report_day)
        cursor.execute(report_crawler)
        result = cursor.fetchall()
        sql1 = pd.DataFrame(result, columns=[i[0] for i in cursor.description]).astype(
            str
        )
        value_status_06 = sql1["Status_06"].drop_duplicates()
        value_status_E5 = sql1["Status_E5"].drop_duplicates()
        value_status = set(value_status_06.append(value_status_E5))
        distinct_value = "".join(str(x) for x in value_status)
        return distinct_value


class newclassic_report:
    def __init__(self, sql_report, report_day, report_type, nc_date):
        self.sql_report = sql_report
        self.report_day = report_day
        self.report_type = report_type
        self.nc_date = nc_date

    def nc_report_check(self):
        print(
            "\n"
            + Fore.LIGHTRED_EX
            + "I/ Now checking s11 crawlingtasks...\n"
            + "Please do NOT exit!"
            + Style.RESET_ALL
        )
        report_crawler = self.sql_report.format(self.report_day)
        cursor.execute(report_crawler)
        result = cursor.fetchall()
        sql1 = pd.DataFrame(result, columns=[i[0] for i in cursor.description]).astype(
            str
        )
        print(sql1)
        value_status_06 = sql1["Status_06"].drop_duplicates().tolist()
        value_status_E5 = sql1["Status_E5"].drop_duplicates().tolist()
        value_status = (
            "Crawler_06: "
            + str(value_status_06)
            + ", Crawler_E5: "
            + str(value_status_E5)
        )
        count_crawlingtasksid = sql1["ID_06"].count()
        sql2_06 = sql1.loc[sql1["Status_06"] != "complete"]
        sql2_E5 = sql1.loc[sql1["Status_E5"] != "complete"]
        id_list_06 = sql2_06["ID_06"].tolist()
        id_list_E5 = sql2_E5["06_to_E5"].tolist()
        id_list = (
            "\nCrawler_06: " + str(id_list_06) + "\nCrawler_E5: " + str(id_list_E5)
        )

        py_message = send_message_slack(
            "S11_crawler",
            count_crawlingtasksid,
            value_status,
            id_list,
            "to be processed in the following steps",
        )
        print("\n1/ ", end="")
        py_message.msg_slack()

        while True:
            try:
                if (
                    str(value_status_06).strip("[]").strip("'") == "complete"
                    and str(value_status_E5).strip("[]").strip("'") == "complete"
                ):
                    print(
                        "\n2/ CONCLUSION:",
                        Fore.LIGHTMAGENTA_EX
                        + "all criteria are SATISFIED"
                        + Style.RESET_ALL,
                    )
                    print(
                        "\n" + Fore.LIGHTRED_EX + "Finish crawling S11\n",
                        "\nII/ Now checking newrelease from Itunes...\n"
                        + "Please do NOT exit!"
                        + Style.RESET_ALL,
                    )
                    break
                else:
                    print(
                        "\n2/ CONCLUSION:",
                        Fore.LIGHTRED_EX
                        + "criteria are NOT satisfied"
                        + Style.RESET_ALL,
                    )
                    print("\n3/ Reset fault crawlingtasks:", end="")
                    reset_error_crawlingtaskid = input(
                        "enter "
                        + Fore.LIGHTBLUE_EX
                        + "YES "
                        + Style.RESET_ALL
                        + "if you want to reset, "
                        + Fore.LIGHTGREEN_EX
                        + "NO "
                        + Style.RESET_ALL
                        + "to exit: "
                    )
                    if reset_error_crawlingtaskid == "NO":
                        print(
                            "\n"
                            + Fore.LIGHTRED_EX
                            + "II/ Now checking newrelease from Itunes...\n"
                            + "Please do NOT exit!"
                            + Style.RESET_ALL
                        )
                        break
                    elif reset_error_crawlingtaskid == "YES":

                        slack_msg = send_message_slack(
                            "S11_crawler",
                            count_crawlingtasksid,
                            value_status,
                            id_list,
                            "not complete, to recheck",
                        )
                        # slack_msg.send_to_slack()
                        slack_msg.send_to_slack_error()  # thêm đoạn này
                        if_exist_report(self.nc_date, self.report_type).create_data()
                        data1 = sheet.get_all_values()
                        df = pd.DataFrame(data1, columns=[i for i in data1[0]])
                        update_report(
                            value_status,
                            id_list,
                            "",
                            "",
                            "",
                            self.report_type,
                            self.nc_date,
                            df,
                            str(date.today()),
                        ).update_gsh()  # chỉnh lại đoạn này, nếu cho reset lại thì vẫn giữ nguyên URL
                        print("Reset_crawler_06:")
                        # for id in id_list_06:
                        #     # # sql_crawl_itunes = """UPDATE crawlingtasks set `Status` = 'complete' where id = '{}';""" # cho testing
                        #     # sql_crawl_itunes = """UPDATE crawlingtasks set `Status` = NULL where id = '{}';"""
                        #     # sql_complete = sql_crawl_itunes.format(id)
                        #     # print(sql_complete)
                        #     # cursor.execute(sql_complete) # dùng cái này cho PROD.V4
                        #     # conn.commit() # dùng cái này cho PROD.V4
                        #     # # cursor.execute(sql_complete) # dùng cái này cho STG.V4
                        #     # # conn.commit() # dùng cái này cho STG.V4
                        #     crawlingtask(id,cursor,conn).itunes_update()
                        crawlingtask(
                            id_list_06, "none", "none", "none"
                        ).check_query_itunes()
                        crawlingtask(
                            id_list_06, cursor, conn, "15 min"
                        ).execute_query_itunes()
                        print("Reset_crawler_E5:")
                        # for id in id_list_E5:
                        #     # # sql_crawl_itunes = """UPDATE crawlingtasks set `Status` = 'complete' where id = '{}';""" #cái này để test
                        #     # sql_crawl_itunes = """UPDATE crawlingtasks set `Status` = NULL where id = '{}';"""
                        #     # sql_complete = sql_crawl_itunes.format(id)
                        #     # print(sql_complete)
                        #     # cursor.execute(sql_complete) # dùng cái này cho PROD.V4
                        #     # conn.commit() # dùng cái này cho PROD.V4
                        #     # # cursor.execute(sql_complete) # dùng cái này cho STG.V4
                        #     # # conn.commit() # dùng cái này cho STG.V4
                        #     crawlingtask(id,cursor,conn).itunes_update()
                        # print('\n' + Fore.LIGHTYELLOW_EX + 'Finish updating fault crawlingtasks, please wait 15min to recheck!')
                        # print('If the result returns NOT COMPLETE please inform Jay and Cc Joy & Minchan'+ Style.RESET_ALL)
                        crawlingtask(
                            id_list_E5, "none", "none", "none"
                        ).check_query_itunes()
                        crawlingtask(
                            id_list_E5, cursor, conn, "15 min"
                        ).execute_query_itunes()
                        print(
                            "\n"
                            + Fore.LIGHTRED_EX
                            + "II/ Now checking newrelease from Itunes...\n"
                            + "Please do NOT exit!"
                            + Style.RESET_ALL
                        )
                        break

                    else:
                        print(
                            Fore.LIGHTRED_EX
                            + "Please recheck CONCLUSION and RE-ENTER option!"
                            + Style.RESET_ALL
                        )
            except IndexError:
                slack_msg = send_message_slack(
                    self.report_type,
                    count_crawlingtasksid,
                    value_status,
                    id_list,
                    "not available, to recheck",
                )
                slack_msg.send_to_slack_error()
                print(
                    Fore.LIGHTRED_EX
                    + "Crawler report failed to run, please recheck and re-insert crawler!"
                    + Style.RESET_ALL
                )
                print(
                    "\n"
                    + Fore.LIGHTRED_EX
                    + "II/ Now checking newrelease from Itunes...\n"
                    + "Please do NOT exit!"
                    + Style.RESET_ALL
                )
                break

    def nc_s11_check(self):
        print(
            "\n"
            + Fore.LIGHTRED_EX
            + "I/ Now checking s11 from Spotify...\n"
            + "Please do NOT exit!"
            + Style.RESET_ALL
        )
        report_crawler = self.sql_report.format(self.report_day)
        cursor.execute(report_crawler)
        result = cursor.fetchall()
        sql1 = pd.DataFrame(result, columns=[i[0] for i in cursor.description]).astype(
            str
        )
        count_album = sql1["AlbumURL"].drop_duplicates().count()
        if count_album < 60:
            slack_msg = send_message_slack(
                "new_classic_s11_Spotify",
                count_album,
                "incomplete",
                "none",
                "incomplete",
            )
            # slack_msg.send_to_slack()
            slack_msg.send_to_slack_error()  # thêm đoạn này
            slack_msg.msg_slack()
            print(
                Fore.LIGHTRED_EX
                + "\nII/ Now checking newrelease from Itunes...\n"
                + "Please do NOT exit!"
                + Style.RESET_ALL
            )
        else:
            if done_extract(self.report_type, self.nc_date).value_done() != "done":
                print("new_classic_s11 from Spotify is ready to extract")
                gs_update = create_update_spreadsheet(
                    report_s11, "S_11", "none", self.report_day, "none", "none"
                )
                url = gs_update.create_gsread_s11()
                slack_msg = send_message_slack(
                    "new_classic_s11_Spotify", count_album, "complete", "none", url
                )
                slack_msg.send_to_slack()
                slack_msg.msg_slack()
                gsheet_name = client_gspread.open_by_url(url).title
                if_exist_report(self.nc_date, "new_classic_s11").create_data()
                data1 = sheet.get_all_values()
                df = pd.DataFrame(data1, columns=[i for i in data1[0]])
                update_report(
                    "complete",
                    "none",
                    url,
                    gsheet_name,
                    "done",
                    self.report_type,
                    self.nc_date,
                    df,
                    str(date.today()),
                ).update_gsh_noidlist()
                print(
                    "\n" + Fore.LIGHTRED_EX + "Finish extracting S11 from Spotify\n",
                    "\nII/ Now checking newrelease from Itunes...\n"
                    + "Please do NOT exit!"
                    + Style.RESET_ALL,
                )
            else:
                data1 = sheet.get_all_values()
                df = pd.DataFrame(data1, columns=[i for i in data1[0]])
                url = url_extract(self.report_type, self.nc_date, df).value_done()
                print(
                    Fore.LIGHTRED_EX
                    + "\nnew_classic_s11 already extracted previously as below:\n"
                    + "URL: "
                    + Fore.LIGHTCYAN_EX
                    + url
                    + Style.RESET_ALL
                )
                print(
                    "\n" + Fore.LIGHTRED_EX + "Finish extracting S11 from Spotify\n",
                    "\nII/ Now checking newrelease from Itunes...\n"
                    + "Please do NOT exit!"
                    + Style.RESET_ALL,
                )
