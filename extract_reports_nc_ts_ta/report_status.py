import pandas as pd

from datetime import date, timedelta, datetime
import calendar

from colorama import Fore, Style

from connect_confidential.connect_gspread import client_gspread
from connect_confidential.connect_db_final import conn, cursor

# from connect_confidential.connect_db_stg import conn, cursor

from raw_sql import (
    report_top_album_mp3,
    report_top_album_mp4,
    report_s11,
    crawler_report_s11,
    crawler_report,
    report_top_single_mp3,
    report_top_single_mp4,
    report_new_classic_mp3,
    report_new_classic_mp4,
    crawlingtask,
)
from slack_report import send_message_slack
from gspread_report import create_update_spreadsheet
from update_data_report import (
    done_extract,
    update_report,
    url_extract,
    if_exist_report,
    sheet,
)
from s11_status import newclassic_report, check_s11_crawler

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
report_date = str(date.today() - timedelta(day_list[myweekday]))
daily_nc_date = str(date.today() - timedelta(1))  # nhớ chỉnh lại ngày
daily_nc_time = datetime.strptime(
    daily_nc_date + " 16:00:00", "%Y-%m-%d %H:%M:%S"
)  # nhớ chỉnh lại ngày

topalbum_actionid = "90ECECF350D94F8C8A16B209CADF9B9E"
topsingle_actionid = "988CE4D571B4455EA4B9B1904BA92916"
newclassic_actionid = "E4B85D0A993146EEB84426C2246EFCA0"

print("Report types: top_album, top_single, new_classic_s11, new_classic")

while True:
    report_type = input(
        Fore.LIGHTCYAN_EX + "Please input the report type: " + Style.RESET_ALL
    )
    print("Please wait, the file is processing...")
    if report_type == "top_album":
        report_crawler = crawler_report.format(topalbum_actionid, report_date)
        crl_id = 14
        report_mp3 = report_top_album_mp3
        report_mp4 = report_top_album_mp4
        break
    elif report_type == "top_single":
        report_crawler = crawler_report.format(topsingle_actionid, report_date)
        crl_id = 10
        report_mp3 = report_top_single_mp3
        report_mp4 = report_top_single_mp4
        break
    elif report_type == "new_classic_s11":  # xuất hàng ngày -> thêm logic
        crl_id = 10
        if myweekday == "Monday":
            newclassic_report(
                report_s11, report_date, report_type, report_date
            ).nc_s11_check()
            report_crawler = crawler_report.format(newclassic_actionid, report_date)
            nc_sql_time = report_date
        else:
            newclassic_report(
                report_s11, daily_nc_time, report_type, daily_nc_date
            ).nc_s11_check()
            report_crawler = crawler_report.format(newclassic_actionid, daily_nc_time)
            report_date = daily_nc_date
            nc_sql_time = daily_nc_time
        break
    elif report_type == "new_classic":
        crl_id = 10
        report_mp3 = report_new_classic_mp3
        report_mp4 = report_new_classic_mp4
        if myweekday == "Monday":
            newclassic_report(
                crawler_report_s11, report_date, report_type, report_date
            ).nc_report_check()
            report_crawler = crawler_report.format(newclassic_actionid, report_date)
            nc_sql_time = report_date
        else:
            newclassic_report(
                crawler_report_s11, daily_nc_time, report_type, daily_nc_date
            ).nc_report_check()
            report_crawler = crawler_report.format(newclassic_actionid, daily_nc_time)
            report_date = daily_nc_date
            nc_sql_time = daily_nc_time
        break
    else:
        print(Fore.LIGHTRED_EX + "Please re-enter the correct option" + Style.RESET_ALL)


cursor.execute(report_crawler)
result = cursor.fetchall()
sql1 = pd.DataFrame(result, columns=[i[0] for i in cursor.description]).astype(str)
value_status = sql1["Status"].drop_duplicates().tolist()
count_distinct_status = sql1["Status"].drop_duplicates().count()
count_crawlingtasksid = sql1["Status"].count()
sql2 = sql1.loc[sql1["Status"] != "complete"]
id_list = sql2["id"].tolist()


class combine_slack_and_report:
    def __init__(self, url):
        self.url = url

    def update_sr(self):
        slack_msg = send_message_slack(
            report_type, count_crawlingtasksid, value_status, id_list, self.url
        )
        slack_msg.send_to_slack()
        slack_msg.msg_slack()
        gsheet_name = client_gspread.open_by_url(self.url).title
        if_exist_report(report_date, report_type).create_data()
        data1 = sheet.get_all_values()
        df = pd.DataFrame(data1, columns=[i for i in data1[0]])
        update_report(
            "complete",
            "none",
            self.url,
            gsheet_name,
            "done",
            report_type,
            report_date,
            df,
            str(date.today()),
        ).update_gsh_noidlist()


def crawler_report_msg():
    print("\n1/ Crawler's status report on", report_type, ":")
    print(sql1)
    if report_type in ["new_classic_s11", "new_classic_s11_final"]:
        py_message = send_message_slack(
            "new_classic_s11_Itunes",
            count_crawlingtasksid,
            value_status,
            id_list,
            "to be processed in the following steps",
        )
    else:
        py_message = send_message_slack(
            report_type,
            count_crawlingtasksid,
            value_status,
            id_list,
            "to be processed in the following steps",
        )
    print("\n2/ ", end="")
    py_message.msg_slack()


def checking_extract_mp3_mp4():
    while True:
        try:
            if (
                str(value_status[0]) == "complete"
                and count_distinct_status == 1
                and count_crawlingtasksid == crl_id
            ):
                print(
                    "\n3/ CONCLUSION:",
                    Fore.LIGHTMAGENTA_EX
                    + "all criteria are SATISFIED"
                    + Style.RESET_ALL,
                )
                if report_type == "new_classic_s11" or (
                    report_type == "new_classic"
                    and check_s11_crawler(nc_sql_time).status() != "complete"
                ):
                    print("\nNow waiting for s11 to be completed crawling!")
                    print(
                        "\n"
                        + Fore.LIGHTYELLOW_EX
                        + "The file is done processing!"
                        + Style.RESET_ALL
                    )
                    break
                else:
                    print("\n4/ Prepare and extract file:", end="")
                    prepare_file = input(
                        "enter "
                        + Fore.LIGHTBLUE_EX
                        + "YES "
                        + Style.RESET_ALL
                        + "to prepare and extract file, "
                        + Fore.LIGHTGREEN_EX
                        + "NO "
                        + Style.RESET_ALL
                        + "to exit: "
                    )
                    if prepare_file == "NO":
                        print(
                            "\n"
                            + Fore.LIGHTYELLOW_EX
                            + "The file is done processing!"
                            + Style.RESET_ALL
                        )
                        break
                    elif prepare_file == "YES":
                        if (
                            done_extract(report_type, report_date).value_done()
                            == "done"
                        ):
                            data1 = sheet.get_all_values()
                            df = pd.DataFrame(data1, columns=[i for i in data1[0]])
                            url = url_extract(report_type, report_date, df).value_done()
                            print(
                                Fore.LIGHTRED_EX
                                + "\n"
                                + report_type
                                + " already extracted previously as below:\n"
                                + "URL: "
                                + Fore.LIGHTCYAN_EX
                                + url
                                + Style.RESET_ALL
                            )
                        elif report_type == "new_classic":
                            data1 = sheet.get_all_values()
                            df = pd.DataFrame(data1, columns=[i for i in data1[0]])
                            url = url_extract(
                                "new_classic_s11", report_date, df
                            ).value_done()
                            create_update_spreadsheet(
                                report_new_classic_mp3,
                                "MP_3",
                                url,
                                nc_sql_time,
                                "none",
                                "none",
                            ).update_gsread()
                            create_update_spreadsheet(
                                report_new_classic_mp4,
                                "MP_4",
                                url,
                                nc_sql_time,
                                "none",
                                "none",
                            ).update_gsread()
                            combine_slack_and_report(url).update_sr()
                        else:
                            gs_update = create_update_spreadsheet(
                                "none",
                                "none",
                                "none",
                                report_date,
                                report_mp3,
                                report_mp4,
                            )
                            url = gs_update.create_gsread_top()
                            combine_slack_and_report(url).update_sr()
                        print(
                            "\n"
                            + Fore.LIGHTYELLOW_EX
                            + "The file is done processing!"
                            + Style.RESET_ALL
                        )
                        break
                    else:
                        print(
                            Fore.LIGHTRED_EX
                            + "Please recheck CONCLUSION and RE-ENTER option!"
                            + Style.RESET_ALL
                        )
            else:
                print(
                    "\n3/ CONCLUSION:",
                    Fore.LIGHTRED_EX + "criteria are NOT satisfied" + Style.RESET_ALL,
                )
                print("\n4/ Reset fault crawlingtasks:", end="")
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
                        + Fore.LIGHTYELLOW_EX
                        + "The file is done processing!"
                        + Style.RESET_ALL
                    )
                    break
                elif reset_error_crawlingtaskid == "YES":
                    slack_msg = send_message_slack(
                        report_type,
                        count_crawlingtasksid,
                        value_status,
                        id_list,
                        "not complete, to recheck",
                    )
                    # slack_msg.send_to_slack()
                    slack_msg.send_to_slack_error()  # thêm đoạn này
                    if_exist_report(report_date, report_type).create_data()
                    data1 = sheet.get_all_values()
                    df = pd.DataFrame(data1, columns=[i for i in data1[0]])
                    update_report(
                        "incomplete",
                        str(id_list),
                        "",
                        "",
                        "",
                        report_type,
                        report_date,
                        df,
                        str(date.today()),
                    ).update_gsh()
                    if report_type == "new_classic_s11":
                        update_report(
                            None,
                            str(id_list),
                            None,
                            None,
                            None,
                            report_type,
                            report_date,
                            df,
                            str(date.today()),
                        ).update_gsh()
                    crawlingtask(id_list, "none", "none", "none").check_query_itunes()
                    crawlingtask(
                        id_list, cursor, conn, "0.5->1.5hr"
                    ).execute_query_itunes()
                    # for id in id_list:
                    #         # # sql_crawl_itunes = """UPDATE crawlingtasks set `Status` = 'complete' where id = '{}';""" #cái này để test
                    #         # sql_crawl_itunes = """UPDATE crawlingtasks set `Status` = NULL where id = '{}';"""
                    #         # sql_complete = sql_crawl_itunes.format(id)
                    #         # print(sql_complete)
                    #         # cursor.execute(sql_complete) # dùng cái này cho PROD.V4
                    #         # conn.commit() # dùng cái này cho PROD.V4
                    #         # # cursor.execute(sql_complete) # dùng cái này cho STG.V4
                    #         # # conn.commit() # dùng cái này cho STG.V4
                    #         crawlingtask(id,cursor,conn).itunes_update()

                    # print('\n' + Fore.LIGHTYELLOW_EX + 'Finish updating fault crawlingtasks, please wait 0.5->1.5hr to recheck!')
                    # print('If the result returns NOT COMPLETE please inform Jay and Cc Joy & Minchan'+ Style.RESET_ALL)
                    print(
                        "\n"
                        + Fore.LIGHTYELLOW_EX
                        + "The file is done processing!"
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
                report_type,
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
                + Fore.LIGHTYELLOW_EX
                + "The file is done processing!"
                + Style.RESET_ALL
            )
            break


def reports():
    crawler_report_msg()
    checking_extract_mp3_mp4()
