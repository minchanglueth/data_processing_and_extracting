from connect_confidential.connect_db_final import conn, cursor
# from connect_confidential.connect_db_stg import conn, cursor
from connect_confidential.connect_gspread import client_gspread
from update_data_report import (
    url_extract,
    report_to_db,
    if_exist_report,
    sheet,
    report_invalid_ids,
)
from nc_banner import nc_banner_removed
from genre_corrected import subgenre_specialcharacter

from colorama import Fore, Style

import time
import calendar
import pandas as pd

from datetime import date, timedelta, datetime
from slack_report import send_message_slack

from raw_sql import (
    count_top_single,
    raw_sql_delete,
    count_top_album,
    top_album_removed,
    album_query,
    single_query,
    newclassic_genre,
    newclassic_title,
    count_new_classic,
)
import os.path

day_list = {
    "Saturday": 7,
    "Sunday": 8,
    "Monday": 2,
    "Tuesday": 3,
    "Wednesday": 4,
    "Thursday": 5,
    "Friday": 6,
}  # thay đổi thứ 7 và CN để phù hợp với tg update
myweekday = calendar.day_name[date.today().weekday()]
report_date = str(date.today() - timedelta(day_list[myweekday]))
daily_nc_date = str(date.today() - timedelta(2))  # nhớ chỉnh lại ngày
daily_nc_time = datetime.strptime(
    daily_nc_date + " 16:00:00", "%Y-%m-%d %H:%M:%S"
)  # nhớ chỉnh lại ngày

open_urls = client_gspread.open_by_url(
    "https://docs.google.com/spreadsheets/d/1MHDksbs-RKXhZZ-LRgRhVy_ldAxK8lSzyoJK4sA_Uyo/edit#gid=141704097"
)
# sheet_report = open_urls.worksheet("Minchan")
sheet_report = open_urls.worksheet('top_ab_sg_cs')
data_report = sheet_report.get_all_values()
df_report = pd.DataFrame(data_report, columns=[i for i in data_report[0]])


report_url = input(
    Fore.LIGHTCYAN_EX + "Please input the report url: " + Style.RESET_ALL
)

print("Report types: top_album, top_single, new_classic")
while True:
    report_type = input(
        Fore.LIGHTCYAN_EX + "Please input the report type: " + Style.RESET_ALL
    )
    if (
        report_type == "new_classic"
        and myweekday != "Monday"
        and myweekday != "Tuesday"
    ):
        report_date = daily_nc_date
        break
    elif report_type not in ("top_album", "top_single", "new_classic"):
        print(Fore.LIGHTRED_EX + "Please re-enter the correct option" + Style.RESET_ALL)
    else:
        break

genreuuid_ts_list = {
    "All": "D64FACE6DD924835A8D84D2BE921A626",
    "Pop": "3FC2DF479D2047618FBE73CB6B97BE93",
    "Rock": "A9929F0E1E914C3BBC35BBAC4EAD565B",
    "Dance": "60A78509773745CEBD86C4FA7341F156",
    "R&B/Soul": "665B80D5511B427081BA13086BD7E21D",
    "Hip-Hop/Rap": "61AF9E4DF27442108683E1618F882988",
    "Latino": "6BFD1EA3553940CA82E74E348759BA24",
    "Country": "335DADDBCB3541ADB817EE7746B89633",
    "World": "B5A900EBE39949B19F6A6F3F8172397F",
}

genreuuid_ta_list = {
    "All": "47217A038CAC4EA69F0C5156F0A8BF49",
    "Pop": "336B3E4A9D344CFE8B205726DC254748",
    "Rock": "CC6AE7D721F742849FAFF24A5593D4FF",
    "Dance": "6F8D9700A45B47B28A6470C946DC0FF3",
    "R&B": "97BF03C44B964915A5046EA14666141D",
    "HIP HOP": "07F536C3F9EF4594B3233D664532A1C7",
    "Latino": "79B79F62AFC54620A60C90B4C43522EE",
    "Country": "C1FFD923F7B5404794064B219084F134",
    "World": "5FE8BD1991CA4676BA32A1A872EC59A5",
}

genreid_ta_list = {
    "All": 369415522042770,
    "Pop": 201965558664655,
    "Rock": 206590576975598,
    "Dance": 426715595286541,
    "R&B": 513840613597484,
    "HIP HOP": 288965631908426,
    "Latino": 638265705152197,
    "Country": 537540686841254,
    "World": 903390650219369,
}

nc_category = {"classic": "classic", "new": "new"}


class report_df:
    def __init__(self, sheet_name, column_1, column_2, column_3):
        self.sheet_name = sheet_name
        self.column_1 = column_1
        self.column_2 = column_2
        self.column_3 = column_3

    def create(self):
        open_urls = client_gspread.open_by_url(report_url)
        open_sheet = open_urls.worksheet(self.sheet_name)
        data = open_sheet.get_all_values()
        df1 = pd.DataFrame(data)
        new_header = df1.iloc[0]
        df1.columns = new_header
        if report_type != "new_classic":
            df = df1[["Genre", "Rank", self.column_1]].drop_duplicates(
                subset=["Genre", self.column_1], keep="first", ignore_index=True
            )  # AlbumUUID
            df = df[1:]
        else:
            df_sg1 = df1[["Category", "TrackId", "Sub Genre 1"]].drop_duplicates(
                subset=None, keep="first", ignore_index=True
            )
            df_sg2 = df1[["Category", "TrackId", "Sub Genre 2"]].drop_duplicates(
                subset=None, keep="first", ignore_index=True
            )
            df_sg2.columns = ["Category", "TrackId", "Sub_Genre"]
            df_sg1.columns = ["Category", "TrackId", "Sub_Genre"]
            df = df_sg1.append(df_sg2, ignore_index=True).drop_duplicates(
                subset=None, keep="first", ignore_index=True
            )
            df = df[(df["TrackId"] != "TrackId")]
            df = df[df["Sub_Genre"] != ""]
        df.insert(3, self.column_2, None, True)  # "ChartId"
        df.insert(4, self.column_3, None, True)  # "AlbumID"
        # df = df[1:]
        return df


class To_uuid:
    def __init__(self, column_1, column_2, id_list, df):
        self.column_1 = column_1
        self.column_2 = column_2
        self.id_list = id_list
        self.df = df

    def transform(self):
        for id in self.id_list:
            self.df[self.column_1].loc[self.df[self.column_2] == id] = str(
                self.id_list.get(id)
            )  # ChartId và Genre và genreid_ta_list


class To_df:
    def __init__(self, column, query, df):
        self.column = column
        self.query = query
        self.df = df

    def list(self):
        id1_list = ",".join(
            "'" + str(x)
            for x in self.df[self.df[self.column].notnull()][self.column] + "'"
        )  # AlbumUUID đây là cách biến column thành giá trị các value
        id1_list = "(" + id1_list + ")"
        query = self.query.format(id1_list)  # album_query
        cursor.execute(query)
        result = cursor.fetchall()
        id2_list = pd.DataFrame(result).set_index(0)[1].to_dict()
        return id2_list


def insert_ta(df):
    df["insert_query"] = (
        """insert into v4.chart_album (ChartId, AlbumId, `Order`, Ext) VALUES ("""
        + df["ChartId"]
        + ","
        + df["AlbumID"]
        + ","
        + df["Rank"]
        + ","
        + """'{\"album_title\":\""""
        + df["AlbumUUID"]
        + """\"}');"""
    )


def insert_ts(df):
    df["insert_query"] = (
        """insert into v4.collection_track (collectionId, trackID, Priority) VALUES ('"""
        + df["collectionId"]
        + "','"
        + df["valid_TrackId"]
        + "','"
        + df["Rank"]
        + "');"
    )


def insert_nc(df):
    df["insert_query"] = (
        """insert ignore into v4.classicnew (trackID, GenreId, Category, `ReleasedAt`) VALUES ('"""
        + df["valid_TrackId"]
        + "','"
        + df["GenreId"]
        + "','"
        + df["valid_Category"]
        + "',"
        + "NOW());"
    )


def find_exist_nc(df):
    df["check_ifexist"] = (
        """(TrackId = '"""
        + df["valid_TrackId"]
        + """' and GenreId = '"""
        + df["GenreId"]
        + """' and Category = '"""
        + df["valid_Category"]
        + "')"
    )
    list_ifexist = " or ".join(str(x) for x in df["check_ifexist"])
    return list_ifexist


def id_list(column, df):
    if df["insert_query"].count() == df[column].count():  # ChartId
        return "ok"
    else:
        return "not ok"


def create_invalid_uuid(column, invalid_sheet, df, df_none):
    if id_list(column, df) == "not ok":
        report_invalid_ids(
            invalid_sheet, report_url, df_none
        ).create_and_update()  # invalid_albumsuuid


def get_gsheet_id_from_url(url: str):
    try:
        url_list = url.split("/")
        gsheet_id = url_list[5]
        return gsheet_id
    except IndexError:
        return None


def compare_url(report_type):
    previous_url = url_extract(report_type, report_date, df_report).value_done()
    previous_url_id = get_gsheet_id_from_url(previous_url)
    insert_url_id = get_gsheet_id_from_url(report_url)
    if previous_url_id == insert_url_id:
        return "ok"
    else:
        return "not ok"


def delete_old_report(db_name, column_title, list_value):
    for uuid in list_value:
        delete_query = raw_sql_delete.format(
            db_name, column_title, list_value.get(uuid)
        )  # v4.chart_album và chartid và genreid_ta_list.get(uuid)
        print(delete_query)
        cursor.execute(delete_query)
        conn.commit()


def update_new_report(df):
    for query in df["insert_query"]:
        print(query)
        cursor.execute(query)
        conn.commit()
        if report_type == "new_classic":
            time.sleep(1)


def remove_ta(df):
    cursor.execute(top_album_removed)
    result = cursor.fetchall()
    df_ta_delete = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
    print("\n4/ Albums have fill rate < 60%")
    print(df_ta_delete)
    report_invalid_ids("albums_deleted", report_url, df_ta_delete).create_and_update()
    print("\n5/ Now deleting from database...")
    for albumid in df_ta_delete["AlbumId"]:
        delete_query = raw_sql_delete.format("v4.chart_album", "AlbumId", albumid)
        print(delete_query)
        cursor.execute(delete_query)
        conn.commit()
    print(Fore.LIGHTMAGENTA_EX + "Deleting completed!" + Style.RESET_ALL)


class After_update:
    def __init__(self, result_count, df):
        self.result_count = result_count
        self.df = df

    def checking(self):
        # if_exist_report(report_date,report_type).create_data()
        data3 = sheet.get_all_values()
        df_report_3 = pd.DataFrame(data3, columns=[i for i in data3[0]])
        if report_type == "new_classic":
            query = find_exist_nc(self.df)
            cursor.execute(count_new_classic.format(query))
        else:
            cursor.execute(self.result_count)
        db_count = int(str(cursor.fetchone()).strip("()").strip(","))
        excel_count = len(self.df["insert_query"])
        if db_count == excel_count and compare_url(report_type) == "ok":
            print(
                Fore.LIGHTMAGENTA_EX
                + "Inserting correctly and finished!"
                + Style.RESET_ALL
            )
            report_to_db(
                str(date.today()),
                report_url,
                compare_url(report_type),
                "done",
                report_type,
                report_date,
                None,
                "new_dupbanner",
                df_report_3,
            ).update_gsh_db()
            if report_type == "top_album":
                remove_ta(self.df)  # chỉ áp dụng với top_album
        else:
            print(
                Fore.LIGHTRED_EX
                + "\nMissing some inserting tasks, please recheck!"
                + Style.RESET_ALL
            )
        if report_type != "new_classic":
            print(
                Fore.LIGHTYELLOW_EX + "\nThe file is done processing!" + Style.RESET_ALL
            )
        else:
            print(
                Fore.LIGHTYELLOW_EX
                + "\nNewClassic finished adding subgenres! Now checking banner..."
                + Style.RESET_ALL
            )


def del_update_report(
    column, report_type, db_name, list_value, count_report, df, new_column1
):
    if id_list(column, df) == "ok" and compare_url(report_type) == "ok":
        send_message_slack(
            report_type, id_list(new_column1, df), compare_url(report_type), report_url
        ).send_to_slack()
        if report_type != "new_classic":
            print("\n2/ Deleting old", report_type, "from database...")
            delete_old_report(db_name, column, list_value)
        print("\n3/ Inserting new", report_type, "to database...")
        update_new_report(df)
        After_update(count_report, df).checking()
    else:
        send_message_slack(
            report_type, id_list(new_column1, df), compare_url(report_type), report_url
        ).send_to_slack_error()
        print(
            Fore.LIGHTRED_EX
            + "\nEither some invalid trackid/albumuuid OR incorrect url, please recheck!"
            + Style.RESET_ALL
        )
        if report_type != "new_classic":
            print(
                Fore.LIGHTYELLOW_EX + "\nThe file is done processing!" + Style.RESET_ALL
            )
        else:
            print(
                Fore.LIGHTYELLOW_EX
                + "\nNewClassic FAILED to add subgenres! Now checking banner..."
                + Style.RESET_ALL
            )


class report_update:
    def __init__(
        self,
        base_gsheet,
        removedup_column,
        new_column1,
        new_column2,
        genre_column,
        genre_list,
        check_query,
        db_name,
        count_db,
        invalid_ids,
    ):
        self.base_gsheet = base_gsheet
        self.removedup_column = removedup_column
        self.new_column1 = new_column1
        self.new_column2 = new_column2
        self.genre_column = genre_column
        self.genre_list = genre_list
        self.check_query = check_query
        self.db_name = db_name
        self.count_db = count_db
        self.invalid_ids = invalid_ids

    def run(self):
        df = report_df(
            self.base_gsheet, self.removedup_column, self.new_column1, self.new_column2
        ).create()
        # df.to_html('index_test00.html')
        To_uuid(self.new_column1, self.genre_column, self.genre_list, df).transform()
        id2_list = To_df(self.removedup_column, self.check_query, df).list()
        To_uuid(self.new_column2, self.removedup_column, id2_list, df).transform()

        if report_type == "top_album":
            insert_ta(df)
            df = df[df["ChartId"].notnull()]  # bắt buộc với top_albums thôi
        elif report_type == "top_single":
            insert_ts(df)
        else:
            correct_genretitle = To_df("Sub_Genre", newclassic_title, df).list()
            correct_genretitle.update(subgenre_specialcharacter)
            # df.to_html('index_test01.html')
            df.insert(4, "Subgerne_corrected", None, True)
            # df.to_html('index_test02.html')
            To_uuid(
                "Subgerne_corrected", "Sub_Genre", correct_genretitle, df
            ).transform()
            # df.to_html('index_test1.html')
            df.insert(5, "GenreId", None, True)
            genreid_list = To_df("Subgerne_corrected", newclassic_genre, df).list()
            To_uuid("GenreId", "Subgerne_corrected", genreid_list, df).transform()
            # df.to_html('index_test2.html')
            insert_nc(df)
            # df.to_html('index_test3.html')
            # df.to_html('index_ta.html')
            # print(find_exist_nc(df))
            count_new_classic.format(find_exist_nc(df))

        df_none = df[df["insert_query"].isnull()]

        create_invalid_uuid(self.new_column1, self.invalid_ids, df, df_none)
        print("\n1/ ", end="")
        send_message_slack(
            report_type,
            id_list(self.new_column1, df),
            compare_url(report_type),
            report_url,
        ).msg_slack()

        if_exist_report(report_date, report_type).create_data()
        data1 = sheet.get_all_values()
        df_report_2 = pd.DataFrame(data1, columns=[i for i in data1[0]])

        report_to_db(
            str(date.today()),
            report_url,
            compare_url(report_type),
            None,
            report_type,
            report_date,
            None,
            "new_dupbanner",
            df_report_2,
        ).update_gsh_db()

        del_update_report(
            self.new_column1,
            report_type,
            self.db_name,
            self.genre_list,
            self.count_db,
            df,
            self.new_column1,
        )


def update_db_report():
    if report_type == "top_album":
        report_update(
            "MP_3",
            "AlbumUUID",
            "ChartId",
            "AlbumID",
            "Genre",
            genreid_ta_list,
            album_query,
            "v4.chart_album",
            count_top_album,
            "invalid_albumuuid",
        ).run()
    elif report_type == "top_single":
        report_update(
            "MP_3",
            "TrackId",
            "collectionId",
            "valid_TrackId",
            "Genre",
            genreuuid_ts_list,
            single_query,
            "v4.collection_track",
            count_top_single,
            "invalid_trackid",
        ).run()
    elif report_type == "new_classic":
        report_update(
            "Subgenre_Update",
            "TrackId",
            "valid_Category",
            "valid_TrackId",
            "Category",
            nc_category,
            single_query,
            None,
            None,
            "invalid_criteria",
        ).run()
        nc_banner_removed(report_url)
