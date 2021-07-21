import time
from datetime import date

import pandas as pd
from colorama import Fore, Style
from connect_confidential.connect_db_v4 import conn, cursor
# from connect_confidential.connect_db_stg import conn, cursor
from connect_confidential.connect_gspread import client_gspread
from gspread_dataframe import set_with_dataframe

from itunes import (check_validate, check_validate_albumitune,
                    check_validate_artistitune)
from notItunes_album import find_dfcolumn, find_rowdf, name
from raw_sql import ma_album_status, ma_image_status, maa_status
from slack_report import (ma_itunes_plupdate, maa_itunes_plupdate,
                          send_message_slack)
from update_data_report import report_invalid_ids, update_data_gsheet

# from notItunes_recheckpl import create_column

def create_artistinfo(df):
    df["artist_info"] = (
        """{"PIC":"""
        + '"'
        + name
        + "_"
        + str(date.today())
        + """","artist_image_url":"""
        + '"'
        + df["Artist_image"]
        + """","itunes_artist_id":"""
        + '"'
        + df["itune_id"]
        + """"}"""
    )
    return df


def create_albuminfo(df):
    df["album_info"] = (
        """{"PIC":"""
        + '"'
        + name
        + "_"
        + str(date.today())
        + """","album_id":"""
        + '"'
        + df["itune_id"]
        + """","album_region":"""
        + '"'
        + df["itune_region"]
        + """"}"""
    )
    return df


def create_query_approve(df, type_info, action_type):
    df["approve_query"] = (
        """Update pointlogs set VerifiedInfo = '"""
        + df[type_info]
        + """', valid = 1 where ActionType = '"""
        + action_type
        + """' and id ="""
        + "'"
        + df["PointlogsID"]
        + "';"
    )
    return df


def create_query_reject(df, action_type):
    df["reject_query"] = (
        """Update pointlogs set valid = -2 where ActionType = '"""
        + action_type
        + """' and id ="""
        + "'"
        + df["PointlogsID"]
        + "';"
    )
    return df


select_valid_plid_MA = """select id, id from pointlogs where ActionType = 'MA' and valid != 1 and id in {}"""  # chọn các pointlog chưa được verify
select_valid_plid_MAA = """select id, id from pointlogs where ActionType = 'MAA' and valid != 1 and id in {}"""
# create_query_reject = """Update pointlogs set valid = -2 where id = {}"""


class Contributions:
    class MA_Contribution:
        sheetname = "Missing Artist"
        actiontype = "MA"
        column_filter = "Artist name on Itunes"
        itune_validate = check_validate_artistitune
        column_info = "artist_info"
        slack_title = "missing artists found from itunes"
        slack_message = ma_itunes_plupdate
        create_info = create_artistinfo
        query_valid_plid = select_valid_plid_MA
        list_input = ["Artist name on Itunes", "Artist_Itunes_link", "Artist_image"]
        ma_status_image = ma_image_status
        ma_status_album = ma_album_status
        df_columns_shorten = [
            "Assignee",
            "PointlogsID",
            "Artist_name",
            "Artist name on Itunes",
            "Artist_Itunes_link",
            "Artist_image",
        ]

    class MAA_Contribution:
        sheetname = "Missing Artist's Album"
        # sheetname = "Minchan"
        actiontype = "MAA"
        column_filter = "Album's Itunes link"
        itune_validate = check_validate_albumitune
        column_info = "album_info"
        slack_title = "missing albums found from itunes"
        slack_message = maa_itunes_plupdate
        create_info = create_albuminfo
        query_valid_plid = select_valid_plid_MAA
        list_input = ["Album's Itunes link"]
        maa_status_album = maa_status
        df_columns_shorten = [
            "Assignee",
            "PointlogsID",
            "Artist_Album",
            "Missing_Album_Name",
            "Album's Itunes link",
        ]


class To_df:
    def __init__(self, column, query, df):
        self.column = column
        self.query = query
        self.df = df

    def combine_columnvalue(self):
        id1_list = ",".join(
            str(x) for x in self.df[self.df[self.column].notnull()][self.column]
        )  # AlbumUUID đây là cách biến column thành giá trị các value
        return id1_list

    def list(self):
        id1_list = ",".join(
            "'" + str(x)
            for x in self.df[self.df[self.column].notnull()][self.column] + "'"
        )  # AlbumUUID đây là cách biến column thành giá trị các value
        id1_list = "(" + id1_list + ")"
        if self.query == ma_image_status:
            query = self.query.format(id1_list, id1_list)
        else:
            query = self.query.format(id1_list)  # album_query
        cursor.execute(query)
        result = cursor.fetchall()
        if pd.DataFrame(result).empty:  # nhớ sửa lại cho Jenny đoạn này
            id2_list = {}
        else:
            id2_list = pd.DataFrame(result).set_index(0)[1].to_dict()
        return id2_list


class To_uuid:
    def __init__(self, column_1, column_2, id_list, df):
        self.column_1 = column_1
        self.column_2 = column_2
        self.id_list = id_list
        self.df = df

    def transform(self):
        if self.column_1 not in ["Image_status", "Itunes_status"]:
            self.df[self.column_1] = ""
        for id in self.id_list:
            self.df[self.column_1].loc[self.df[self.column_2] == id] = str(
                self.id_list.get(id)
            )  # ChartId và Genre và genreid_ta_list


def update_db(df_column):
    for query in df_column:
        print(query)
        cursor.execute(query)
        conn.commit()


class column_index:
    def __init__(self, value, df):
        self.value = value
        self.df = df

    def colum_index_value(self):
        return self.df.columns.get_loc(self.value) + 1


class row_index:
    def __init__(self, column_name, column_value, df):
        self.column_value = column_value
        self.column_name = column_name
        self.df = df

    def row_index_value(self):
        row_index = self.df.index[
            self.df[self.column_name] == self.column_value
        ].to_list()
        # return int(str(row_index).strip('[]'))
        return row_index


# row_index_list = row_index().row_index_value()
class update_data:
    def __init__(self, row, column, value, sheet):
        self.row = row
        self.column = column
        self.value = value
        self.sheet = sheet

    def update_data_gspread(self):
        self.sheet.update_cell(self.row, self.column, self.value)


def create_df_dict(df, columnkey, columnvalue):
    df_dict = dict(zip(df[columnkey], df[columnvalue]))
    return df_dict


def create_df_approve(df_ori, contribution):
    df_approve = df_ori[df_ori[contribution.column_filter] != "not found"]
    return df_approve


def create_df_reject(df_ori, contribution):
    df_reject = df_ori[df_ori[contribution.column_filter] == "not found"]
    return df_reject


def df_filter_column(df, contribution):
    df = df[contribution.df_columns_shorten]
    return df


# 'missing artists found from itunes' ma_itunes_plupdate
# report_description
# MA
def update_slack_report(
    slack_title, df_approve, slack_message, report_actiontype, report_description
):
    send_message_slack(slack_title, len(df_approve.index), slack_message).msg_slack()
    send_message_slack(
        slack_title, len(df_approve.index), slack_message
    ).send_to_slack()
    update_data_gsheet(
        str(date.today()), report_actiontype, report_description, len(df_approve.index)
    )


open_urls = client_gspread.open_by_url(
    "https://docs.google.com/spreadsheets/d/1ZUzx1smeyIKD4PtQ-hhT1kbPSTGRdu8I8NG1uvzcWr4/edit#gid=1108901814"
)


class df_processing:
    def __init__(self, contribution):
        self.contribution = contribution

    def create_sheet(self):
        sheet = open_urls.worksheet(self.contribution.sheetname)
        return sheet

    def create_df_ori(self):
        sheet = self.create_sheet()
        data = sheet.get_all_values()
        df_ori = pd.DataFrame(data)
        return df_ori

    def create_df_tocheck_ori(self):
        # Tạo df với columns có chứa PoinglogsID
        df_ori = self.create_df_ori()
        df_tocheck_ori = find_dfcolumn(df_ori, "PointlogsID")
        return df_tocheck_ori

    def create_prevalid_row(self):
        # Xác định dòng cuối giá trị prevalid và tạo df với các dòng sau prevalid
        df_ori = self.create_df_ori()
        find_dfcolumn(df_ori, "PointlogsID")
        prevalid_row = find_rowdf(df_ori, "pre_valid")
        return prevalid_row

    def create_last_prevalid_row_ori(self):
        df_tocheck_ori = self.create_df_tocheck_ori()
        df_prevalid = df_tocheck_ori["pre_valid"][df_tocheck_ori["pre_valid"] != ""]
        # print(df_prevalid)
        if df_prevalid.empty:
            last_prevalid_row_ori = self.create_prevalid_row()
        else:
            last_prevalid_row_ori = df_prevalid[-1:].index.tolist()[0]

        return last_prevalid_row_ori

    def create_df_tocheck(self):
        last_prevalid_row_ori = self.create_last_prevalid_row_ori()
        prevalid_row = self.create_prevalid_row()
        df_tocheck_ori = self.create_df_tocheck_ori()
        last_prevalid_row = last_prevalid_row_ori - prevalid_row
        df_tocheck = df_tocheck_ori[last_prevalid_row:]
        return df_tocheck

    def create_df_approve(self):
        df_tocheck = self.create_df_tocheck()
        df_approve = create_df_approve(df_tocheck, self.contribution)
        return df_approve


def check_validate_and_update_db(contribution):
    df_approve = df_processing(contribution).create_df_approve()
    df_approve = check_validate(
        df_approve, contribution.itune_validate, contribution.column_filter
    )
    # check validate cho df_approve
    df_approve["itune_id_region"] = (
        df_approve["itune_id"] + "_" + df_approve["itune_region"]
    )

    df_donechecking = (
        df_approve[["itune_id", "itune_region"]]
        .set_index("itune_id")["itune_region"]
        .to_dict()
    )

    print(
        "\n"
        + Fore.LIGHTGREEN_EX
        + "Now checking validate of itunesurl..."
        + Style.RESET_ALL
    )
    start1 = time.time()
    dict_itune_id_region_validate = {}
    for i in df_donechecking:
        itune_id_region = i + "_" + df_donechecking.get(i)
        validate_result = contribution.itune_validate(i, df_donechecking.get(i))
        dict_itune_id_region_validate.update({itune_id_region: validate_result})
    end1 = time.time()
    To_uuid(
        "check_validate", "itune_id_region", dict_itune_id_region_validate, df_approve
    ).transform()
    print("finished in", end1 - start1, "seconds")
    # df_approve.to_html('df_approve.html')

    df_filtered_false_validate = df_approve[df_approve["check_validate"] != "True"]
    if df_filtered_false_validate.empty:
        df_tocheck = df_processing(contribution).create_df_tocheck()
        print("\n" + Fore.LIGHTBLUE_EX + "Now updating pointlogs..." + Style.RESET_ALL)
        df_reject = create_query_reject(
            create_df_reject(df_tocheck, contribution), contribution.actiontype
        )
        df_approve = contribution.create_info(df_approve)
        df_approve = create_query_approve(
            df_approve, contribution.column_info, contribution.actiontype
        )
        update_db(df_reject["reject_query"])
        update_db(df_approve["approve_query"])
        df_tocheck["pre_valid"] = str(date.today())
        start_column_insert = df_tocheck.columns.get_loc("pre_valid") + 1
        sheet_tocheck = df_processing(contribution).create_sheet()
        last_prevalid_row_ori = df_processing(
            contribution
        ).create_last_prevalid_row_ori()
        set_with_dataframe(
            sheet_tocheck,
            df_tocheck[["pre_valid"]],
            row=last_prevalid_row_ori + 1,
            col=start_column_insert,
            include_column_header=False,
        )
        print(Fore.LIGHTGREEN_EX + "\nSending message to slack..." + Style.RESET_ALL)
        update_slack_report(
            contribution.slack_title,
            df_approve,
            contribution.slack_message,
            contribution.actiontype,
            "crawl_itunes",
        )
    else:
        print(
            Fore.LIGHTRED_EX
            + "Please recheck invalid Artist Itunes from below cases"
            + Style.RESET_ALL
        )
        print(df_filter_column(df_filtered_false_validate, contribution.sheetname))
    # df_tocheck.to_html('df_tocheck_ori.html')


def check_valid_empty_dup(contribution):
    # check valid:
    df_approve = df_processing(contribution).create_df_approve()
    list_input = contribution.list_input

    # print(df_approve)
    pointlogID_all = df_approve["PointlogsID"].to_list()

    # print(pointlogID_MAA_all)
    pointlogID_valid = To_df(
        "PointlogsID", contribution.query_valid_plid, df_approve
    ).list()
    invalid_plid = [i for i in pointlogID_all if i not in pointlogID_valid]
    valid_plid = [i for i in pointlogID_valid]
    # check empty and duplicate:
    append_emptydata = []
    for i in list_input:
        count = df_approve[df_approve[i] == ""]
        append_emptydata.append(count)
    append_emptydata = pd.concat(append_emptydata).drop_duplicates(
        subset=None, keep="first"
    )
    pointlogs_duplicated = df_approve[df_approve.duplicated(subset=["PointlogsID"])]

    if (
        append_emptydata.empty
        and pointlogs_duplicated.empty
        and len(valid_plid) >= 1
        and len(invalid_plid) == 0
    ):
        print(
            Fore.LIGHTGREEN_EX + "\nAll pointlogs are valid to update" + Style.RESET_ALL
        )
        print(
            Fore.LIGHTGREEN_EX
            + "\nNo rows have empty input / No rows have been duplicated"
            + Style.RESET_ALL
        )
        check_validate_and_update_db(contribution)
    else:
        print(
            Fore.LIGHTGREEN_EX
            + "\nvalid pointlogs.id list as below:\n"
            + Style.RESET_ALL,
            valid_plid,
        )
        print(
            Fore.LIGHTGREEN_EX
            + "\ninvalid pointlogs.id list as below:\n"
            + Style.RESET_ALL,
            invalid_plid,
        )
        print(
            Fore.LIGHTRED_EX
            + "\nRows with missing info as below, please ignore if empty"
            + Style.RESET_ALL
        )
        print(df_filter_column(append_emptydata, contribution.sheetname))
        print(
            Fore.LIGHTRED_EX
            + "\nRows with duplicated pointlogs as below, please ignore if empty"
            + Style.RESET_ALL
        )
        print(df_filter_column(pointlogs_duplicated, contribution.sheetname))

    print(Fore.LIGHTYELLOW_EX + "\nThe file is done processing!" + Style.RESET_ALL)


def check_status(contribution, status_type, status_column, pre_valid_list):
    df_tocheck_ori = df_processing(contribution).create_df_tocheck_ori()
    df_check_status = df_tocheck_ori[df_tocheck_ori["pre_valid"].isin(pre_valid_list)]
    lookup_list = To_df("PointlogsID", status_type, df_check_status).list()
    To_uuid(status_column, "PointlogsID", lookup_list, df_tocheck_ori).transform()
    start_column_insert = df_tocheck_ori.columns.get_loc(status_column) + 1
    sheet_tocheck = df_processing(contribution).create_sheet()
    prevalid_row = df_processing(contribution).create_prevalid_row()
    set_with_dataframe(
        sheet_tocheck,
        df_tocheck_ori[[status_column]],
        row=prevalid_row + 1,
        col=start_column_insert,
        include_column_header=False,
    )


def check_image_and_album_status(contribution, pre_valid_list):
    if contribution == Contributions.MA_Contribution:
        check_status(contribution, ma_image_status, "Image_status", pre_valid_list)
        check_status(contribution, ma_album_status, "Itunes_status", pre_valid_list)
    elif contribution == Contributions.MAA_Contribution:
        check_status(contribution, maa_status, "Itunes_status", pre_valid_list)


# check_valid_empty_dup(Contributions.MAA_Contribution)
# pre_valid_list = ["2021-06-29"]

# check_image_album_status(Contributions.MAA_Contribution,pre_valid_list)
# check_image_album_status(Contributions.MA_Contribution, pre_valid_list)
