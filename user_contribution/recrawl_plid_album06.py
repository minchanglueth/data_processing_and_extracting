from connect_confidential.connect_db_v4 import conn, cursor

# from connect_confidential.connect_db_stg import conn, cursor
from connect_confidential.connect_gspread import client_gspread

import pandas as pd
from colorama import Fore, Style
from datetime import date

# from slack_report import send_message_slack
# from update_data_report import update_data_gsheet

open_urls = client_gspread.open_by_url(
    "https://docs.google.com/spreadsheets/d/1ZUzx1smeyIKD4PtQ-hhT1kbPSTGRdu8I8NG1uvzcWr4/edit#gid=218846379&fvid=15272935"
)
gsheet_name = "Youtube collect_experiment"
file_name = open_urls.title
sheet = open_urls.worksheet(gsheet_name)
data = sheet.get_all_values()


def get_itune_id_region_from_itune_url(url: str):
    itune_id = url.split("/")[-1]
    itune_region = url.split("/")[3]
    if "?" in itune_id:
        itune_id = itune_id.split("?")[0]
    return [itune_id, itune_region]


def get_dict(dict_name, position, df):
    dict_name = {}
    for ituneurl in df["Itunes_Album_Link"]:
        itune_id_region = get_itune_id_region_from_itune_url(ituneurl)
        dict_name.update({ituneurl: itune_id_region[position]})
    return dict_name


class To_uuid:
    def __init__(self, column_1, column_2, id_list, df):
        self.column_1 = column_1
        self.column_2 = column_2
        self.id_list = id_list
        self.df = df

    def transform(self):
        self.df[self.column_1] = ""
        for id in self.id_list:
            self.df[self.column_1].loc[self.df[self.column_2] == id] = str(
                self.id_list.get(id)
            )  # ChartId và Genre và genreid_ta_list


def update_db(df_column):
    for query in df_column:
        print(query)
        # cursor.execute(query)
        # conn.commit()


def check_pointlog(check_query, recrawl_pointlogID_CY):
    query = check_query.format(recrawl_pointlogID_CY)  # album_query
    print(query)
    cursor.execute(query)
    result = cursor.fetchall()
    pointlog_list = pd.DataFrame(result)[0].to_list()
    # pointlog_list = pd.DataFrame(result).values.tolist()
    return pointlog_list


def recrawl_album06(recrawl_pointlogID_CY):

    valid_plid_query = """select id from pointlogs where ActionType = 'CY' and valid = 0 and id in {}"""
    valid_plid = check_pointlog(
        valid_plid_query, str(recrawl_pointlogID_CY).replace("[", "(").replace("]", ")")
    )
    invalid_plid = [i for i in recrawl_pointlogID_CY if i not in valid_plid]
    print(
        Fore.LIGHTGREEN_EX + "\nvalid pointlogs.id list as below:\n" + Style.RESET_ALL,
        valid_plid,
    )
    print(
        Fore.LIGHTGREEN_EX
        + "\ninvalid pointlogs.id list as below:\n"
        + Style.RESET_ALL,
        invalid_plid,
    )

    decision = input(
        Fore.LIGHTMAGENTA_EX
        + "\nEnter YES if you still want to recrawl those valid pointlogs: "
        + Style.RESET_ALL
    )

    if decision == "YES" and len(valid_plid) >= 1:
        df_ori = pd.DataFrame(data)
        df_ori.index += 1
        new_header = df_ori.iloc[0]  # create column title
        df_ori.columns = new_header
        df_ori = df_ori[1:]
        df_ori = df_ori[["PointlogsID", "Itunes_Album_Link", "06_id"]]

        df_filtered = df_ori[df_ori["PointlogsID"].isin(valid_plid)].reset_index()
        To_uuid(
            "itune_id",
            "Itunes_Album_Link",
            get_dict("ituneurl_id_dict", 0, df_filtered),
            df_filtered,
        ).transform()
        To_uuid(
            "itune_region",
            "Itunes_Album_Link",
            get_dict("ituneurl_region_dict", 1, df_filtered),
            df_filtered,
        ).transform()

        df_filtered["query"] = (
            """INSERT INTO crawlingtasks(Id,Priority,ActionID,Taskdetail) VALUES (uuid4(),1997,'9C8473C36E57472281A1C7936108FC06','{"PIC":"""
            + '"'
            + file_name
            + "_"
            + gsheet_name
            + "_"
            + str(date.today())
            + '"'
            + """, "region": """
            + '"'
            + df_filtered["itune_region"]
            + '"'
            + """, "album_id": """
            + '"'
            + df_filtered["itune_id"]
            + '",'
            + """"is_new_release": false}');"""
        )

        # df_filtered.to_html('user_contribution/recrawl_06list.html')

        print(Fore.LIGHTBLUE_EX + "\nNow insert crawlingtasks..." + Style.RESET_ALL)
        update_db(df_filtered["query"])
    else:
        print("\nNo changes made, now exit")

    print(Fore.LIGHTYELLOW_EX + "\nThe file is done processing!" + Style.RESET_ALL)


# --------------------------------INSTRUCTION---------------------------------------------------------------


""" 
Điền list poinlogID mà cần crawl lại ở dưới đây nhé
Example:
recrawl_pointlogID_CY = ['DDFCACEA9584463282EBD00150337A8A'
'F22F3FCC48D847A0B7A7613F8B7D1AF8']
"""

recrawl_pointlogID_CY = ["C7FC23ECF4F644BCB671AC6E32526C55"]


# ------------------------------------END-------------------------------------------------------------------

recrawl_album06(recrawl_pointlogID_CY)
