from connect_confidential.connect_db_v4 import conn, cursor
# from connect_confidential.connect_db_stg import conn, cursor
from connect_confidential.connect_gspread import client_gspread

from colorama import Fore, Style

import pandas as pd

print(
    Fore.LIGHTGREEN_EX + "\nWhich video type to recrawl?\n",
    "A. MP3\n",
    "B. MP4\n",
    "C. Both\n" + Style.RESET_ALL,
)
option = str(
    input(Fore.LIGHTMAGENTA_EX + "Please input the option: " + Style.RESET_ALL)
)

report_url = input(
    Fore.LIGHTMAGENTA_EX + "Please input the report url: " + Style.RESET_ALL
)
open_urls = client_gspread.open_by_url(report_url)
file_name = open_urls.title
sheet_name = "missing_youtube"
sheet = open_urls.worksheet(sheet_name)
data = sheet.get_all_values()
df = pd.DataFrame(data, columns=[i for i in data[0]])
df = df[1:]

# def create_df(url_crawled,url_to_replace):
def delete_query(df_filtered, sheet):
    df_filtered["delete_query"] = (
        """DELETE from crawlingtasks where ActionId = 'F91244676ACD47BD9A9048CF2BA3FFC1' and ObjectId = '"""
        + df_filtered["TrackId"]
        + "'"
        + " and TaskDetail->>'$.PIC' = '"
        + file_name
        + "_"
        + sheet
        + "'"
        + " and (`Status` = 'incomplete' or `Status` is NULL);"
    )


def insert_query(df_filtered, formatid, url_to_replace, sheet):
    df_filtered["insert_query"] = (
        "insert into crawlingtasks(Id, ObjectID, ActionId, TaskDetail, Priority) values (uuid4(), '"
        + df_filtered["TrackId"]
        + "','F91244676ACD47BD9A9048CF2BA3FFC1', JSON_SET(IFNULL(crawlingtasks.TaskDetail, JSON_OBJECT()), '$.when_exists', 'skip', '$.youtube_url', '"
        + df_filtered[url_to_replace]
        + "', '$.data_source_format_id', '"
        + formatid
        + "', '$.PIC', '"
        + file_name
        + "_"
        + sheet
        + "'),1998);"
    )


def update_db(df_column):
    for query in df_column:
        print(query)
        cursor.execute(query)
        conn.commit()


def delete_insert_mp3(df_filtered):
    delete_query(df_filtered, "MP_3")

    print(
        Fore.LIGHTCYAN_EX
        + "\nNow deleting old mp3 crawlingtask(s)..."
        + Style.RESET_ALL
    )
    update_db(df_filtered["delete_query"])

    print(
        Fore.LIGHTCYAN_EX
        + "\nNow inserting new mp3 crawlingtask(s)..."
        + Style.RESET_ALL
    )
    insert_query(
        df_filtered, "1A67A5F1E0D84FB9B48234AE65086375", "MP3_to_replace", "MP_3"
    )
    update_db(df_filtered["insert_query"])

    df_filtered_c = df_filtered[
        (df_filtered["Type"] == "c") | (df_filtered["Type"] == "z") | (df_filtered["Type"] == "C") | (df_filtered["Type"] == "Z")
    ]
    insert_query(
        df_filtered_c, "C78F687CB3BE4D90B30F49435317C3AC", "MP3_to_replace", "MP_3"
    )
    update_db(df_filtered_c["insert_query"])

    df_filtered_d = df_filtered[(df_filtered["Type"] == "d") | (df_filtered["Type"] == "D")]
    insert_query(
        df_filtered_d, "3CF047F3B0F349B3A9A39CE7FDAB1DA6", "MP3_to_replace", "MP_3"
    )
    update_db(df_filtered_d["insert_query"])


def delete_insert_mp4(df_filtered):
    delete_query(df_filtered, "MP_4")
    print(
        Fore.LIGHTCYAN_EX
        + "\nNow deleting old mp4 crawlingtask(s)..."
        + Style.RESET_ALL
    )
    update_db(df_filtered["delete_query"])

    print(
        Fore.LIGHTCYAN_EX
        + "\nNow inserting new mp4 crawlingtask(s)..."
        + Style.RESET_ALL
    )
    insert_query(
        df_filtered, "74BA994CF2B54C40946EA62C3979DDA3", "MP4_to_replace", "MP_4"
    )
    update_db(df_filtered["insert_query"])


def filtered_delete_insert_mp3():
    df_filtered = df[["TrackId", "MP3_crawled", "MP3_to_replace", "Type"]]
    df_filtered = df_filtered[
        (df_filtered["MP3_to_replace"] != "")
        & (
            (df_filtered["Type"] == "c")
            | (df_filtered["Type"] == "d")
            | (df_filtered["Type"] == "z")
            | (df_filtered["Type"] == "C")
            | (df_filtered["Type"] == "D")
            | (df_filtered["Type"] == "Z")
        )
    ]
    delete_insert_mp3(df_filtered)


def filtered_delete_insert_mp4():
    df_filtered = df[["TrackId", "MP4_crawled", "MP4_to_replace"]]
    df_filtered = df_filtered[(df_filtered["MP4_to_replace"] != "")]
    delete_insert_mp4(df_filtered)


def reset_crawlingsid():
    if option == "A":
        filtered_delete_insert_mp3()
    elif option == "B":
        filtered_delete_insert_mp4()
    elif option == "C":
        filtered_delete_insert_mp3()
        filtered_delete_insert_mp4()
    else:
        print("No changes made, now exit")

    print("\n" + Fore.LIGHTYELLOW_EX + "The file is done processing!" + Style.RESET_ALL)


# reset_crawlingsid()

# df_filtered.to_html('nc_subgenre_summary/nc_Test.html')
