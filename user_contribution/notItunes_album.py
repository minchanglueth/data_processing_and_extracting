from connect_confidential.connect_db_v4 import conn, cursor

# from connect_confidential.connect_db_stg import conn, cursor
from connect_confidential.connect_gspread import client_gspread

from datetime import date
import pandas as pd

from colorama import Fore, Style

from slack_report import send_message_slack, maa_crawl_not_itunes
from update_data_report import update_data_gsheet

name = str(input("Please enter your vibbidi name: "))
open_urls = client_gspread.open_by_url(
    "https://docs.google.com/spreadsheets/d/1ZUzx1smeyIKD4PtQ-hhT1kbPSTGRdu8I8NG1uvzcWr4/edit#gid=1108901814"
)

sheet = open_urls.worksheet("Missing Artist Album not Itunes")
# sheet = open_urls.worksheet('Minchan_experiment')

data = sheet.get_all_values()

df_ori = pd.DataFrame(data)


def create_trackinfo(df):
    l = []
    for i in df["Length"]:
        if len(i) == 4:
            i = "0" + i
        l.append(i)
    df["Length"] = l
    df["track_info"] = (
        """{"seq":"""
        + '"'
        + df["Track No"]
        + """","tracknum":"""
        + '"'
        + df["Track No"]
        + """","durations":"""
        + '"'
        + df["Length"]
        + """","trackname":"""
        + '"'
        + df["Track Name"]
        + """","artistname":"""
        + '"'
        + df["Track's Artist"]
        + """"}"""
    )
    return df


def create_albuminfo(df, track_column):
    df["album_info"] = (
        """{"PIC":"""
        + '"'
        + name
        + "_"
        + str(date.today())
        + """","album_url":"""
        + '"'
        + df["Album_Info_URL"]
        + """","image_url":"""
        + '"'
        + df["Album image"]
        + """","track_list":"""
        + "["
        + df[track_column]
        + "]"
        + ""","album_title":"""
        + '"'
        + df["Album Name"]
        + """","artist_album":"""
        + '"'
        + df["Artist Name"]
        + """","total_track":"1","release_date":"""
        + '"'
        + df["Release date"]
        + """"}"""
    )
    return df


def create_query(df):
    df["update_query"] = (
        """Update pointlogs set VerifiedInfo = '"""
        + df["album_info"]
        + """', valid = 1 where id ="""
        + "'"
        + df["ID_MAA"]
        + "';"
    )
    return df


def insert_pointlogsMAA(df):
    df["pointlog_insert"] = (
        """INSERT INTO pointlogs(Id,valid,ActionType,Info) VALUES (uuid4(),0,'MAA','{"pointlog_id":"""
        + '"'
        + df["pointlogid"]
        + """","albums_not_itunes":"""
        + '"'
        + str(date.today())
        + '"'
        + """}');"""
    )


select_MAA = """select Info->>'$.pointlog_id' as ID_CY, id as ID_MAA from pointlogs where Info->>'$.pointlog_id' in {}"""


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
        query = self.query.format(id1_list)  # album_query
        cursor.execute(query)
        result = cursor.fetchall()
        id2_list = pd.DataFrame(result).set_index(0)[1].to_dict()
        return id2_list


def create_df_1track(df_albumartist, df_tocheck):
    albumartist_1track = df_albumartist.index[df_albumartist["Track No"] == 1].to_list()
    album_1track = df_tocheck[
        df_tocheck["album_artist_name"].isin(albumartist_1track)
    ].reset_index()
    album_1track = create_trackinfo(album_1track)
    album_1track = create_albuminfo(album_1track, "track_info")
    return album_1track


def create_df_tracks(df_albumartist, df_tocheck):
    albumartist_tracks = df_albumartist.index[df_albumartist["Track No"] != 1].to_list()
    append_data = []
    for i in albumartist_tracks:
        album_tracks = df_tocheck[df_tocheck["album_artist_name"] == i].reset_index()
        album_tracks = create_trackinfo(album_tracks)
        album_tracks["all_track"] = To_df(
            "track_info", None, album_tracks
        ).combine_columnvalue()
        append_data.append(album_tracks)
    append_data = pd.concat(append_data)
    album_tracks = create_albuminfo(append_data, "all_track")
    return album_tracks


def update_db(df_column):
    for query in df_column:
        print(query)
        cursor.execute(query)
        conn.commit()


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


def find_rowdf(df, column_name):
    header_row = str(df.loc[(df == column_name).any(axis=1)].index)
    for i in ["Int64Index([", "], dtype='int64')"]:
        header_row = header_row.replace(i, "")
    # print(type(header_row))
    # print(header_row)
    header_row = int(header_row)
    # print(header_row)
    return header_row


def find_dfcolumn(df, column_name):
    header_row = find_rowdf(df, column_name)
    df.index += 1
    new_header = df.iloc[header_row]  # create column title
    df.columns = new_header
    df_tocheck_ori = df[header_row + 1 :]
    return df_tocheck_ori


def filter_column_to_check():
    df_tocheck_ori = find_dfcolumn(df_ori, "Missing_Track_Name/Album_Name")
    df_tocheck_ori = df_tocheck_ori[
        [
            "pointlogid",
            "Missing_Track_Name/Album_Name",
            "Youtube_URL",
            "Album_Info_URL",
            "Album Name",
            "Artist Name",
            "Album image",
            "Track No",
            "Track Name",
            "Track's Artist",
            "Length",
            "Total track",
            "Release date",
            "PointlogID MAA",
        ]
    ]
    # print(df_tocheck) #kiểm tra xem column bắt đầu và kết thúc ở đâu
    return df_tocheck_ori


def check_typo(df_tocheck_ori):
    list = ["Album Name", "Artist Name", "Track Name", "Track's Artist"]
    print(
        Fore.LIGHTYELLOW_EX
        + "\nPlease check words with special characters as below:"
        + Style.RESET_ALL
    )
    for i in list:
        for special_character in df_tocheck_ori[i]:
            if '"' in special_character or "'" in special_character:
                print(special_character)
        df_tocheck_ori[i] = df_tocheck_ori[i].str.replace(
            "'", "\\'"
        )  # thay dấu đặc biệt -> gộp tất cả cột có chữ vào làm 1 rồi thay luôn thể, thay cả cột nữa -> nghĩ cách sao thay 1 thể
        df_tocheck_ori[i] = df_tocheck_ori[i].str.replace('"', '\\\\"')

    # df_tocheck[i] = df_tocheck[i].str.replace(r'["][\w\s]+["]',df_tocheck[i].str) #không tìm được cách bỏ dấu ngoặc trước chữ


def check_filledinfo(df_tocheck_ori):
    df_tocheck_ori["album_artist_name"] = (
        df_tocheck_ori["Album Name"] + df_tocheck_ori["Artist Name"]
    )
    df_tocheck_ori["track_artist_name"] = (
        df_tocheck_ori["Track Name"] + df_tocheck_ori["Track's Artist"]
    )
    df_tocheck = df_tocheck_ori[df_tocheck_ori["Album_Info_URL"] != "not found"]

    list_input = [
        "Album_Info_URL",
        "Album Name",
        "Artist Name",
        "Album image",
        "Track No",
        "Track Name",
        "Track's Artist",
        "Length",
        "Total track",
        "Release date",
    ]

    append_emptydata = []
    for i in list_input:
        count = df_tocheck[df_tocheck[i] == ""]
        append_emptydata.append(count)
    append_emptydata = pd.concat(append_emptydata).drop_duplicates(
        subset=None, keep="first"
    )
    count_RowEmptyValue = len(append_emptydata.index)
    # print(count_RowEmptyValue)

    df_albumurl = df_tocheck.groupby("Album_Info_URL", sort=False).nunique()
    df_albumartist = df_tocheck.groupby(["album_artist_name"], sort=False).nunique()
    df_albumimage = df_tocheck.groupby("Album image", sort=False).nunique()

    index_albumurl = df_albumurl[["Track No", "Track Name"]].to_string(index=False)
    index_albumartist = df_albumartist[["Track No", "Track Name"]].to_string(
        index=False
    )
    index_albumimage = df_albumimage[["Track No", "Track Name"]].to_string(index=False)
    count_TrackNo = df_albumartist["Track No"]
    count_TrackArtist = df_albumartist["track_artist_name"]

    special_character_check = input(
        Fore.LIGHTMAGENTA_EX
        + "Enter EXIT if above cases have typo mistakes, CONTINUE if none: "
        + Style.RESET_ALL
    )

    if (
        index_albumurl == index_albumartist == index_albumimage
        and count_TrackNo.equals(count_TrackArtist) == True
        and count_RowEmptyValue == 0
        and special_character_check == "CONTINUE"
    ):
        """So sánh Track No, Track Name group theo 3 criteria có giống nhau ko
        So sánh Track No, Track_Artist_Name có giống nhau ko
        Liệu có ô nào bị empty ko
        Liệu bị typo mistake ko"""

        print(
            Fore.LIGHTMAGENTA_EX
            + "The file is ready to be updated to DB!"
            + Style.RESET_ALL
        )
        decision = str(
            input(Fore.LIGHTBLUE_EX + "Enter YES to proceed: " + Style.RESET_ALL)
        )
        if decision == "YES":
            album_1track = create_df_1track(
                df_albumartist, df_tocheck
            )  # Filter các album có 1 track và tạo album info hoàn chỉnh
            album_tracks = create_df_tracks(
                df_albumartist, df_tocheck
            )  # Filter các album có nhiều track và tạo album info hoàn chỉnh
            df = album_1track.append(
                album_tracks, ignore_index=True
            )  # gộp albums thuộc 2 thể loại trên thành 1 df
            df = df[
                (df["pointlogid"] != "") & (df["PointlogID MAA"] == "")
            ].drop_duplicates(
                subset=["album_artist_name"], keep="first", ignore_index=True
            )  # lọc row ko có pointlogid và pointlogid MAA là rỗng
            # df.to_html('contribution.html')
            insert_pointlogsMAA(df)
            # df.to_html('contribution.html')
            if df["pointlog_insert"].empty:
                print("All albums are already completed inserting previously")
            else:
                print(
                    Fore.LIGHTYELLOW_EX
                    + "\nNow inserting new MAA pointlogs..."
                    + Style.RESET_ALL
                )
                update_db(df["pointlog_insert"])
                pointlog_CYlist = To_df("pointlogid", select_MAA, df).list()
                # print(pointlog_CYlist)
                df["ID_MAA"] = ""
                To_uuid("ID_MAA", "pointlogid", pointlog_CYlist, df).transform()
                create_query(df)
                # df.to_html('contribution.html')
                print(
                    Fore.LIGHTYELLOW_EX
                    + "\nNow verifying Info of MAA pointlogs..."
                    + Style.RESET_ALL
                )
                update_db(df["update_query"])
                print(
                    Fore.LIGHTYELLOW_EX
                    + "\nNow printing out PointlogID MAA to gsheet..."
                    + Style.RESET_ALL
                )

                id_MAA_dict = create_df_dict(df, "album_artist_name", "ID_MAA")
                for album_artist_name in id_MAA_dict:
                    row_index_list = row_index(
                        "album_artist_name", album_artist_name, df_tocheck_ori
                    ).row_index_value()
                    for i in row_index_list:
                        # print(i)
                        # print(type(i))
                        update_data(
                            i,
                            column_index("PointlogID MAA", df_ori).colum_index_value(),
                            id_MAA_dict[album_artist_name],
                            sheet,
                        ).update_data_gspread()
                # df.to_html('contribution.html')

                send_message_slack(
                    "missing albums not from itunes",
                    len(df.index),
                    maa_crawl_not_itunes,
                ).msg_slack()
                send_message_slack(
                    "missing albums not from itunes",
                    len(df.index),
                    maa_crawl_not_itunes,
                ).send_to_slack()
                update_data_gsheet(
                    str(date.today()), "MAA", "crawl_not_itunes", len(df.index)
                )
        else:
            print("No changes made, now Exit")
    elif special_character_check == "EXIT":
        print(
            Fore.LIGHTRED_EX
            + "\nPlease fix typo mistakes from the spreadsheet!"
            + Style.RESET_ALL
        )
    else:
        print(Fore.LIGHTRED_EX + "\nSomething wrong with the file..." + Style.RESET_ALL)
        print(Fore.LIGHTYELLOW_EX + "\nRows which have empty input:" + Style.RESET_ALL)
        if append_emptydata.empty:
            print("No rows have empty input")
        else:
            print(append_emptydata)

        duplicated_albumurl = df_albumurl.index[
            (df_albumurl["album_artist_name"] > 1) | (df_albumurl["Album image"] > 1)
        ].values
        duplicated_albumartist = df_albumartist.index[
            (df_albumartist["Album_Info_URL"] > 1) | (df_albumartist["Album image"] > 1)
        ].values
        duplicated_albumimage = df_albumimage.index[
            (df_albumimage["album_artist_name"] > 1)
            | (df_albumimage["Album_Info_URL"] > 1)
        ].values

        def duplicated_criteria(criteria, duplicated_criteria):
            print(
                Fore.LIGHTYELLOW_EX
                + "\nDuplicated"
                + criteria
                + "from different albums:"
                + Style.RESET_ALL
            )
            if duplicated_criteria.size == 0:
                print("No Duplicated " + criteria + " is found")
            else:
                print(duplicated_criteria)

        duplicated_criteria("albumurls", duplicated_albumurl)
        duplicated_criteria("albumartist", duplicated_albumartist)
        duplicated_criteria("albumimage", duplicated_albumimage)

        print(
            Fore.LIGHTRED_EX
            + "\nMake sure no rows with empty values and no duplicates of albumurl / albumartist / albumimage from different albums!"
            + Style.RESET_ALL
        )

    print("\n" + Fore.LIGHTYELLOW_EX + "The file is done processing!" + Style.RESET_ALL)


def run_albumnotitunes():
    df_tocheck_ori = filter_column_to_check()
    check_typo(df_tocheck_ori)
    check_filledinfo(df_tocheck_ori)


# run_albumnotitunes()
