from connect_confidential.connect_db_v4 import conn, cursor

# from connect_confidential.connect_db_stg import conn, cursor
from connect_confidential.connect_gspread import client_gspread

from datetime import date
import pandas as pd

from colorama import Fore, Style

from slack_report import send_message_slack, maa_crawl_not_itunes
from update_data_report import update_data_gsheet
from df_processing import df_processing, find_dfcolumn, find_rowdf

name = str(input("Please enter your vibbidi name: "))
# open_urls = client_gspread.open_by_url(
#     "https://docs.google.com/spreadsheets/d/1ZUzx1smeyIKD4PtQ-hhT1kbPSTGRdu8I8NG1uvzcWr4/edit#gid=1108901814"
# )

# sheet = open_urls.worksheet("Missing Artist Album not Itunes")
# sheetname = "Missing Artist Album not Itunes"
# # sheet = open_urls.worksheet('Minchan_experiment')

# data = sheet.get_all_values()

# df_ori = pd.DataFrame(data)

# class df_processing:
#     def __init__(self, open_urls):
#         self.url = open_urls

#     def create_sheet(self):
#         sheet = self.url.worksheet(sheetname)
#         return sheet

#     def create_df_ori(self):
#         sheet = self.create_sheet()
#         data = sheet.get_all_values()
#         df_ori = pd.DataFrame(data)
#         return df_ori

class MAA_Contribution_notItunes:
        sheetname = "Missing Artist Album not Itunes"
        # sheetname = "Minchan"
        actiontype = "MAA"
        
        # columns_name:
        pointlogid = "PointlogsID"
        missing_Track_Name_or_Album_Name = "Missing_Track_Name/Album_Name"
        youtube_URL = "Youtube_URL"
        album_Info_URL = "Album_Info_URL"
        album_Name = "Album Name"
        artist_Name = "Artist Name"
        album_image = "Album image"
        track_No = "Track No"
        track_Name = "Track Name"
        track_Artist = "Track's Artist"
        length = "Length"
        total_track = "Total track"
        release_date = "Release date"
        pointlogID_MAA = "PointlogID MAA"
        track_info_new = "track_info"
        album_info_new = "album_info"
        album_artist_name_new = "album_artist_name"
        track_artist_name_new = "track_artist_name"
        pointlog_insert_new = "pointlog_insert"
        update_query_new = "update_query"

        columns_to_check = [
            pointlogid,
            missing_Track_Name_or_Album_Name,
            youtube_URL,
            album_Info_URL,
            album_Name,
            artist_Name,
            album_image,
            track_No,
            track_Name,
            track_Artist,
            length,
            total_track,
            release_date,
            pointlogID_MAA
            ]

        info_user_input = [
            album_Info_URL,
            album_Name,
            artist_Name,
            album_image,
            track_No,
            track_Name,
            track_Artist,
            length,
            total_track,
            release_date
            ]

        columns_typo_check = [
            album_Name,
            artist_Name, 
            track_Name, 
            track_Artist
            ]

def create_trackinfo(df):
    l = []
    for i in df[MAA_Contribution_notItunes.length]:
        if len(i) == 4:
            i = "0" + i
        l.append(i)
    df[MAA_Contribution_notItunes.length] = l
    df[MAA_Contribution_notItunes.track_info_new] = (
        """{"seq":"""
        + '"'
        + df[MAA_Contribution_notItunes.track_No]
        + """","tracknum":"""
        + '"'
        + df[MAA_Contribution_notItunes.track_No]
        + """","durations":"""
        + '"'
        + df[MAA_Contribution_notItunes.length]
        + """","trackname":"""
        + '"'
        + df[MAA_Contribution_notItunes.track_Name]
        + """","artistname":"""
        + '"'
        + df[MAA_Contribution_notItunes.track_Artist]
        + """"}"""
    )
    return df


def create_albuminfo(df, track_column):
    df[MAA_Contribution_notItunes.album_info_new] = (
        """{"PIC":"""
        + '"'
        + name
        + "_"
        + str(date.today())
        + """","album_url":"""
        + '"'
        + df[MAA_Contribution_notItunes.album_Info_URL]
        + """","image_url":"""
        + '"'
        + df[MAA_Contribution_notItunes.album_image]
        + """","track_list":"""
        + "["
        + df[track_column]
        + "]"
        + ""","album_title":"""
        + '"'
        + df[MAA_Contribution_notItunes.album_Name]
        + """","artist_album":"""
        + '"'
        + df[MAA_Contribution_notItunes.album_Name]
        + """","total_track":"1","release_date":"""
        + '"'
        + df[MAA_Contribution_notItunes.release_date]
        + """"}"""
    )
    return df

def insert_pointlogsMAA(df):
    df[MAA_Contribution_notItunes.pointlog_insert_new] = (
        """INSERT INTO pointlogs(Id,valid,ActionType,Info) VALUES (uuid4(),0,'MAA','{"pointlog_id":"""
        + '"'
        + df[MAA_Contribution_notItunes.pointlogid]
        + """","albums_not_itunes":"""
        + '"'
        + str(date.today())
        + '"'
        + """}');"""
    )

id_MAA = "ID_MAA"
select_MAA = "select Info->>'$.pointlog_id' as ID_CY, id as " + id_MAA + " from pointlogs where Info->>'$.pointlog_id' in {}"

def create_query(df):
    df[MAA_Contribution_notItunes.update_query_new] = (
        """Update pointlogs set VerifiedInfo = '"""
        + df[MAA_Contribution_notItunes.album_info_new]
        + """', valid = 1 where id ="""
        + "'"
        + df[id_MAA]
        + "';"
    )
    return df

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
    albumartist_1track = df_albumartist.index[df_albumartist[MAA_Contribution_notItunes.track_No] == 1].to_list()
    album_1track = df_tocheck[
        df_tocheck[MAA_Contribution_notItunes.album_artist_name_new].isin(albumartist_1track)
    ].reset_index()
    album_1track = create_trackinfo(album_1track)
    album_1track = create_albuminfo(album_1track, MAA_Contribution_notItunes.track_info_new)
    return album_1track


def create_df_tracks(df_albumartist, df_tocheck):
    albumartist_tracks = df_albumartist.index[df_albumartist[MAA_Contribution_notItunes.track_No] != 1].to_list()
    append_data = []
    for i in albumartist_tracks:
        album_tracks = df_tocheck[df_tocheck[MAA_Contribution_notItunes.album_artist_name_new] == i].reset_index()
        album_tracks = create_trackinfo(album_tracks)
        album_tracks["all_track"] = To_df(
            MAA_Contribution_notItunes.track_info_new, None, album_tracks
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


# def find_rowdf(df, column_name):
#     header_row = str(df.loc[(df == column_name).any(axis=1)].index)
#     for i in ["Int64Index([", "], dtype='int64')"]:
#         header_row = header_row.replace(i, "")
#     # print(type(header_row))
#     # print(header_row)
#     header_row = int(header_row)
#     # print(header_row)
#     return header_row


# def find_dfcolumn(df, column_name):
#     header_row = find_rowdf(df, column_name)
#     df.index += 1
#     new_header = df.iloc[header_row]  # create column title
#     df.columns = new_header
#     df_tocheck_ori = df[header_row + 1 :]
#     df_tocheck_ori = df_tocheck_ori.loc[:, ~df_tocheck_ori.columns.duplicated()]
#     return df_tocheck_ori


def filter_column_to_check(open_urls):
    df_tocheck_ori = find_dfcolumn(df_processing(MAA_Contribution_notItunes, open_urls).create_df_ori(), MAA_Contribution_notItunes.missing_Track_Name_or_Album_Name)
    df_tocheck_ori = df_tocheck_ori[MAA_Contribution_notItunes.columns_to_check]
    # print(df_tocheck) #kiểm tra xem column bắt đầu và kết thúc ở đâu
    return df_tocheck_ori


def check_typo(df_tocheck_ori):
    list = MAA_Contribution_notItunes.columns_typo_check
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


def check_filledinfo(df_tocheck_ori, open_urls):
    df_tocheck_ori[MAA_Contribution_notItunes.album_artist_name_new] = (
        df_tocheck_ori[MAA_Contribution_notItunes.album_Name] + df_tocheck_ori[MAA_Contribution_notItunes.album_Name]
    )
    df_tocheck_ori[MAA_Contribution_notItunes.track_artist_name_new] = (
        df_tocheck_ori[MAA_Contribution_notItunes.track_Name] + df_tocheck_ori[MAA_Contribution_notItunes.track_Artist]
    )
    df_tocheck = df_tocheck_ori[df_tocheck_ori[MAA_Contribution_notItunes.album_Info_URL] != "not found"]

    append_emptydata = []
    for i in MAA_Contribution_notItunes.info_user_input:
        count = df_tocheck[df_tocheck[i] == ""]
        append_emptydata.append(count)
    append_emptydata = pd.concat(append_emptydata).drop_duplicates(
        subset=None, keep="first"
    )
    count_RowEmptyValue = len(append_emptydata.index)
    # print(count_RowEmptyValue)

    df_albumurl = df_tocheck.groupby(MAA_Contribution_notItunes.album_Info_URL, sort=False).nunique()
    df_albumartist = df_tocheck.groupby([MAA_Contribution_notItunes.album_artist_name_new], sort=False).nunique()
    df_albumimage = df_tocheck.groupby(MAA_Contribution_notItunes.album_image, sort=False).nunique()

    index_albumurl = df_albumurl[[MAA_Contribution_notItunes.track_No, MAA_Contribution_notItunes.track_Name]].to_string(index=False)
    index_albumartist = df_albumartist[[MAA_Contribution_notItunes.track_No, MAA_Contribution_notItunes.track_Name]].to_string(
        index=False
    )
    index_albumimage = df_albumimage[[MAA_Contribution_notItunes.track_No, MAA_Contribution_notItunes.track_Name]].to_string(index=False)
    count_TrackNo = df_albumartist[MAA_Contribution_notItunes.track_No]
    count_TrackArtist = df_albumartist[MAA_Contribution_notItunes.track_artist_name_new]

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
                (df[MAA_Contribution_notItunes.pointlogid] != "") & (df[MAA_Contribution_notItunes.pointlogID_MAA] == "")
            ].drop_duplicates(
                subset=[MAA_Contribution_notItunes.album_artist_name_new], keep="first", ignore_index=True
            )  # lọc row ko có pointlogid và pointlogid MAA là rỗng
            # df.to_html('contribution.html')
            insert_pointlogsMAA(df)
            # df.to_html('contribution.html')
            if df[MAA_Contribution_notItunes.pointlog_insert_new].empty:
                print("All albums are already completed inserting previously")
            else:
                print(
                    Fore.LIGHTYELLOW_EX
                    + "\nNow inserting new MAA pointlogs..."
                    + Style.RESET_ALL
                )
                update_db(df[MAA_Contribution_notItunes.pointlog_insert_new])
                pointlog_CYlist = To_df(MAA_Contribution_notItunes.pointlogid, select_MAA, df).list()
                # print(pointlog_CYlist)
                df[id_MAA] = ""
                To_uuid(id_MAA, MAA_Contribution_notItunes.pointlogid, pointlog_CYlist, df).transform()
                create_query(df)
                # df.to_html('contribution.html')
                print(
                    Fore.LIGHTYELLOW_EX
                    + "\nNow verifying Info of MAA pointlogs..."
                    + Style.RESET_ALL
                )
                update_db(df[MAA_Contribution_notItunes.update_query_new])
                print(
                    Fore.LIGHTYELLOW_EX
                    + "\nNow printing out PointlogID MAA to gsheet..."
                    + Style.RESET_ALL
                )

                id_MAA_dict = create_df_dict(df, MAA_Contribution_notItunes.album_artist_name_new, id_MAA)
                for album_artist_name in id_MAA_dict:
                    row_index_list = row_index(
                        MAA_Contribution_notItunes.album_artist_name_new, album_artist_name, df_tocheck_ori
                    ).row_index_value()
                    for i in row_index_list:
                        # print(i)
                        # print(type(i))
                        update_data(
                            i,
                            column_index(MAA_Contribution_notItunes.pointlogID_MAA, df_processing(MAA_Contribution_notItunes,open_urls).create_df_tocheck_ori()).colum_index_value(),
                            id_MAA_dict[album_artist_name],
                            df_processing(MAA_Contribution_notItunes,open_urls).create_sheet(),
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
                    "user_contribute" ,str(date.today()), "MAA", "crawl_not_itunes", len(df.index)
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
            (df_albumurl[MAA_Contribution_notItunes.album_artist_name_new] > 1) | (df_albumurl[MAA_Contribution_notItunes.album_image] > 1)
        ].values
        duplicated_albumartist = df_albumartist.index[
            (df_albumartist[MAA_Contribution_notItunes.album_Info_URL] > 1) | (df_albumartist[MAA_Contribution_notItunes.album_image] > 1)
        ].values
        duplicated_albumimage = df_albumimage.index[
            (df_albumimage[MAA_Contribution_notItunes.album_artist_name_new] > 1)
            | (df_albumimage[MAA_Contribution_notItunes.album_Info_URL] > 1)
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


def run_albumnotitunes(open_urls):
    df_tocheck_ori = filter_column_to_check(open_urls)
    check_typo(df_tocheck_ori)
    check_filledinfo(df_tocheck_ori, open_urls)


# run_albumnotitunes()
