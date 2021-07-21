from connect_confidential.connect_db_v4 import conn, cursor

# from connect_confidential.connect_db_stg import conn, cursor
from connect_confidential.connect_gspread import client_gspread

from datetime import date
import pandas as pd

from colorama import Fore, Style

from slack_report import send_message_slack, cy_notItunes_extract, cy_notItunes_plupdate
from update_data_report import update_data_gsheet
from raw_sql import (
    missing_albums_existed,
    missing_albums_added,
    artist_name_albums,
    uri_to_trackid,
    track_to_trackid,
    track_artist,
    track_title,
    youtube_artist_query,
)

from notItunes_album import (
    filter_column_to_check,
    open_urls,
    find_dfcolumn,
    find_rowdf,
    update_db,
)
from update_data_report import report_invalid_ids

from unidecode import unidecode
import re
from fuzzywuzzy import fuzz
from gspread_dataframe import set_with_dataframe
import urllib
import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.ext.declarative import declarative_base, declared_attr

gsheet_name = 'Youtube collect_Recheck_notItunes'
# gsheet_name = "Minchan"
sheet_contribute_notItunes = open_urls.worksheet(gsheet_name)
file_name = open_urls.title
data_contribute_notItunes = sheet_contribute_notItunes.get_all_values()
df_contribute_notItunes = pd.DataFrame(data_contribute_notItunes)

df_notItunes_ori = find_dfcolumn(df_contribute_notItunes, "PointlogsID")
# print(df_contribute_notItunes)
prevalid_row = find_rowdf(df_contribute_notItunes, "pre_valid")
df_prevalid = df_notItunes_ori["pre_valid"][df_notItunes_ori["pre_valid"] != ""]
# print(prevalid_row)
# print(df_prevalid)
if df_prevalid.empty:
    last_prevalid_row_ori = prevalid_row
else:
    last_prevalid_row_ori = df_prevalid[-1:].index.tolist()[0]
# print(last_prevalid_row_ori)
# print(prevalid_row)
last_prevalid_row = last_prevalid_row_ori - prevalid_row

df_notItunes = df_notItunes_ori[last_prevalid_row:]
# print(last_prevalid_row)
# print(df_notItunes)


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
            )


class To_df:
    def __init__(self, column, query, df, id_list):
        self.column = column
        self.query = query
        self.df = df
        self.id_list = id_list

    def list(self):
        if self.id_list == None:
            id_list = ",".join(
                "'" + str(x)
                for x in self.df[self.df[self.column].notnull()][self.column] + "'"
            )  # AlbumUUID đây là cách biến column thành giá trị các value
            id_list = "(" + id_list + ")"
            return id_list

    def create_df(self):
        if self.id_list == None:
            id_list = self.list()
        elif self.df == None:
            id_list = self.id_list
        query = self.query.format(id_list)  # album_query
        cursor.execute(query)
        result = cursor.fetchall()
        if pd.DataFrame(result).empty:  # nhớ sửa lại cho Jenny đoạn này
            id2_list = {}
        else:
            id2_list = pd.DataFrame(result).set_index(0)[1].to_dict()
        # print(id2_list)
        return id2_list


def create_column(new_column, lookup_column, query, df):
    lookup_list = To_df(lookup_column, query, df, None).create_df()
    To_uuid(new_column, lookup_column, lookup_list, df).transform()


def extract_report():

    df = filter_column_to_check()
    df = df[
        (df["pointlogid"] != "") & (df["PointlogID MAA"] != "")
    ]  # lọc các TH có youtubeurl ở sheet Missing Artist Album not Itunes
    df = df[
        ["pointlogid", "PointlogID MAA", "Missing_Track_Name/Album_Name", "Youtube_URL"]
    ]  # filter columns cần có ở sheet Recheck_notItunes
    df.insert(0, "CreatedAt", str(date.today()), allow_duplicates=True)
    df["Hyperlink"] = ""
    df.insert(4, "Artist_Album", str(date.today()), allow_duplicates=True)
    df["Album_UUID"] = ""

    for i in df["Youtube_URL"]:  # chỉnh lại format của link youtube_url
        if len(i) < 43:
            df["Hyperlink"].loc[df["Youtube_URL"] == i] = (
                "https://www.youtube.com/watch?v=" + i.strip()[-11:]
            )
        else:
            df["Hyperlink"].loc[df["Youtube_URL"] == i] = i.strip()[:43]

    create_column("Album_UUID", "PointlogID MAA", missing_albums_added, df)
    create_column("Album_UUID", "PointlogID MAA", missing_albums_existed, df)
    create_column("Artist_Album", "Album_UUID", artist_name_albums, df)

    df["VIBBIDI_album_link"] = "https://www.vibbidi.net/album?id=" + df["Album_UUID"]

    # filter những case đã check ở sheet Recheck_notItunes
    df_donechecking = (
        df_notItunes_ori[["PointlogsID", "Youtube_URL"]]
        .set_index("PointlogsID")["Youtube_URL"]
        .to_dict()
    )
    df["checked"] = ""
    To_uuid("checked", "pointlogid", df_donechecking, df).transform()
    df = df[df["checked"] == ""]

    report_invalid_ids(
        # "minchan_notitune",
        "Youtube collect_Recheck_notItunes",
        "https://docs.google.com/spreadsheets/d/1ZUzx1smeyIKD4PtQ-hhT1kbPSTGRdu8I8NG1uvzcWr4/edit#gid=433164688",
        df,
    ).create_and_update()
    df_row = len(df_contribute_notItunes) + 1
    set_with_dataframe(
        sheet_contribute_notItunes, df, row=df_row, col=2, include_column_header=False
    )

    send_message_slack(
        "missing songs not from itunes", len(df.index), cy_notItunes_extract
    ).msg_slack()
    send_message_slack(
        "missing songs not from itunes", len(df.index), cy_notItunes_extract
    ).send_to_slack()
    update_data_gsheet(str(date.today()), "CY", "notItunes_extract", len(df.index))

    print("\n" + Fore.LIGHTYELLOW_EX + "The file is done processing!" + Style.RESET_ALL)


# extract_report()

"--------------------------------END_Extract_Report--------------------------------"

"""
Sources: https://www.datacamp.com/community/tutorials/fuzzy-string-python
"""


def string_reformat(string: str):
    pat_redundant_chars = re.compile(r"[\s`~!@#$%^&*()\-_+={\}\[\]\\|:;<>,./?]+")
    pat_quotations = re.compile(r"[\"«»‘’‚‛“”„‟‹›❛❜❝❞❮❯〝〞〟＂⹂]+")

    str_remove_accent = unidecode(string).lower()  # remove accent

    str_remove_quatation = str_remove_accent.replace(
        "'", ""
    )  # remove single quotations
    str_quotation = pat_quotations.sub("", str_remove_quatation)
    str_reformat_result = pat_redundant_chars.sub(" ", str_quotation)

    return str_reformat_result


def get_token_set_ratio(str1: str, str2: str):
    string_reformat1 = string_reformat(str1)
    string_reformat2 = string_reformat(str2)
    token_set_ratio = fuzz.token_set_ratio(string_reformat1, string_reformat2)
    return token_set_ratio


class my_dictionary(dict):
    def __init__(self):
        self = dict()

    def add(self, key, value):
        self[key] = value


def extract_similarity():
    single_page_column = df_notItunes["VIBBIDI_single_link"]

    dict_full_shorturi = my_dictionary()
    short_uri_list = []
    for full_uri in single_page_column:
        if "https://www.vibbidi.net/single?id=" in full_uri:
            short_uri = full_uri.replace("https://www.vibbidi.net/single?id=", "")
        elif "https://www.vibbidi.net" in full_uri:
            short_uri = full_uri.replace("https://www.vibbidi.net", "")
        else:
            short_uri = ""
        short_uri = urllib.parse.unquote(short_uri)
        dict_full_shorturi.add(full_uri, short_uri)
        short_uri_list.append(short_uri)

    short_uri_tuple = str(short_uri_list).replace("[", "(").replace("]", ")")
    # print(valid_plid)

    uri_trackid = To_df(None, uri_to_trackid, None, short_uri_tuple).create_df()
    track_trackid = To_df(None, track_to_trackid, None, short_uri_tuple).create_df()
    uri_trackid.update(track_trackid)

    df_notItunes["short_uri"] = ""
    To_uuid(
        "short_uri", "VIBBIDI_single_link", dict_full_shorturi, df_notItunes
    ).transform()
    To_uuid("track_id", "short_uri", uri_trackid, df_notItunes).transform()

    create_column("track_title", "track_id", track_title, df_notItunes)
    create_column("track_artist", "track_id", track_artist, df_notItunes)
    create_column("youtube_artist", "PointlogsID", youtube_artist_query, df_notItunes)

    for i in df_notItunes.index.tolist():
        df_notItunes["similarity - tracktitle"].loc[i] = get_token_set_ratio(
            df_notItunes["track_title"].loc[i],
            df_notItunes["Missing_Track_Name"].loc[i],
        )
        compare_artistchannel = get_token_set_ratio(
            df_notItunes["track_artist"].loc[i], df_notItunes["youtube_artist"].loc[i]
        )
        compare_youtubetitle = get_token_set_ratio(
            df_notItunes["track_artist"].loc[i],
            df_notItunes["Missing_Track_Name"].loc[i],
        )
        if compare_artistchannel > compare_youtubetitle:
            df_notItunes["similarity - artistname"].loc[i] = str(compare_artistchannel)
        else:
            df_notItunes["similarity - artistname"].loc[i] = str(compare_youtubetitle)

    # df_notItunes.to_html('df_notItunes.html')
    df_notItunes_toinsert = df_notItunes[
        [
            "track_id",
            "track_title",
            "track_artist",
            "youtube_artist",
            "similarity - tracktitle",
            "similarity - artistname",
        ]
    ]
    start_column_insert = df_notItunes.columns.get_loc("track_id") + 1
    set_with_dataframe(
        sheet_contribute_notItunes,
        df_notItunes_toinsert,
        row=last_prevalid_row_ori + 1,
        col=start_column_insert,
        include_column_header=False,
    )
    print("\n" + Fore.LIGHTYELLOW_EX + "The file is done processing!" + Style.RESET_ALL)


"--------------------------------END_Check_Similarity--------------------------------"


class CustomBase(object):
    # Default __tablename__
    @declared_attr
    def __tablename__(self):
        return self.__name__.lower()


Base = declarative_base(cls=CustomBase)


class DataSourceFormatMaster(Base):
    __tablename__ = "DataSourceFormatMaster"
    format_id = sa.Column("FormatID", sa.String(32), primary_key=True)
    name = sa.Column("Name", sa.String(256), nullable=False)
    priority = sa.Column("Priority", sa.SmallInteger, nullable=False, default=9999)
    ext = sa.Column("Ext", MutableDict.as_mutable(sa.JSON))

    FORMAT_ID_MP4_FULL = "74BA994CF2B54C40946EA62C3979DDA3"
    FORMAT_ID_MP4_STATIC = "C78F687CB3BE4D90B30F49435317C3AC"
    FORMAT_ID_MP4_LIVE = "7F8B6CD82F28437888BD029EFDA1E57F"
    FORMAT_ID_MP4_COVER = "F5D2FE4A15FB405E988A7309FD3F9920"
    FORMAT_ID_MP4_FEATURE = "408EEAB1D3CF41F3941F62F97372184F"
    FORMAT_ID_MP4_REMIX = "BB423826E6FA4839BBB4DA718F483D18"
    FORMAT_ID_MP4_LYRIC = "3CF047F3B0F349B3A9A39CE7FDAB1DA6"
    FORMAT_ID_MP3_FULL = "1A67A5F1E0D84FB9B48234AE65086375"


def get_format_id_from_content_type(content_type: str):
    if content_type in ("OFFICIAL_MUSIC_VIDEO", "OFFICIAL_MUSIC_VIDEO_2"):
        return DataSourceFormatMaster.FORMAT_ID_MP4_FULL
    elif content_type == "STATIC_IMAGE_VIDEO":
        return DataSourceFormatMaster.FORMAT_ID_MP3_FULL
    elif content_type == "COVER_VIDEO":
        return DataSourceFormatMaster.FORMAT_ID_MP4_COVER
    elif content_type == "LIVE_VIDEO":
        return DataSourceFormatMaster.FORMAT_ID_MP4_LIVE
    elif content_type == "REMIX_VIDEO":
        return DataSourceFormatMaster.FORMAT_ID_MP4_REMIX
    elif content_type == "LYRIC_VIDEO":
        return DataSourceFormatMaster.FORMAT_ID_MP4_LYRIC
    else:
        return "Unknown"


class WhenExist:
    SKIP = "skip"
    REPLACE = "replace"
    KEEP_BOTH = "keep both"


def update_contribution(
    pointlogsid: str,
    content_type: str,
    track_id: str,
    concert_live_name: str,
    artist_name: str,
    year: str,
    pic: str,
    youtube_url: str,
    other_official_version: str,
):

    # format_id
    format_id = get_format_id_from_content_type(content_type=content_type)

    # when exist
    if content_type == "OFFICIAL_MUSIC_VIDEO_2":
        when_exists = WhenExist.KEEP_BOTH
    elif format_id in (
        DataSourceFormatMaster.FORMAT_ID_MP4_FULL,
        DataSourceFormatMaster.FORMAT_ID_MP3_FULL,
    ):
        when_exists = WhenExist.SKIP
    else:
        when_exists = WhenExist.KEEP_BOTH

    if content_type == "OFFICIAL_MUSIC_VIDEO_2":
        query = f"UPDATE pointlogs SET VerifiedInfo = JSON_SET(IFNULL(pointlogs.VerifiedInfo, JSON_OBJECT()), '$.PIC', '{pic}','$.when_exists', '{when_exists}', '$.youtube_url', '{youtube_url}', '$.data_source_format_id', '{format_id}', '$.other_official_version', '{other_official_version}'), TargetId = '{track_id}', Valid = 1  WHERE id = '{pointlogsid}';"
    elif content_type == "OFFICIAL_MUSIC_VIDEO":
        query = f"UPDATE pointlogs SET VerifiedInfo = JSON_SET(IFNULL(pointlogs.VerifiedInfo, JSON_OBJECT()), '$.PIC', '{pic}','$.when_exists', '{when_exists}', '$.youtube_url', '{youtube_url}', '$.data_source_format_id', '{format_id}'), TargetId = '{track_id}', Valid = 1  WHERE id = '{pointlogsid}';"
    elif content_type == "STATIC_IMAGE_VIDEO":
        query = f"UPDATE pointlogs SET VerifiedInfo = JSON_SET(IFNULL(pointlogs.VerifiedInfo, JSON_OBJECT()), '$.PIC', '{pic}','$.when_exists', '{when_exists}', '$.youtube_url', '{youtube_url}', '$.data_source_format_id', '{format_id}'), TargetId = '{track_id}', Valid = 1  WHERE id = '{pointlogsid}';"
    elif content_type == "LYRIC_VIDEO":
        query = f"UPDATE pointlogs SET VerifiedInfo = JSON_SET(IFNULL(pointlogs.VerifiedInfo, JSON_OBJECT()), '$.PIC', '{pic}','$.when_exists', '{when_exists}', '$.youtube_url', '{youtube_url}', '$.data_source_format_id', '{format_id}'), TargetId = '{track_id}', Valid = 1  WHERE id = '{pointlogsid}';"
    elif content_type == "REMIX_VIDEO":
        query = f"UPDATE pointlogs SET VerifiedInfo = JSON_SET(IFNULL(pointlogs.VerifiedInfo, JSON_OBJECT()), '$.PIC', '{pic}','$.when_exists', '{when_exists}', '$.youtube_url', '{youtube_url}', '$.data_source_format_id', '{format_id}', '$.remix_artist', '{artist_name}'), TargetId = '{track_id}', Valid = 1  WHERE id = '{pointlogsid}';"
    elif content_type == "COVER_VIDEO":
        query = f"UPDATE pointlogs SET VerifiedInfo = JSON_SET(IFNULL(pointlogs.VerifiedInfo, JSON_OBJECT()), '$.PIC', '{pic}','$.when_exists', '{when_exists}', '$.youtube_url', '{youtube_url}', '$.data_source_format_id', '{format_id}', '$.covered_artist_name', '{artist_name}'), TargetId = '{track_id}', Valid = 1  WHERE id = '{pointlogsid}';"
    elif content_type == "LIVE_VIDEO":
        query = f"UPDATE pointlogs SET VerifiedInfo = JSON_SET(IFNULL(pointlogs.VerifiedInfo, JSON_OBJECT()), '$.PIC', '{pic}','$.when_exists', '{when_exists}', '$.youtube_url', '{youtube_url}', '$.data_source_format_id', '{format_id}', '$.concert_live_name', '{concert_live_name}', '$.year', '{year}'), TargetId = '{track_id}', Valid = 1  WHERE id = '{pointlogsid}';"
    # elif content_type.contains('REJECT'):
    elif "REJECT" in content_type:
        query = f"UPDATE pointlogs SET  Valid = -2  WHERE id = '{pointlogsid}';"
    else:
        query = f"-- content_type not existed"
    return query


def update_d9():
    filter_df = df_notItunes
    filter_df["pre_valid"] = str(date.today())
    start_column_insert = filter_df.columns.get_loc("pre_valid") + 1
    set_with_dataframe(
        sheet_contribute_notItunes,
        filter_df[["pre_valid"]],
        row=last_prevalid_row_ori + 1,
        col=start_column_insert,
        include_column_header=False,
    )
    sheet_name = file_name
    PIC_taskdetail = f"{gsheet_name}_{sheet_name}_{str(date.today())}"
    filter_df["crawling_task"] = filter_df.apply(
        lambda x: update_contribution(
            content_type=x["content type"],
            track_id=x["track_id"],
            concert_live_name=x["live_concert_name_place"],
            artist_name=x["artist_name"],
            year=x["year"],
            pic=PIC_taskdetail,
            youtube_url=x["Hyperlink"],
            other_official_version=x["official_music_video_2"],
            pointlogsid=x["PointlogsID"],
        ),
        axis=1,
    )
    update_db(filter_df["crawling_task"])
    report_invalid_ids(
        # "minchan_notitune",
        "Youtube collect_Recheck_notItunes",
        "https://docs.google.com/spreadsheets/d/1ZUzx1smeyIKD4PtQ-hhT1kbPSTGRdu8I8NG1uvzcWr4/edit#gid=1732133691&fvid=1833119072",
        filter_df,
    ).create_and_update()


"--------------------------------END_Update_pointlogs--------------------------------"


def check_and_update():
    criteria = {
        "LIVE_VIDEO": "live_concert_name_place",
        "OFFICIAL_MUSIC_VIDEO_2": "official_music_video_2",
        "COVER_VIDEO": "artist_name",
        "REMIX_VIDEO": "artist_name",
        "LIVE_VIDEO": "track_id",
        "OFFICIAL_MUSIC_VIDEO_2": "track_id",
        "COVER_VIDEO": "track_id",
        "REMIX_VIDEO": "track_id",
        "OFFICIAL_MUSIC_VIDEO": "track_id",
        "STATIC_IMAGE_VIDEO": "track_id",
        "LYRIC_VIDEO": "track_id",
    }

    append_missing_df = pd.DataFrame()
    for content_type in criteria:
        df_value = df_notItunes[df_notItunes["content type"] == content_type][
            criteria.get(content_type)
        ]
        for value in df_value:
            if value.strip() == "":
                missing_df = df_notItunes[
                    (df_notItunes["content type"] == content_type)
                    & (df_notItunes[criteria.get(content_type)] == value)
                ][
                    [
                        "PointlogsID",
                        "content type",
                        "official_music_video_2",
                        "artist_name",
                        "year",
                        "live_concert_name_place",
                        "track_id",
                    ]
                ]
            else:
                missing_df = []
            append_missing_df = append_missing_df.append(missing_df)
    append_missing_df = append_missing_df.drop_duplicates(subset=None, keep="first")

    df_notItunes_recheck = df_notItunes[
        (
            (df_notItunes["similarity - tracktitle"] != "100")
            | (df_notItunes["similarity - artistname"] != "100")
        )
        & (df_notItunes["recheck"] != "ok")
    ]
    df_notItunes_recheck = df_notItunes_recheck[
        ["PointlogsID", "similarity - tracktitle", "similarity - artistname", "recheck"]
    ]

    if append_missing_df.empty and df_notItunes_recheck.empty:
        print(
            Fore.LIGHTGREEN_EX
            + "\ncontent type and similarity recheck is ok"
            + Style.RESET_ALL
        )
        print(Fore.LIGHTBLUE_EX + "\nNow updating pointlogs..." + Style.RESET_ALL)
        update_d9()
        print(Fore.LIGHTBLUE_EX + "\nNow notify slack..." + Style.RESET_ALL)
        send_message_slack(
            "missing songs not from itunes",
            len(df_notItunes.index),
            cy_notItunes_plupdate,
        ).msg_slack()
        send_message_slack(
            "missing songs not from itunes",
            len(df_notItunes.index),
            cy_notItunes_plupdate,
        ).send_to_slack()
        update_data_gsheet(
            str(date.today()), "CY", "notItunes_verifypl", len(df_notItunes.index)
        )
    else:
        print(
            Fore.LIGHTRED_EX
            + "\nmissing info from content type/track_id as below, please ignore if empty"
            + Style.RESET_ALL
        )
        print(append_missing_df)
        print(
            Fore.LIGHTRED_EX
            + "\nmissing similarity recheck as below, please ignore if empty"
            + Style.RESET_ALL
        )
        print(df_notItunes_recheck)
    print("\n" + Fore.LIGHTYELLOW_EX + "The file is done processing!" + Style.RESET_ALL)


"--------------------------------END_Check_and_Update--------------------------------"

# extract_report()
# extract_similarity()
# check_and_update()
