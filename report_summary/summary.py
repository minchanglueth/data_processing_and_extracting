from connect_confidential.connect_db_final import conn, cursor

# from connect_confidential.connect_db_stg import conn, cursor
from connect_confidential.connect_gspread import client_gspread
from update_ts_ta_nc.update_data_report import report_invalid_ids

from colorama import Fore, Style

import pandas as pd

from datetime import date

# from slack_report import send_message_slack

from raw_sql import check_if_subgenred, check_if_mp3crawled, check_if_mp4crawled
from update_data_report import get_gsheet_id_from_url, report_to_db, sheet

report_url = input(
    Fore.LIGHTCYAN_EX + "Please input the report url: " + Style.RESET_ALL
)

open_urls = client_gspread.open_by_url(report_url)
# sheet_report = open_urls.worksheet('top_ab_sg_cs')


def df_ori(sheet_name):
    sheet = open_urls.worksheet(sheet_name)
    data = sheet.get_all_values()
    df = pd.DataFrame(data, columns=[i for i in data[0]])
    df = df.loc[:, ~df.columns.duplicated()]  # remove duplicate column
    return df


def df_report_type(report_type, df):
    if report_type == "new_classic":
        df = df[
            [
                "Release_date",
                "Category",
                "Source",
                "Albumuuid",
                "AlbumTitle",
                "AlbumArtist",
                "AlbumURL",
                "TrackNum",
                "ArtistName",
                "ArtistUUID",
                "TrackTitle",
                "TrackId",
                "is_released",
                "need_subgenre",
            ]
        ]
    elif report_type == "top_album":
        df = df[
            [
                "Genre",
                "Rank",
                "AlbumTitle",
                "ItunesAlbumId",
                "Ituneslink",
                "AlbumUUID",
                "TrackNum",
                "Verification",
                "ArtistName",
                "ArtistUUID",
                "TrackTitle",
                "TrackId",
                "Already_existed",
            ]
        ]
    elif report_type == "top_single":
        df = df[
            [
                "Genre",
                "Rank",
                "album_uuid",
                "album_title",
                "album_artist",
                "album_url",
                "ArtistName",
                "ArtistUUID",
                "TrackTitle",
                "TrackId",
                "Already_existed",
            ]
        ]
    return df


def df_compare(sheet_name, report_type):
    df = df_ori(sheet_name)
    df_compare = df_report_type(report_type, df)
    return df_compare


def df_summary(sheet_name, video_type, column_url, report_type):
    df = df_ori(sheet_name)
    df_append = df_compare(sheet_name, report_type)

    df_append[[video_type, "url_to_add"]] = df[[video_type, "url_to_add"]]
    df_append = df_append.loc[df["Duplicate_Track"] == "FALSE"]

    if sheet_name == "MP_3":
        df_append["Spotify"] = df["Spotify"]
    # df_append = df_append[1:]
    df_append.insert(len(df_append.columns), column_url, None, True)
    df_append[column_url].fillna(
        df_append[video_type] + df_append["url_to_add"], inplace=True
    )  # đây là cách 1 của việc fill đầy cell trống
    return df_append
    # df_mp3_append[df_mp3_append['MP3_url'].isnull()] = df_mp3_append['MP3_link'] + df_mp3_append['url_to_add'] #đây là cách 2 của việc fill đầy cell trống


def create_df_dict(sheet_name, columnkey, columnvalue):
    df = df_ori(sheet_name)
    # df = df.loc[df[filter_column] == filter_value]
    df.drop_duplicates(subset=["TrackId"], keep="first", ignore_index=True)
    df_dict = dict(zip(df[columnkey], df[columnvalue]))
    return df_dict


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


def create_column(column_name, lookup_value, check_query, df):
    df[column_name] = ""
    df_list = To_df(lookup_value, check_query, df).list()
    To_uuid(column_name, lookup_value, df_list, df).transform()


def create_video_type(
    sheet_name,
    ori_link,
    all_link,
    column_videotype,
    link_crawled,
    query,
    video_type,
    report_type,
):
    df_video = df_summary(sheet_name, ori_link, all_link, report_type)
    create_column(link_crawled, "TrackId", query, df_video)
    df_video[column_videotype] = ""
    df_video[column_videotype].loc[df_video[link_crawled] != ""] = video_type
    return df_video


def update_report(
    df,
    ok_youtube,
    missing_youtube,
    ok_spotify,
    missing_spotify,
    error_sheet,
    report_type,
):
    data1 = sheet.get_all_values()
    df_report = pd.DataFrame(data1, columns=[i for i in data1[0]])
    if df.empty:
        report_to_db(
            str(date.today()),
            ok_youtube,
            ok_spotify,
            report_type,
            get_gsheet_id_from_url(report_url),
            df_report,
        ).update_gsh_db()
    else:
        report_to_db(
            str(date.today()),
            missing_youtube,
            missing_spotify,
            report_type,
            get_gsheet_id_from_url(report_url),
            df_report,
        ).update_gsh_db()
        report_invalid_ids(error_sheet, report_url, df).create_and_update()


def create_summary(report_type):
    df = (
        pd.concat([df_compare("MP_3", report_type), df_compare("MP_4", report_type)])
        .drop_duplicates(keep=False)
        .reset_index()
    )
    if (
        df_compare("MP_3", report_type).equals(df_compare("MP_4", report_type)) == True
        or len(df) == 0
    ):
        try:
            df_mp4 = create_video_type(
                "MP_4",
                "MP4link",
                "MP4_url",
                "Type_mp4",
                "MP4_crawled",
                check_if_mp4crawled,
                "MP4 full",
                report_type,
            )
            df_mp3 = create_video_type(
                "MP_3",
                "MP3link",
                "MP3_url",
                "Type_mp3",
                "MP3_crawled",
                check_if_mp3crawled,
                "MP3 full",
                report_type,
            )

            # Find tracks missing spotify on sheet MP3 and create report sheet
            df_mp3_missing_spotify = df_mp3.loc[
                (df_mp3["MP3link"] == "")
                & (df_mp3["url_to_add"] == "")
                & (~df_mp3["Spotify"].isin(["nf", "not found", "not_found"]))
                & (df_mp3["MP3_crawled"] == "")
            ]
            if report_type == "new_classic":
                df_mp3_missing_spotify_filtered = df_mp3_missing_spotify.loc[
                    (df_mp3["is_released"] == "TRUE")
                ]
            else:
                df_mp3_missing_spotify_filtered = df_mp3_missing_spotify.loc[
                    (df_mp3["Already_existed"] == "null")
                ]
            df_mp3_missing_spotify = df_report_type(
                report_type, df_mp3_missing_spotify_filtered
            )
            df_mp3_missing_spotify[
                ["Spotify", "MP3_crawled"]
            ] = df_mp3_missing_spotify_filtered[["Spotify", "MP3_crawled"]]
            update_report(
                df_mp3_missing_spotify,
                None,
                None,
                "ok",
                len(df_mp3_missing_spotify["TrackId"]),
                "missing_spotify",
                report_type,
            )

            # Filter those columns on MP3 sheet for later merge
            df_mp3_tomerge = df_mp3[
                [
                    "TrackId",
                    "MP3link",
                    "url_to_add",
                    "Spotify",
                    "MP3_url",
                    "MP3_crawled",
                    "Type_mp3",
                ]
            ]

            # Append 2 sheet MP4 và MP3
            df_merge = df_mp4.merge(
                df_mp3_tomerge, on="TrackId", how="inner"
            )  # join 2 bảng với nhau

            # Ghép dataframe của 2 lần lọc khác nhau
            df_youtube_mp4 = df_merge.loc[
                (df_merge["MP4_url"] != "") & (df_merge["MP4_crawled"] == "")
            ]
            df_youtube_mp3 = df_merge.loc[
                (df_merge["MP3_url"] != "") & (df_merge["MP3_crawled"] == "")
            ]
            df_missing_youtube = df_youtube_mp4.append(
                df_youtube_mp3, ignore_index=True
            )
            update_report(
                df_missing_youtube,
                "ok",
                len(df_missing_youtube["TrackId"]),
                None,
                None,
                "missing_youtube",
                report_type,
            )

            if report_type == "new_classic":
                df_merge_filtered = df_merge.loc[(df_merge["is_released"] == "TRUE")]
            else:
                df_merge_filtered = df_merge.loc[
                    (df_merge["Already_existed"] == "null")
                ]

            # Tạo column 'Type' và fill giá trị Type theo thứ tự MP4 -> MP3 full
            df_merge_filtered["Type"] = ""
            df_merge_filtered.loc[
                df_merge_filtered["Type"] == "", "Type"
            ] = df_merge_filtered["Type_mp4"]
            df_merge_filtered.loc[
                df_merge_filtered["Type"] == "", "Type"
            ] = df_merge_filtered["Type_mp3"]

            # Tạo column 'SourceURI' và fill giá trị SourceURI theo thứ tự MP4_crawled -> MP3_crawled
            df_merge_filtered["SourceURI"] = ""
            df_merge_filtered.loc[
                df_merge_filtered["SourceURI"] == "", "SourceURI"
            ] = df_merge_filtered["MP4_crawled"]
            df_merge_filtered.loc[
                df_merge_filtered["SourceURI"] == "", "SourceURI"
            ] = df_merge_filtered["MP3_crawled"]

            # Rút gọn df_summary và filter những track có MP4/MP3
            df_summary = df_report_type(report_type, df_merge_filtered)
            df_summary[["SourceURI", "Type"]] = df_merge_filtered[["SourceURI", "Type"]]
            if report_type == "new_classic":
                create_column("if_subgenred", "TrackId", check_if_subgenred, df_summary)
            df_summary = df_summary.loc[df_summary["Type"] != ""]
            report_invalid_ids("Summary", report_url, df_summary).create_and_update()

            # Tạo subgenre_update
            if report_type == "new_classic":
                df_summary["link"] = "https://www.vibbidi.net/single?id="
                df_summary["Vibbidi_link"] = df_summary["link"] + df_summary["TrackId"]
                df_subgenred = df_summary.loc[
                    (df_summary["need_subgenre"] == "TRUE")
                    & (df_summary["if_subgenred"] == "")
                ].drop(["is_released", "need_subgenre", "if_subgenred", "link"], axis=1)
                df_subgenred.insert(12, "Sub Genre 1", None, True)
                df_subgenred.insert(13, "Sub Genre 2", None, True)
                report_invalid_ids(
                    "Subgenre_Update", report_url, df_subgenred
                ).create_and_update()

            print(
                "\n"
                + Fore.LIGHTYELLOW_EX
                + "The file is done processing!"
                + Style.RESET_ALL
            )

        except ValueError:
            print(
                "\n"
                + Fore.LIGHTRED_EX
                + "report_url NOT MATCH with any existing ones from Data_report, please recheck !"
                + Style.RESET_ALL
            )
            print(
                "\n"
                + Fore.LIGHTYELLOW_EX
                + "The file is done processing!"
                + Style.RESET_ALL
            )
    else:
        print(
            "\n"
            + Fore.LIGHTRED_EX
            + "Sheet MP_3 and Sheet MP_4 are not alike, please recheck!"
        )
        print(
            "Differences are printed out in sheet `comparesheet_dif`, index follows order: sheet MP_3 -> sheet MP_4"
            + Style.RESET_ALL
        )
        # df = pd.concat([df_compare('MP_3'),df_compare('MP_4')]).drop_duplicates(keep=False).reset_index()
        report_invalid_ids("comparesheet_dif", report_url, df).create_and_update()
        print(
            "\n"
            + Fore.LIGHTYELLOW_EX
            + "The file is done processing!"
            + Style.RESET_ALL
        )

        ##Cách 1: in ra dataframe gồm những giá trị khác nhau
        # df = pd.concat([df_compare('MP_3'), df_compare('MP_4')])
        # df = df.reset_index(drop=True)
        # df_gpby = df.groupby(list(df.columns))
        # idx = [x[0] for x in df_gpby.groups.values() if len(x) == 1]
        # print(idx)
        # df.reindex(idx)
        # print(df.reindex(idx))

        ##Cách 2: chỉ in ra những giá trị khác nhau thôi, nhưng dataframe phải giống shape(số line), giống columns.title và giống index
        # df = df_compare('MP_3').compare(df_compare('MP_4'),align_axis=0)
        # print(df)


def run_summary():
    print("Report types: top_album, top_single, new_classic")
    report_type = input(
        Fore.LIGHTCYAN_EX + "Please input the report type: " + Style.RESET_ALL
    )
    create_summary(report_type)
