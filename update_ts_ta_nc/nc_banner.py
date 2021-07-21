from connect_confidential.connect_db_final import conn, cursor
# from connect_confidential.connect_db_stg import conn, cursor

from update_data_report import report_invalid_ids, report_to_db, if_exist_report, sheet

import pandas as pd
import calendar
from datetime import date, timedelta, datetime
from colorama import Fore, Style

genre_query = "select DISTINCT UUID from v4.genres where ParentId is not null"

image_genre = """SELECT
	cn.TrackId,
	cn.GenreId,
	cn.Category,
	cn.ReleasedAt,
	tr.imageurl,
	ats.ArtistId 
FROM
	v4.classicnew cn
	JOIN v4.tracks tr ON tr.id = cn.trackid
	JOIN v4.artist_track ats ON ats.TrackId = tr.Id 
WHERE
	tr.valid = 1 
	AND cn.Category = '{}' 
	AND cn.GenreId = '{}' 
ORDER BY
	cn.ReleasedAt DESC 
	LIMIT 1"""

select_banner = """SELECT
	cn.TrackId,
	cn.GenreId,
	cn.Category,
	cn.ReleasedAt,
	tr.id,
	tr.imageurl,
	ats.ArtistId 
FROM
	v4.classicnew cn
	JOIN v4.tracks tr ON tr.id = cn.trackid
	JOIN v4.artist_track ats ON ats.TrackId = tr.Id 
WHERE
	tr.valid = 1 
	AND cn.Category = '{}' 
	AND cn.GenreId = '{}' 
	and tr.ImageURL not in ({})
	and ats.ArtistId not in ({})
ORDER BY
	cn.ReleasedAt DESC 
	LIMIT 1"""

update_banner = """insert ignore into v4.classicnew (trackID, GenreId, Category, `ReleasedAt`) VALUES ("{}","{}","{}",NOW())"""

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

if myweekday != "Monday" and myweekday != "Tuesday":
    report_date = daily_nc_date


def create_banner_list(nc_type):
    cursor.execute(genre_query)
    result = cursor.fetchall()
    sql_genreid = pd.DataFrame(result)
    list_genreid = sql_genreid[0].to_list()
    df_banner = pd.DataFrame(
        columns=["TrackId", "GenreId", "Category", "ReleasedAt", "imageurl", "ArtistId"]
    )
    dic_banner = {}
    for i in list_genreid:
        query = image_genre.format(nc_type, i)  # đặt function
        cursor.execute(query)
        result = cursor.fetchall()
        sql = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        dic_banner["TrackId"] = str(sql["TrackId"].values).strip("[]").strip("'")
        dic_banner["GenreId"] = str(sql["GenreId"].values).strip("[]").strip("'")
        dic_banner["Category"] = str(sql["Category"].values).strip("[]").strip("'")
        dic_banner["ReleasedAt"] = str(sql["ReleasedAt"].values).strip("[]").strip("'")
        dic_banner["imageurl"] = str(sql["imageurl"].values).strip("[]").strip("'")
        dic_banner["ArtistId"] = str(sql["ArtistId"].values).strip("[]").strip("'")
        df_banner = df_banner.append(dic_banner, ignore_index=True)
    return df_banner


def create_image_list(df_banner):
    list_imageurl = ",".join("'" + str(x) for x in df_banner["imageurl"] + "'")
    return list_imageurl


def create_artist_list(df_banner):
    list_artistid = ",".join("'" + str(x) for x in df_banner["ArtistId"] + "'")
    return list_artistid


def find_remove_banner(
    df_banner, gsheet_name, nc_type, nc_dupbanner_column, report_url
):
    # report_invalid_ids(gsheet_name,'https://docs.google.com/spreadsheets/d/1TGNFci-sn9vyCKD4zU2My-R20EzFDtbLMZivohfbim0/edit#gid=5410219&fvid=575774724',df_banner).create_and_update()
    report_invalid_ids(gsheet_name, report_url, df_banner).create_and_update()
    banner_duplicated = df_banner[df_banner.duplicated(subset=["imageurl"])]
    if banner_duplicated.empty:
        if_exist_report(report_date, "new_classic").create_data()
        data1 = sheet.get_all_values()
        df_report = pd.DataFrame(data1, columns=[i for i in data1[0]])
        report_to_db(
            None,
            None,
            None,
            None,
            "new_classic",
            report_date,
            "not_found",
            nc_dupbanner_column,
            df_report,
        ).update_gsh_db()
        print("No duplicated banners found!")
    else:
        print(
            "\n1. Genres whose banners should be replaced as below:\n",
            banner_duplicated,
        )
        for i in banner_duplicated["GenreId"]:
            # print(i) #check genreid
            create_banner_list(nc_type)
            df_banner = create_banner_list(nc_type)
            create_image_list(df_banner)
            list_imageurl = create_image_list(df_banner)
            create_artist_list(df_banner)
            list_artistid = create_artist_list(df_banner)
            query = select_banner.format(nc_type, i, list_imageurl, list_artistid)
            # print(query) #check câu query đúng ko
            cursor.execute(query)
            result = cursor.fetchall()
            sql = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
            # print(sql)
            trackid = str(sql["TrackId"].values).strip("[]")
            genreid = str(sql["GenreId"].values).strip("[]")
            category = str(sql["Category"].values).strip("[]")
            update_query = (
                "UPDATE v4.classicnew set ReleasedAt = NOW() where TrackId = "
                + trackid
                + " and GenreId = "
                + genreid
                + " and Category = "
                + category
            )
            print("\n2. Updating banner of genreid:", i, "\n", update_query)
            cursor.execute(update_query)
            conn.commit()
            create_banner_list(nc_type)  # đang phải xem lại thứ tự
            df_banner = create_banner_list(nc_type)
            # report_invalid_ids(gsheet_name,'https://docs.google.com/spreadsheets/d/1TGNFci-sn9vyCKD4zU2My-R20EzFDtbLMZivohfbim0/edit#gid=5410219&fvid=575774724',df_banner).create_and_update()
            report_invalid_ids(gsheet_name, report_url, df_banner).create_and_update()
            banner_duplicated = df_banner[df_banner.duplicated(subset=["imageurl"])]
            if banner_duplicated.empty:
                if_exist_report(report_date, "new_classic").create_data()
                data1 = sheet.get_all_values()
                df_report = pd.DataFrame(data1, columns=[i for i in data1[0]])
                report_to_db(
                    None,
                    None,
                    None,
                    None,
                    "new_classic",
                    report_date,
                    "removed",
                    nc_dupbanner_column,
                    df_report,
                ).update_gsh_db()


def banner_function_combine(nc_type, gsheet_name, nc_dupbanner_column, report_url):
    create_banner_list(nc_type)
    df_banner = create_banner_list(nc_type)
    create_image_list(df_banner)
    create_artist_list(df_banner)
    find_remove_banner(df_banner, gsheet_name, nc_type, nc_dupbanner_column, report_url)


def nc_banner_removed(report_url):
    print(
        Fore.LIGHTMAGENTA_EX
        + "\nA/ Check and update banners of New this week:"
        + Style.RESET_ALL
    )
    banner_function_combine("new", "banner_new", "new_dupbanner", report_url)
    print(
        Fore.LIGHTMAGENTA_EX
        + "\nB/ Check and update banners of Streams:"
        + Style.RESET_ALL
    )
    banner_function_combine(
        "classic", "banner_classic", "classic_dupbanner", report_url
    )
    print("\n" + Fore.LIGHTYELLOW_EX + "The file is done processing!" + Style.RESET_ALL)


# nc_banner_removed()
