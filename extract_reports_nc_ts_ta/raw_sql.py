from colorama import Fore, Style

crawler_report = """WITH t1 AS ( SELECT DISTINCT cl.taskdetail ->> '$.parent_task_id' AS prid 
		FROM v4.crawlingtasks cl WHERE cl.ActionId = '{}' 
		AND cl.CreatedAt > '{}' ) SELECT
		cl.actionid,
		cl.id,
		cl.taskdetail ->> '$.parent_task_id' AS id_parent_task,
		cl.UpdatedAt,
		cl.CreatedAt,
		cl.TaskDetail ->> '$.rank' AS ranking,
		cl.TaskDetail ->> '$.genre' AS genre,
		cl.`Status` 
		FROM
			v4.crawlingtasks cl
			JOIN t1 ON t1.prid = cl.taskdetail ->> '$.parent_task_id' 
		ORDER BY
			cl.Ext ->> '$.start_time' ASC,
			cl.Ext ->> '$.finish_time' DESC"""

report_top_album_mp3 = """SELECT
			ta.CreatedAt,
			ta.Genre,
			ta.Rank,
			ta.AlbumTitle,
			ta.ItunesAlbumId,
			ta.Ext ->> '$.album_url' Ituneslink,
			ta.Ext ->> '$.album_uuid' AlbumUUID,
			ta.TrackNum,
			ta.Verification,
			ta.ArtistName,
			ta.ArtistUUID,
			ta.TrackTitle,
			ta.TrackId,
			ta.DataSourcesExisted ->> '$.MP3_FULL' AS MP3_dsid,
			ds.SourceURI AS MP3link,
			ds.Valid,
			ta.Ext ->> '$.is_album_duplicated' AS Duplicate_Album,
			ta.Ext ->> '$.is_track_duplicated' AS Duplicate_Track,
			ta.Ext ->> '$.need_checking_mp3' AS Checking_mp3,
			ta.Ext ->> '$.already_existed_in' AS Already_existed 
		FROM
			v4.reportautocrawler_top100albums ta
			LEFT JOIN v4.datasources ds ON ds.id = ta.DataSourcesExisted ->> '$.MP3_FULL' 
		WHERE
			ta.CreatedAt > '{}' 
		ORDER BY
			ta.Genre,
			ta.Rank ASC,
			ta.TrackNum ASC,
			ta.CreatedAt ASC"""

report_top_album_mp4 = """SELECT
			ta.CreatedAt,
			ta.Genre,
			ta.Rank,
			ta.AlbumTitle,
			ta.ItunesAlbumId,
			ta.Ext ->> '$.album_url' Ituneslink,
			ta.Ext ->> '$.album_uuid' AlbumUUID,
			ta.TrackNum,
			ta.Verification,
			ta.ArtistName,
			ta.ArtistUUID,
			ta.TrackTitle,
			ta.TrackId,
			ta.DataSourcesExisted ->> '$.MP4_FULL' AS MP4_dsid,
			ds.SourceURI AS MP4link,
			ds.Valid,
			ta.Ext ->> '$.is_album_duplicated' AS Duplicate_Album,
			ta.Ext ->> '$.is_track_duplicated' AS Duplicate_Track,
			ta.Ext ->> '$.need_checking_mp4' AS Checking_mp4,
			ta.Ext ->> '$.already_existed_in' AS Already_existed 
		FROM
			v4.reportautocrawler_top100albums ta
			LEFT JOIN v4.datasources ds ON ds.id = ta.DataSourcesExisted ->> '$.MP4_FULL' 
		WHERE
			ta.CreatedAt > '{}' 
		ORDER BY
			ta.Genre,
			ta.Rank ASC,
			ta.TrackNum ASC,
			ta.CreatedAt ASC"""

crawler_report_s11 = """WITH t1 AS 
		( SELECT * FROM v4.crawlingtasks cl 
		WHERE cl.ActionId = '9C8473C36E57472281A1C7936108FC06' 
		AND cl.Priority = 530 AND cl.CreatedAt >= '{}' ) 
		SELECT
		t1.id AS ID_06,
		t1.CreatedAt AS CreatedAt_06,
		t1.UpdatedAt AS UpdatedAt_06,
		t1.TaskDetail AS TaskDetail_06,
		t1.Priority AS Priority_06,
		t1.ext AS EXT_06,
		t1.Priority AS Priority_E5,
		t1.`Status` AS Status_06,
		cl2.`Status` AS Status_E5,
		cl2.id AS ID_E5,
		t1.ext ->> '$.itunes_track_task_id' AS 06_to_E5
		FROM
			t1
			LEFT JOIN v4.crawlingtasks cl2 ON t1.ext ->> '$.itunes_track_task_id' = cl2.id"""

report_s11 = """SELECT DISTINCT
			nc.AlbumInfo ->> '$.release_date' Release_date,
			Category,
			Source,
			nc.AlbumInfo ->> '$.album_title' AS AlbumTitle,
			nc.AlbumInfo ->> '$.album_artist' AS AlbumArtist,
			nc.AlbumInfo ->> '$.album_url' AS AlbumURL
		FROM
			v4.reportautocrawler_newclassic nc
			LEFT JOIN v4.datasources ds ON ds.id = nc.DataSourcesExisted ->> '$.MP4_FULL' 
		WHERE
			nc.CreatedAt >= '{}' 
			AND nc.TrackId IS NULL 
		ORDER BY
			CONCAT( nc.CreatedAt, nc.AlbumInfo ->> '$.track_index' ) ASC"""

report_top_single_mp3 = """SELECT
			ts.CreatedAt,
			ts.Genre,
			ts.Rank,
			ts.Ext->>'$.album_uuid' AS album_uuid,
			ts.Ext->>'$.album_title' AS album_title,
			ts.Ext->>'$.album_artist' AS album_artist,
			ts.Ext->>'$.album_url' AS album_url,
			ts.ArtistName,
			ts.ArtistUUID,
			ts.TrackTitle,
			ts.TrackId,
			ts.DataSourcesExisted->>'$.MP3_FULL' AS MP3_dsid,
			ds.SourceURI AS MP3link, 
			ts.Ext->>'$.is_track_duplicated' AS Duplicate_Track,
			ts.Ext->>'$.need_checking_mp3' AS Checking_mp3,
			ts.Ext->>'$.already_existed_in' AS Already_existed
		FROM
			v4.reportautocrawler_topsingle ts
		LEFT JOIN
		v4.datasources ds
		ON
		ds.id = ts.DataSourcesExisted ->> '$.MP3_FULL'
		WHERE
			ts.CreatedAt >= '{}'
		ORDER BY
			ts.Genre, ts.Rank ASC, ts.CreatedAt ASC
		"""
report_top_single_mp4 = """SELECT
			ts.CreatedAt,
			ts.Genre,
			ts.Rank,
			ts.Ext->>'$.album_uuid' AS album_uuid,
			ts.Ext->>'$.album_title' AS album_title,
			ts.Ext->>'$.album_artist' AS album_artist,
			ts.Ext->>'$.album_url' AS album_url,
			ts.ArtistName,
			ts.ArtistUUID,
			ts.TrackTitle,
			ts.TrackId,
			ts.DataSourcesExisted->>'$.MP4_FULL' AS MP4_dsid,
			ds.SourceURI AS MP4link, 
			ts.Ext->>'$.is_track_duplicated' AS Duplicate_Track,
			ts.Ext->>'$.need_checking_mp4' AS Checking_mp4,
			ts.Ext->>'$.already_existed_in' AS Already_existed
		FROM
			v4.reportautocrawler_topsingle ts
		LEFT JOIN
		v4.datasources ds
		ON
		ds.id = ts.DataSourcesExisted ->> '$.MP4_FULL'
		WHERE
			ts.CreatedAt >= '{}'
		ORDER BY
			ts.Genre, ts.Rank ASC, ts.CreatedAt ASC"""

report_new_classic_mp4 = """SELECT
			nc.CreatedAt,
			nc.AlbumInfo->>'$.release_date' Release_date,
			Category,
			Source,
			nc.Ext->>'$.album_uuid' AS Albumuuid,
			nc.AlbumInfo->>'$.album_title' AS AlbumTitle,
			nc.AlbumInfo->>'$.album_artist' AS AlbumArtist,
			nc.AlbumInfo->>'$.album_url' AS AlbumURL,
			nc.AlbumInfo->>'$.track_index' AS TrackNum,
			nc.ArtistName,
			ArtistUUID,
			TrackTitle,
			nc.TrackId,
			nc.Ext ->> '$.track_id' Subgerned,
			nc.DataSourcesExisted ->> '$.MP4_FULL' AS MP4_dsid,
			ds.SourceURI AS MP4link, 
			nc.Ext->>'$.in_top_single' AS in_top_single,
			nc.Ext->>'$.is_released' AS is_released,
			nc.Ext->>'$.need_subgenre' AS need_subgenre,
			nc.Ext->>'$.is_track_duplicated' AS Duplicate_Track,
			nc.Ext->>'$.need_checking_mp4' AS Checking_mp4,
			'' as Verified,
			'' as url_to_add,
			'' as Memo,
			'' as Length
		FROM
			v4.reportautocrawler_newclassic nc
		LEFT JOIN
		v4.datasources ds
		ON
		ds.id = nc.DataSourcesExisted ->> '$.MP4_FULL'
		WHERE
			nc.CreatedAt >= '{}'
			and nc.TrackId is not null
		ORDER BY
		nc.CreatedAt asc,
		nc.Ext->>'$.album_uuid',
		CAST(nc.AlbumInfo->>'$.track_index' as UNSIGNED) ASC"""

report_new_classic_mp3 = """SELECT
			nc.CreatedAt,
			nc.AlbumInfo->>'$.release_date' Release_date,
			Category,
			Source,
			nc.Ext->>'$.album_uuid' AS Albumuuid,
			nc.AlbumInfo->>'$.album_title' AS AlbumTitle,
			nc.AlbumInfo->>'$.album_artist' AS AlbumArtist,
			nc.AlbumInfo->>'$.album_url' AS AlbumURL,
			nc.AlbumInfo->>'$.track_index' AS TrackNum,
			nc.ArtistName,
			ArtistUUID,
			TrackTitle,
			nc.TrackId,
			nc.Ext ->> '$.track_id' Subgerned,
			nc.DataSourcesExisted ->> '$.MP3_FULL' AS MP3_dsid,
			ds.SourceURI AS MP3link,
			nc.Ext->>'$.in_top_single' AS in_top_single,
			nc.Ext->>'$.is_released' AS is_released,
			nc.Ext->>'$.need_subgenre' AS need_subgenre,
			nc.Ext->>'$.is_track_duplicated' AS Duplicate_Track,
			nc.Ext->>'$.need_checking_mp3' AS Checking_mp3,
			'' as url_to_add,
			'' as Type,
			'' as Memo,
			'' as Length
		FROM
			v4.reportautocrawler_newclassic nc
		LEFT JOIN
		v4.datasources ds
		ON
		ds.id = nc.DataSourcesExisted ->> '$.MP3_FULL'
		WHERE
			nc.CreatedAt >= '{}'
			and nc.TrackId is not null
		ORDER BY
		nc.CreatedAt asc,
		nc.Ext->>'$.album_uuid',
		CAST(nc.AlbumInfo->>'$.track_index' as UNSIGNED) ASC"""

# report_topsingle_mp4
# import calendar
# from datetime import date, timedelta

# list1 = {'Sunday':0,'Monday':1,'Tuesday':2,'Wednesday':3,'Thursday':4,'Friday':5,'Saturday':6}
# myweekday = calendar.day_name[date.today().weekday()]
# date1 = date.today() - timedelta(list1[myweekday])
# print(date1)

rerun_crawlingtask = """UPDATE v4.crawlingtasks set `Status` = NULL where actionid !='ZL1MORSOVFESAQQNYFM7OHEHPI4XTGL5' and id = '{}';"""
# rerun_crawlingtask = """UPDATE v4.crawlingtasks set `Status` = 'complete' where actionid !='ZL1MORSOVFESAQQNYFM7OHEHPI4XTGL5' and id = '{}';"""


class crawlingtask:
    def __init__(self, id_list, cursor, conn, duration):
        self.id_list = id_list
        self.cursor = cursor
        self.conn = conn
        self.duration = duration

    def check_query_itunes(self):
        for id in self.id_list:
            sql_complete = rerun_crawlingtask.format(id)
            print(Fore.LIGHTCYAN_EX + sql_complete + Style.RESET_ALL)

    def execute_query_itunes(self):
        while True:
            check_query = input(
                Fore.LIGHTYELLOW_EX
                + "Please check query format and ignore if empty"
                + Style.RESET_ALL
                + "\nEnter EXIT if queries have problems, OK to continue: "
            )
            if check_query == "OK":
                for id in self.id_list:
                    sql_complete = rerun_crawlingtask.format(id)
                    self.cursor.execute(sql_complete)
                    self.conn.commit()
                print(
                    "\n"
                    + Fore.LIGHTYELLOW_EX
                    + "Finish updating fault crawlingtasks, please wait "
                    + self.duration
                    + " to recheck!"
                )
                print(
                    "If the result returns NOT COMPLETE please inform Jay and Cc Joy & Minchan"
                    + Style.RESET_ALL
                )
                break
            elif check_query == "EXIT":
                break
            else:
                print("Please re-enter option!")
