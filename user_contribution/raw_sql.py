missing_albums_existed = """SELECT
            pl.id AS pl_id,
            mid( cl.ext, 49, 32 ) AS album_uuid 
        FROM
            pointlogs pl
            JOIN crawlingtasks cl ON cl.id = pl.ext ->> '$.crawler_id' 
        WHERE
            pl.valid = 1 
            AND cl.`Status` = 'incomplete'
            AND pl.id IN {}
        """
missing_albums_added = """SELECT
            pl.id AS pl_id,
            cl.Ext ->> '$.album_uuid' AS album_uuid
        FROM
            pointlogs pl
            JOIN crawlingtasks cl ON cl.id = pl.ext ->> '$.crawler_id' 
        WHERE
            pl.valid = 1 
            AND cl.`Status` = 'complete' 
            AND pl.id IN {} 
        ORDER BY
            pl.CreatedAt DESC"""

artist_name_albums = """SELECT DISTINCT
            albums.UUID,
            artists.`Name` 
        FROM
            albums
            JOIN artist_album atb ON atb.AlbumId = albums.id
            JOIN artists ON artists.id = atb.ArtistId 
        WHERE
            albums.Valid = 1 
            AND albums.UUID IN {}"""

uri_to_trackid = """SELECT
	uri.URI,
	EntityId 
FROM
	v4.urimapper uri
	JOIN v4.tracks tr ON uri.EntityId = tr.id 
WHERE
	tr.Valid = 1 
	AND uri.URI IN {}"""

track_to_trackid = """SELECT
	tr.id,
	tr.id 
FROM
	v4.tracks tr 
WHERE
	tr.Valid = 1 
	AND tr.id IN {}"""

track_title = "select id, Title from v4.tracks where id in {}"
track_artist = "select id, Artist from v4.tracks where id in {}"

youtube_artist_query = """SELECT
	pl.id,
	ds.Info ->> '$.source.uploader' AS youtube_artist 
FROM
	v4.pointlogs pl
	JOIN v4.crawlingtasks cl ON cl.id = pl.Ext ->> '$.crawler_id'
	JOIN v4.datasources ds ON ds.id = cl.Ext ->> '$.data_source_id' 
	AND ds.valid = 1 
WHERE
	pl.id IN {}"""

ma_image_status = """SELECT
	pl.id,
	'incomplete' AS `Status` 
FROM
	pointlogs pl
	JOIN crawlingtasks cl ON cl.id = pl.Ext ->> '$.crawler_id[0]' 
WHERE
	cl.ActionId = 'B06AEE0F622D47F8B6FEF07DC2EABEAE' 
	AND pl.ActionType = 'MA' 
	AND pl.valid = 1 
	AND ( cl.Ext ->> '$.message' = 'Cannot download or upload artist image' OR cl.`Status` = 'incomplete' ) 
	AND pl.id IN {} UNION
SELECT
	pl.id,
	cl.`Status` 
FROM
	pointlogs pl
	JOIN crawlingtasks cl ON cl.id = pl.Ext ->> '$.crawler_id[0]' 
WHERE
	cl.ActionId = 'B06AEE0F622D47F8B6FEF07DC2EABEAE' 
	AND pl.ActionType = 'MA' 
	AND pl.valid = 1 
	AND ( cl.Ext ->> '$.message' != 'Cannot download or upload artist image' OR cl.Ext ->> '$.message' IS NULL ) 
	AND pl.id IN {}"""

ma_album_status = """SELECT
	pl.id,
	cl.`Status`
FROM
	pointlogs pl
	JOIN crawlingtasks cl ON cl.id = pl.Ext ->> '$.crawler_id[1]' 
WHERE
	cl.ActionId = '3FFA9CB0E221416288ACFE96B5810BD2' 
	AND pl.ActionType = 'MA' 
	AND pl.valid = 1 
	AND pl.id IN {}"""

maa_status = """SELECT
	pl.id,
	cl.`Status`
FROM
	pointlogs pl
	JOIN crawlingtasks cl ON cl.id = pl.Ext ->> '$.crawler_id' 
WHERE
	cl.ActionId = '9C8473C36E57472281A1C7936108FC06' 
	AND pl.ActionType = 'MAA' 
	AND pl.valid = 1 
	AND pl.id IN {}"""

cy_notItunes_D9_id = """SELECT
	pl.id AS PointlogsID,
	cl.id AS D9_id
FROM
	pointlogs pl
	LEFT JOIN crawlingtasks cl ON cl.id = pl.Ext ->> '$.crawler_id' 
WHERE
	cl.ActionId = '1BB6B994C60F4216998282F92D27EDD9' 
	AND pl.ActionType = 'CY' 
	AND pl.valid = 1 
	AND pl.id IN {}"""

cy_notItunes_D9_status = """SELECT
	pl.id AS PointlogsID,
	cl.`Status` AS D9_status
FROM
	pointlogs pl
	LEFT JOIN crawlingtasks cl ON cl.id = pl.Ext ->> '$.crawler_id' 
WHERE
	cl.ActionId = '1BB6B994C60F4216998282F92D27EDD9' 
	AND pl.ActionType = 'CY' 
	AND pl.valid = 1 
	AND pl.id IN {}"""

maa_notItunes_status = """SELECT
	pl.id AS PointlogID_MAA,
	cl.`Status` AS notItunes_status 
FROM
	pointlogs pl
	LEFT JOIN crawlingtasks cl ON cl.id = pl.Ext ->> '$.crawler_id' 
WHERE
	cl.ActionId = '348637F531454DD6B1CC108A77C94DB2' 
	AND pl.ActionType = 'MAA' 
	AND pl.Valid = 1 
	AND pl.id IN {}"""

ma_and_maa_mp3_mp4_checking = """
	(SELECT
	Trim( SUBSTRING_INDEX( albums.iTunesUrl, '/',- 1 ) ) AS ItunesID,
	artists.uuid `Artist_track_uuid`,
	-- artists.square_image_url as artist_image_url, 
	-- artists.info ->> '$.wiki_url' as artist_wiki_url,
	-- artists.info ->> '$.wiki.brief' as artist_wiki_content,
	albums.UUID AS `Album_uuid`,
	itunes_album_tracks_release.Artist `Artist Album`,
	albums.Title AS `Album Title`,
	albums.square_image_url,
	-- albums.info ->> '$.wiki_url' as album_wiki_url,
	-- albums.info ->> '$.wiki.brief' as album_wiki_content,
	itunes_album_tracks_release.iTunesUrl `iTunes album URL`,
	itunes_album_tracks_release.Seq `Track Number`,
	tracks.id `track_id`,
	itunes_album_tracks_release.TrackName AS `Song Title on Itunes`,
	itunes_album_tracks_release.TrackArtist AS `Artist Track on iTunes`,
	-- tracks.lyrics,
	-- datasources.SourceURI as 'MP4_link',
	-- d.SourceURI AS 'Mp3_link',
	{}
	-- trackcountlog.DataSourceCount ->> '$.live' as count_live,
	-- trackcountlog.DataSourceCount ->> '$.remix' as count_remix
	
	FROM
		albums
		LEFT JOIN itunes_album_tracks_release ON albums.uuid = itunes_album_tracks_release.AlbumUUID 
		AND itunes_album_tracks_release.valid = 1
		LEFT JOIN tracks ON tracks.Title = itunes_album_tracks_release.trackname 
		AND tracks.Artist = itunes_album_tracks_release.trackartist 
		AND tracks.Valid = 1
		LEFT JOIN artists ON Artists.`Name` = itunes_album_tracks_release.TrackArtist 
		AND artists.valid = 1
		LEFT JOIN datasources ON datasources.trackid = tracks.id 
		AND datasources.Valid > 0 
		AND datasources.formatid = '74BA994CF2B54C40946EA62C3979DDA3'
		LEFT JOIN datasources d ON d.trackid = tracks.id 
		AND d.Valid > 0 
		AND d.formatid = '1A67A5F1E0D84FB9B48234AE65086375'
		LEFT JOIN trackcountlog ON trackcountlog.TrackID = tracks.id 
	WHERE
		(
			albums.UUID IN (
			SELECT
				cl.Ext ->> '$.album_uuid' 
			FROM
				pointlogs pl
				JOIN crawlingtasks cl ON cl.id = pl.Ext ->> '$.crawler_id' 
			WHERE
				pl.id IN {} # input pointlogs của MAA vào
			) 
		) 
		AND albums.valid = 1 AND
		-- AND d.SourceURI IS NULL
		{}
	GROUP BY
		itunes_album_tracks_release.TrackName,
		itunes_album_tracks_release.TrackArtist 
	ORDER BY
		itunes_album_tracks_release.Artist,
		itunes_album_tracks_release.AlbumUUID,
		itunes_album_tracks_release.Seq ASC 
	) 
	UNION ALL
	(
	SELECT
	Trim( SUBSTRING_INDEX( albums.iTunesUrl, '/',- 1 ) ) AS ItunesID,
	artists.uuid `Artist_uuid`,
	-- artists.square_image_url as artist_image_url,
	-- artists.info ->> '$.wiki_url' as artist_wiki_url,
	-- artists.info ->> '$.wiki.brief' as artist_wiki_content,
	albums.UUID AS `Album_uuid`,
	itunes_album_tracks_release.Artist `Artist Album`,
	albums.Title AS `Album Title`,
	albums.square_image_url,
	-- albums.info ->> '$.wiki_url' as album_wiki_url,
	-- albums.info ->> '$.wiki.brief' as album_wiki_content,
	itunes_album_tracks_release.iTunesUrl `iTunes album URL`,
	itunes_album_tracks_release.Seq `Track Number`,
	tracks.id `track_id`,
	itunes_album_tracks_release.TrackName AS `Song Title on Itunes`,
	itunes_album_tracks_release.TrackArtist AS `Artist Track on iTunes`,
	-- tracks.lyrics,
	-- datasources.SourceURI as 'MP4_link',
	-- d.SourceURI AS 'Mp3_link',
	{}
	-- trackcountlog.DataSourceCount ->> '$.live' as count_live,
	-- trackcountlog.DataSourceCount ->> '$.remix' as count_remix
		
	FROM
		albums
		LEFT JOIN itunes_album_tracks_release ON albums.uuid = itunes_album_tracks_release.AlbumUUID 
		AND itunes_album_tracks_release.valid = 1
		LEFT JOIN tracks ON tracks.Title = itunes_album_tracks_release.trackname 
		AND tracks.Artist = itunes_album_tracks_release.trackartist 
		AND tracks.Valid = 1
		LEFT JOIN artists ON Artists.`Name` = itunes_album_tracks_release.TrackArtist 
		AND artists.valid = 1
		LEFT JOIN datasources ON datasources.trackid = tracks.id 
		AND datasources.Valid > 0 
		AND datasources.formatid = '74BA994CF2B54C40946EA62C3979DDA3'
		LEFT JOIN datasources d ON d.trackid = tracks.id 
		AND d.Valid > 0 
		AND d.formatid = '1A67A5F1E0D84FB9B48234AE65086375'
		LEFT JOIN trackcountlog ON trackcountlog.TrackID = tracks.id 
	WHERE
		(
			artists.UUID IN (
			SELECT
				cl.Ext ->> '$.artist_uuid' 
			FROM
				pointlogs pl
				JOIN crawlingtasks cl ON cl.id = pl.Ext ->> '$.crawler_id[0]' 
			WHERE
				cl.ActionId = 'B06AEE0F622D47F8B6FEF07DC2EABEAE' 
				AND pl.ActionType = 'MA' 
				AND pl.valid = 1 
				AND ( cl.Ext ->> '$.message' != 'Cannot download or upload artist image' OR cl.Ext ->> '$.message' IS NULL ) 
				AND pl.id IN {} # input pointlogs của MA vào
			) 
		) 
		AND albums.valid = 1 AND
		-- d.SourceURI IS NULL 
		{}
	GROUP BY
		itunes_album_tracks_release.TrackName,
		itunes_album_tracks_release.TrackArtist 
	ORDER BY
		itunes_album_tracks_release.Artist,
		itunes_album_tracks_release.AlbumUUID,
	itunes_album_tracks_release.Seq ASC 
	)"""