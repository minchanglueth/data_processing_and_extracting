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
