count_top_single = """select count(CollectionId) from v4.collection_track where CollectionId in
        ('D64FACE6DD924835A8D84D2BE921A626',
        '3FC2DF479D2047618FBE73CB6B97BE93',
        'A9929F0E1E914C3BBC35BBAC4EAD565B',
        '61AF9E4DF27442108683E1618F882988',
        '60A78509773745CEBD86C4FA7341F156',
        '665B80D5511B427081BA13086BD7E21D',
        '335DADDBCB3541ADB817EE7746B89633',
        '6BFD1EA3553940CA82E74E348759BA24',
        'B5A900EBE39949B19F6A6F3F8172397F')
        """
count_top_album = """select count(ChartId) from v4.chart_album where ChartId in
        (369415522042770,
        201965558664655,
        206590576975598,
        426715595286541,
        513840613597484,
        288965631908426,
        638265705152197,
        537540686841254,
        903390650219369)
        """

count_new_classic = "select count(TrackId) from v4.classicnew where {}"

top_album_removed = """SELECT DISTINCT
            ca.AlbumId,
            ab.uuid albumuuid,
            count( abt.trackid ),
            count( ds.TrackId ),
            round( ( count( ds.TrackId ) / count( abt.trackid ) ) * 100, 0 ) AS fill_rate,
            ca.`Order` 
        FROM
            v4.chart_album ca
            LEFT JOIN v4.albums ab ON ab.id = ca.AlbumId
            LEFT JOIN v4.album_track abt ON abt.AlbumId = ab.UUID
            LEFT JOIN v4.tracks tr ON tr.id = abt.TrackId
            LEFT JOIN v4.datasources ds ON ds.TrackId = tr.id 
            AND ds.valid > 0 
            AND ds.formatid IN ( '1A67A5F1E0D84FB9B48234AE65086375', '74BA994CF2B54C40946EA62C3979DDA3' ) 
        GROUP BY
            ca.AlbumId 
        HAVING
            round( ( count( ds.TrackId ) / count( abt.trackid ) ) * 100, 0 ) < 60 
            OR count( ds.TrackId ) = 0"""

raw_sql_delete = "delete from {} where {} = '{}'"

album_query = """select UUID, Id from v4.albums where valid = 1 and UUID in {}"""

single_query = """select id, id from v4.tracks where valid = 1 and id in {}"""

newclassic_genre = (
    """select Title, UUID from v4.genres where valid = 1 and Title in {}"""
)

newclassic_title = (
    """select Title, Title from v4.genres where valid = 1 and Title in {}"""
)

genre_query = "select DISTINCT UUID from v4.genres where ParentId is not null"
