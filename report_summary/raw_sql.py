check_if_subgenred = """SELECT DISTINCT
            tr.id AS tracks_id,
            'yes' AS if_subgenred 
        FROM
            v4.tracks tr
            LEFT JOIN v4.classicnew cn ON cn.TrackId = tr.id 
        WHERE
            tr.valid = 1 -- ('1A67A5F1E0D84FB9B48234AE65086375','74BA994CF2B54C40946EA62C3979DDA3')
            
            AND cn.TrackId IS NOT NULL 
            AND tr.id IN {}"""

check_if_mp3crawled = """SELECT
            tr.id AS tracks_id,
            ds.SourceURI 
        FROM
            v4.datasources ds
            JOIN v4.tracks tr ON tr.id = ds.TrackId 
        WHERE
            ds.valid = 1 
            AND tr.valid = 1 
            AND ds.FormatID = '1A67A5F1E0D84FB9B48234AE65086375'	
            AND tr.id IN {}"""

check_if_mp4crawled = """SELECT
            tr.id AS tracks_id,
            ds.SourceURI 
        FROM
            v4.datasources ds
            JOIN v4.tracks tr ON tr.id = ds.TrackId 
        WHERE
            ds.valid = 1 
            AND tr.valid = 1 
            AND ds.FormatID = '74BA994CF2B54C40946EA62C3979DDA3'	
            AND tr.id IN {}"""
