class SqlQueries:
    """
    The class, SqlQueries, contains SQL queries used in the ETL process for a data pipeline.
    It is designed to work with AWS Redshift and it contains SQL strings for making and populating tables.
    """

    songplay_table_insert = ("""
        SELECT
            md5(events.sessionid || events.start_time) songplay_id,  -- Generate a unique ID for each songplay
            events.start_time,  -- Timestamp of the songplay
            events.userid,  -- User ID
            events.level,  -- User's service level (free or paid)
            songs.song_id,  -- Song ID
            songs.artist_id,  -- Artist ID
            events.sessionid,  -- Session ID
            events.location,  -- User's location
            events.useragent  -- User's browser/OS information
        FROM (
            SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, * -- Convert timestamp to datetime
            FROM staging_events  -- Source table: staging_events
            WHERE page='NextSong'  -- Filter for 'NextSong' events
        ) events
        LEFT JOIN staging_songs songs  -- Join with staging_songs table
        ON events.song = songs.title  -- Join condition: song title
            AND events.artist = songs.artist_name  -- Join condition: artist name
            AND events.length = songs.duration  -- Join condition: song duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level  -- Select distinct user information
        FROM staging_events  -- Source table: staging_events
        WHERE page='NextSong'  -- Filter for 'NextSong' events
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration  -- Select distinct song information
        FROM staging_songs  -- Source table: staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude  -- Select distinct artist information
        FROM staging_songs  -- Source table: staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time,  -- Start time of the songplay
               extract(hour from start_time),  -- Extract hour from start time
               extract(day from start_time),  -- Extract day from start time
               extract(week from start_time),  -- Extract week from start time
               extract(month from start_time),  -- Extract month from start time
               extract(year from start_time),  -- Extract year from start time
               extract(dayofweek from start_time)  -- Extract day of week from start time
        FROM songplays  -- Source table: songplays
    """)