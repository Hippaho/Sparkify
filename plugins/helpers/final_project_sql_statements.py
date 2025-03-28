class SqlQueries:
    """
    SqlQueries: A collection of SQL queries for our ETL pipeline.
    Think of this as our SQL recipe book for transforming and loading data into Redshift.
    We've got everything from fact table inserts to dimension table populating magic.
    """

    songplay_table_insert = ("""
        SELECT
            md5(events.sessionid || events.start_time) songplay_id,  -- Let's give each songplay a unique fingerprint
            events.start_time,  -- When did this songplay happen?
            events.userid,  -- Who was listening?
            events.level,  -- Free or premium listener?
            songs.song_id,  -- What song was playing?
            songs.artist_id,  -- And who sang it?
            events.sessionid,  -- Which session was this?
            events.location,  -- Where was the listener?
            events.useragent  -- What device were they using?
        FROM (
            SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, * -- Turning those timestamps into something readable
            FROM staging_events  -- Grabbing our raw event data
            WHERE page='NextSong'  -- Only interested in song plays
        ) events
        LEFT JOIN staging_songs songs  -- Matching up songs with event data
        ON events.song = songs.title  -- By song title
            AND events.artist = songs.artist_name  -- And artist name
            AND events.length = songs.duration  -- And song duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level  -- Getting the lowdown on our users
        FROM staging_events  -- From our raw event logs
        WHERE page='NextSong'  -- Only when they play a song
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration  -- All about the songs
        FROM staging_songs  -- From our song metadata
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude  -- The artists behind the music
        FROM staging_songs  -- From our song metadata
    """)

    time_table_insert = ("""
        SELECT start_time,  -- When did the songplay happen?
               extract(hour from start_time),  -- Hour of the day
               extract(day from start_time),  -- Day of the month
               extract(week from start_time),  -- Week of the year
               extract(month from start_time),  -- Month of the year
               extract(year from start_time),  -- Year
               extract(dayofweek from start_time)  -- Day of the week
        FROM songplays  -- From our songplays fact table
    """)