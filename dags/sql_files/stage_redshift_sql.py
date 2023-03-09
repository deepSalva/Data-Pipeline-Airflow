class SqlQueries:
    songplay_table_insert = ("""
        INSERT INTO songplays (playid, start_time, userid, level, songid, artistid, sessionid, 
                       location, user_agent)
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)


    user_table_append = ("""
        INSERT INTO users (userid, first_name, last_name, gender, level)
        SELECT 
            distinct userid, 
            firstname, 
            lastname, 
            gender, 
            level
        FROM staging_events
        WHERE page='NextSong'
    """)

    user_table_truncate = ("""
    BEGIN;
    TRUNCATE users;
        INSERT INTO users (userid, first_name, last_name, gender, level)
        SELECT 
            distinct userid, 
            firstname, 
            lastname, 
            gender, 
            level
        FROM staging_events
        WHERE page='NextSong';
        COMMIT;
    """)

    song_table_append = ("""
        INSERT INTO songs (songid, title, artistid, year, duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    song_table_truncate = ("""
    BEGIN;
    TRUNCATE songs;
        INSERT INTO songs (songid, title, artistid, year, duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs;
        COMMIT;
    """)

    artist_table_append = ("""
        INSERT INTO artists (artistid, name, location, lattitude, longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    artist_table_truncate = ("""
    BEGIN;
    TRUNCATE TABLE artists;
        INSERT INTO artists (artistid, name, location, lattitude, longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs;
        COMMIT;
    """)

    time_table_append = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

    time_table_truncate = ("""
    BEGIN;
        TRUNCATE TABLE time;
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays;
        COMMIT;
    """)

    user_table_insert = [user_table_append, user_table_truncate]
    song_table_insert = [song_table_append, song_table_truncate]
    artist_table_insert = [artist_table_append, artist_table_truncate]
    time_table_insert = [time_table_append, time_table_truncate]

    COPY_SQL = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        region 'us-east-1'
        {}
    """

    quality_null = ("""
        select 
            sum(case when {} is null then 1 else 0 end) as sum_colum
        from {}
    """)