class SqlQueries:
    # CREATE TABLES
    staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.staging_events(
        artist          VARCHAR,
        auth            VARCHAR,
        firstName       VARCHAR,
        gender          CHAR(1),
        itemInSession   INTEGER,
        lastName        VARCHAR,
        length          FLOAT,
        level           VARCHAR,
        location        VARCHAR,
        method          VARCHAR,
        page            VARCHAR,
        registration    FLOAT,
        sessionId       INTEGER,
        song            VARCHAR,
        status          INTEGER,
        ts              TIMESTAMP,
        userAgent       VARCHAR,
        userId          INTEGER
    );
    """)

    staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.staging_songs(
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER
    );
    """)

    songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.songplays(
        songplay_id INTEGER     NOT NULL PRIMARY KEY IDENTITY(0,1), 
        start_time  TIMESTAMP   NOT NULL SORTKEY DISTKEY, 
        user_id     INTEGER     NOT NULL, 
        level       VARCHAR, 
        song_id     VARCHAR     NOT NULL, 
        artist_id   VARCHAR     NOT NULL, 
        session_id  INTEGER     NOT NULL, 
        location    VARCHAR, 
        user_agent  VARCHAR
    );
    ;
    """)

    user_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.users(
        user_id     INTEGER     NOT NULL PRIMARY KEY,
        first_name  VARCHAR     NOT NULL, 
        last_name   VARCHAR     NOT NULL, 
        gender      CHAR(1), 
        level       VARCHAR
    )
    COMPOUND SORTKEY(last_name, first_name);
    """)

    song_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.songs(
        song_id     VARCHAR     NOT NULL PRIMARY KEY, 
        title       VARCHAR     NOT NULL, 
        artist_id   VARCHAR     NOT NULL, 
        year        INTEGER, 
        duration    FLOAT4
    )
    COMPOUND SORTKEY(artist_id, song_id, year, title);
    """)

    artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.artists(
        artist_id   VARCHAR     PRIMARY KEY NOT NULL, 
        name        VARCHAR     SORTKEY,
        location    VARCHAR, 
        latitude    FLOAT8, 
        longitude   FLOAT8
    );
    """)

    time_table_create = ("""
    CREATE TABLE IF NOT EXISTS public."time"(
        start_time  TIMESTAMP   PRIMARY KEY NOT NULL DISTKEY SORTKEY, 
        hour        SMALLINT    NOT NULL,
        day         SMALLINT    NOT NULL, 
        week        SMALLINT    NOT NULL, 
        month       SMALLINT    NOT NULL, 
        year        SMALLINT    NOT NULL, 
        weekday     SMALLINT    NOT NULL
    );
    """)

    create_table_statements = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

    ## INSERTIONS
    # songplay_table_insert = ("""
    #     SELECT
    #             md5(events.sessionId || events.start_time) songplay_id,
    #             events.start_time, 
    #             events.userid, 
    #             events.level, 
    #             songs.song_id, 
    #             songs.artist_id, 
    #             events.sessionId, 
    #             events.location, 
    #             events.useragent
    #             FROM (
    #                 SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
    #                 FROM staging_events
    #                 WHERE page='NextSong'
    #             ) events
    #         LEFT JOIN staging_songs songs
    #         ON events.song = songs.title
    #             AND events.artist = songs.artist_name
    #             AND events.length = songs.duration
    # """)


    songplay_table_insert = ("""
        SELECT DISTINCT(e.ts) AS start_time,
            e.userid AS user_id,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionid AS session_id,
            e.location,
            e.useragent AS user_agent
        FROM staging_events e
        JOIN staging_songs s
            ON (e.song=s.title AND e.artist=s.artist_name)
        WHERE e.page='NextSong'
            AND e.userid IS NOT NULL;
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
