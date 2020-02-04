# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id int PRIMARY KEY, 
        start_time date NOT NULL, 
        user_id int NOT NULL, 
        level varchar, 
        song_id varchar, 
        artist_id varchar, 
        session_id int NOT NULL, 
        location varchar, 
        user_agent varchar, 
        UNIQUE(start_time, user_id, song_id, artist_id, session_id)
    );
""")

# user_id, first_name, last_name, gender, level
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY, 
    first_name varchar, 
    last_name varchar, 
    gender char(1), 
    level varchar
);
""")

# song_id, title, artist_id, year, duration
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY, 
    title varchar NOT NULL, 
    artist_id varchar NOT NULL, 
    year int, 
    duration float8,
    UNIQUE(song_id, title, artist_id, year, duration)
);
""")

# artist_id, name, location, latitude, longitude
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY, 
    name varchar NOT NULL, 
    location varchar, 
    latitude float8, 
    longitude float8
);
""")

# timestamps of records in songplays broken down into specific units
# start_time, hour, day, week, month, year, weekday
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time date PRIMARY KEY, 
    hour int NOT NULL, 
    day int NOT NULL, 
    week int NOT NULL, 
    month int NOT NULL, 
    year int NOT NULL, 
    weekday int NOT NULL
);""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays
    (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (songplay_id) DO UPDATE
        SET start_time=EXCLUDED.start_time,
            user_id=EXCLUDED.user_id,
            level=EXCLUDED.level,
            song_id=EXCLUDED.song_id,
            artist_id=EXCLUDED.artist_id,
            session_id=EXCLUDED.session_id,
            location=EXCLUDED.location,
            user_agent=EXCLUDED.user_agent
        WHERE EXCLUDED.start_time > songplays.start_time
""")

user_table_insert = ("""
INSERT INTO users
    (user_id, first_name, last_name, gender, level)
    VALUES (%s,%s,%s,%s,%s)
    ON CONFLICT (user_id) DO UPDATE 
        SET first_name=EXCLUDED.first_name, 
            last_name=EXCLUDED.last_name, 
            gender=EXCLUDED.gender, 
            level=EXCLUDED.level
""")

song_table_insert = ("""
INSERT INTO songs 
    (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id) DO UPDATE
        SET title=EXCLUDED.title,
            artist_id=EXCLUDED.artist_id, 
            year=EXCLUDED.year, 
            duration=EXCLUDED.duration
""")

artist_table_insert = ("""
INSERT INTO artists
    (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO UPDATE
        SET name=EXCLUDED.name, 
            location=EXCLUDED.location, 
            latitude=EXCLUDED.latitude, 
            longitude=EXCLUDED.longitude
""")

time_table_insert = ("""
INSERT INTO time
    (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time) DO NOTHING 
""")

# FIND SONGS

song_select = ("""
    SELECT song_id, songs.artist_id
    FROM songs
    JOIN artists 
        ON artists.artist_id=songs.artist_id
    WHERE title=%s and artists.name=%s and duration=%s;
""")

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, time_table_create, song_table_create, songplay_table_create]
drop_table_queries = [user_table_drop, artist_table_drop, time_table_drop, song_table_drop, songplay_table_drop]