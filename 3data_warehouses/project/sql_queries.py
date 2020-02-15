import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
HOST                 = config.get('CLUSTER','HOST')
IAM_ARN                 = config.get('IAM_ROLE','ARN')
LOG_DATA                 = config.get('S3','LOG_DATA')
LOG_JSONPATH                 = config.get('S3','LOG_JSONPATH')
SONG_DATA                 = config.get('S3','SONG_DATA')


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
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
CREATE TABLE staging_songs(
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
CREATE TABLE IF NOT EXISTS songplays(
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
CREATE TABLE IF NOT EXISTS users(
    user_id     INTEGER     NOT NULL PRIMARY KEY,
    first_name  VARCHAR     NOT NULL, 
    last_name   VARCHAR     NOT NULL, 
    gender      CHAR(1), 
    level       VARCHAR
)
COMPOUND SORTKEY(last_name, first_name);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id     VARCHAR     NOT NULL PRIMARY KEY, 
    title       VARCHAR     NOT NULL, 
    artist_id   VARCHAR     NOT NULL, 
    year        INTEGER, 
    duration    FLOAT4
)
COMPOUND SORTKEY(artist_id, song_id, year, title);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id   VARCHAR     PRIMARY KEY NOT NULL, 
    name        VARCHAR     SORTKEY,
    location    VARCHAR, 
    latitude    FLOAT8, 
    longitude   FLOAT8
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time  TIMESTAMP   PRIMARY KEY NOT NULL DISTKEY SORTKEY, 
    hour        SMALLINT    NOT NULL,
    day         SMALLINT    NOT NULL, 
    week        SMALLINT    NOT NULL, 
    month       SMALLINT    NOT NULL, 
    year        SMALLINT    NOT NULL, 
    weekday     SMALLINT    NOT NULL
);
""")

# STAGING TABLES
# log_json_path.json selects the correct ordering from the json keys to merge into the ordered columns in the table
staging_events_copy = ("""
    COPY staging_events
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {};
""").format(LOG_DATA, IAM_ARN, LOG_JSONPATH)
# also tried this: to_timestamp(ts,'MM/DD/YYYY HH24:MI:SS')
    
staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE off
    REGION 'us-west-2'
    FORMAT AS JSON 'auto'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, IAM_ARN)


# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
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
WHERE e.page='NextSong';
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT(userid) AS user_id,
    firstname AS first_name,
    lastname AS last_name,
    gender,
    level
FROM staging_events
WHERE userid IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT(song_id),
    title,
    artist_id,
    year,
    duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT(artist_id) AS artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts AS start_time,
    EXTRACT(hour FROM ts) AS hour,
    EXTRACT(day FROM ts) AS day,
    EXTRACT(week FROM ts) AS week,
    EXTRACT(month FROM ts) AS month,
    EXTRACT(year FROM ts) AS year,
    EXTRACT(weekday FROM ts) AS weekday
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
