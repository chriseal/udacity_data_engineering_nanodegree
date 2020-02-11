staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE off
    REGION 'us-west-2'
    FORMAT AS JSON 'auto';
""").format(SONG_DATA, IAM_ARN)


KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
HOST                 = config.get('CLUSTER','HOST')
IAM_ARN                 = config.get('IAM_ROLE','ARN')
LOG_DATA                 = config.get('S3','LOG_DATA')
LOG_JSONPATH                 = config.get('S3','LOG_JSONPATH')
SONG_DATA                 = config.get('S3','SONG_DATA')


# EXAMPLE SONG DATA
{
    "num_songs": 1, 
    "artist_id": "ARJIE2Y1187B994AB7", 
    "artist_latitude": null, 
    "artist_longitude": null, 
    "artist_location": "", 
    "artist_name": "Line Renaud", 
    "song_id": "SOUPIRU12A6D4FA1E1", 
    "title": "Der Kleine Dompfaff", 
    "duration": 152.92036, 
    "year": 0
}

# EXAMPLE LOG DATA
{
    "artist":"N.E.R.D. FEATURING MALICE",
    "auth":"Logged In",
    "firstName":"Jayden",
    "gender":"M",
    "itemInSession":0,
    "lastName":"Fox",
    "length":288.9922,
    "level":"free",
    "location":"New Orleans-Metairie, LA",
    "method":"PUT",
    "page":"NextSong",
    "registration":1541033612796.0,
    "sessionId":184,
    "song":"Am I High (Feat. Malice)",
    "status":200,
    "ts":1541121934796,
    "userAgent":"\"Mozilla\/5.0 (Windows NT 6.3; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
    "userId":"101"
}