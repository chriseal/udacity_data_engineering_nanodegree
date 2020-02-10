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


