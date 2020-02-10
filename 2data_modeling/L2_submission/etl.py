import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(conn, cur, filepath):
    """ Ingests a song json file and makes one insertion each in the song and artist tables, respectively.

    Args:
        cur: The postgres database cursor object
        conn: The postgres database connection object
        filepath (str): Path to a file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df.loc[:, ["song_id", "title", "artist_id", "year", "duration"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    conn.commit()
    
    # insert artist record
    artist_data = df.loc[:, ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)
    conn.commit()


def process_log_file(conn, cur, filepath):
    """ Ingests a user activity log json file and makes one insertion in the songplay table.

    Args:
        cur: The postgres database cursor object
        conn: The postgres database connection object
        filepath (str): Path to a file
    """
    
    # open log file
    df = df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.query("page == 'NextSong'").copy()

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = time_data = list(zip(t.values.tolist(), t.dt.hour.values.tolist(), t.dt.day.values.tolist(), 
             t.dt.week.values.tolist(), t.dt.month.values.tolist(), t.dt.year.values.tolist(), 
             t.dt.weekday.values.tolist()))
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame(time_data, columns=column_labels)
    time_df['start_time'] = pd.to_datetime(time_df['start_time'])

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        conn.commit()

    # load user table
    user_df = df.loc[:, ['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        conn.commit()
        
    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        song_id, artist_id = None, None
        if results:
            song_id, artist_id = results

        # insert songplay record
        songplay_data = (
            index, # songplay_id
            pd.to_datetime(row.ts, unit='ms'), # start_time
            int(row.userId), # user_id
            row.level, # level
            song_id, # song_id
            artist_id, # artist_id
            row.sessionId, # session_id
            row.location, # location
            row.userAgent # user_agent
        )
        cur.execute(songplay_table_insert, songplay_data)
        conn.commit()


def process_data(cur, conn, folderpath, func):
    """ Iterates over all .json files in a folder and processes them according to a function.
    
    Args:
        cur: The postgres database cursor object
        conn: The postgres database connection object
        folderpath (str): Path to a folder containing .json files
        func (function): Function used to process each individual .json file    
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(folderpath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, folderpath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(conn, cur, datafile)
        # conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """ Runs the entire ETL process for the sparkifydb. """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, folderpath='data/song_data', func=process_song_file)
    process_data(cur, conn, folderpath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()