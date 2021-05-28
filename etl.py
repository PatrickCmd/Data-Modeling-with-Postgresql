import os
import sys
import glob
import psycopg2
import pandas as pd
from datetime import datetime
from sql_queries import *


def process_song_file(cur, filepath):
    """Process songs data and return song and artist dataframes"""
    # open song file
    df = pd.read_json(filepath, lines=True)

    # song dataframe
    song_data = df.loc[:, ["song_id", "title", "artist_id", "year", "duration"]]
    song_data.fillna("", inplace=True)
    
    # artist dataframe
    artist_data = df.loc[:, ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]]
    artist_data.fillna(0, inplace=True)
    
    return song_data, artist_data


def insert_song_data(cur, song_data):
    """Load song data into the songs table"""
    # insert song record
    song_data = list(song_data.values[0])
    cur.execute(song_table_insert, song_data)

    
def insert_artists_data(cur, artist_data):
    """Load artist data into the artist table"""
    # insert artist record
    # check if artist already exists
    cur.execute(artist_select, tuple(artist_data["artist_id"].values))
    artist = cur.fetchone()
    if not artist:
        artist_data = list(artist_data.values[0]) 
        cur.execute(artist_table_insert, artist_data)
        

def ts_to_datetime(timestamp):
    """Convert timestamp into a datetime object"""
    # timestamp is in milliseconds so we divide by 1000.0
    dt_object = datetime.fromtimestamp(timestamp / 1000.0)
    return dt_object


def process_log_file(cur, filepath):
    """Process logs data and return dataframe, user and time dataframes"""
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    df["ts"] = df["ts"].apply(ts_to_datetime)
    t = df 
    
    # Extract the timestamp, hour, day, week of year, month, year, and weekday from the ts column and set time_data to a list containing these values in order
    # (start_time, hour, day, week, month, year, weekday)
    time_data = (df["ts"], df["ts"].dt.hour, df["ts"].dt.day, df["ts"].dt.week, df["ts"].dt.month, df["ts"].dt.year, df["ts"].dt.weekday)
    column_labels = ("timestamp", "hour", "day", "weekofyear", "month", "year", "weekday")
    
    # Create a dataframe, time_df, containing the time data for this file by combining column_labels and time_data into a dictionary and converting this into a dataframe
    time_data_dict = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame(time_data_dict)

    # load user table
    user_df = df.loc[:, ["userId", "firstName", "lastName", "gender", "level"]]

    return df, time_df, user_df


def insert_time_data(cur, time_df):
    """Load time data into the time table"""
    # insert time data records
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))


def insert_user_and_songplays_data(cur, df, user_df):
    """Load user and songplay data into users and songplays tables respectively"""
    # insert user records if not already exists
    for i, row in user_df.iterrows():
        cur.execute(user_select, (row["userId"], ))
        user = cur.fetchone()
        if user:
            # print("Skipping insertion of already existing user!")
            continue
        cur.execute(user_table_insert, list(row))

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record (start_time, user_id, level, song_id, artist_id, location, user_agent) 
        # row["ts"], row["userId"], row["level"], row["sessionId"], row["location"], row["userAgent"])
        songplay_data = (row["ts"], row["userId"], row["level"], songid, artistid, row["sessionId"], row["location"], row["userAgent"])
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Iterate over the files in a given filepath, process the data and load it
    into the respective database tables.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        if filepath == 'data/song_data':
            song_data, artist_data = func(cur, datafile)
            insert_song_data(cur, song_data)
            insert_artists_data(cur, artist_data)
         
        if filepath == 'data/log_data':
            df, time_df, user_df = func(cur, datafile)
            insert_time_data(cur, time_df)
            insert_user_and_songplays_data(cur, df, user_df)

        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()