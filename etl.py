import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """"
    This part entails the Extraction (E), Transformation (T) and Loading (L) 
    or insertion of data or records into our already created tables. 
    In the lessons in classroom, we learned how to insert data manually 
    or from an already existing table using the SELECT statement. 
    Here, I used 'ETL' technique to extract, transform and load the data from 
    a json file into the tables to create a database schema.

    To start with, I performed ETL on a single song file and loaded a single record into each table 
    (artist and song tables because the song file contains data with respect to artist and song).
    
    A common code you would see in this part is the df.values and tolist() attributes. df.values gets the values and stores them in       an array and tolist() converts the array to a list form.
    
   """
    # open song file
    #filepath is already defined. see the latter part of this file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    df_columns = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = df_columns.values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    df_columns1 = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = df_columns1.values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    
    """
    Processed the log_data (as I previously did for the songs_data) to extract records, transformed the records 
    (especially, for the time table) and loaded into the users and time table as well as the songplays table. 
    This is because the log_data contains records for these tables.
    
    More on the time dimensional table: 
    I filtered records by NextSong action, converted the 'ts' timestamp column to datetime.
    I then extracted the timestamp, hour, day, week of year, month, year, and weekday from the converted ts 
    column and set time_data to a list containing these values. I then created a dataframe containing the time data against their         column labels.
    
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    from datetime import datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    start_time = t
    hour = t.dt.hour
    day = t.dt.day 
    week_of_year = t.dt.week 
    month = t.dt.month
    year = t.dt.year
    weekday = t.dt.weekday_name
    time_data = [start_time.tolist(), hour.values.tolist(), day.values.tolist(), week_of_year.values.tolist(), month.values.tolist(),     year.values.tolist(), weekday.values.tolist()]
    column_labels = ["start_time", "hour", "day", "week_of_year", "month", "year", "weekday"]
    time_ = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame.from_dict(time_)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location,             row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    """gets all files matching extension from directory"""
    
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
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    
    """
    - Establishes connection with the sparkify database and gets cursor to it. 
    - gets all the files
    - Finally, closes the connection. 
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()