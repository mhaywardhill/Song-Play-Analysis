import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *



def process_song_file(cur, dataset):
    # load song staging table
    dataset.to_csv("song_file.csv", sep = '|',index=False, header=False)
    with open('song_file.csv',encoding='utf-8') as f:
        cur.copy_from(f, 'song_staging', sep = '|', null='')

    # load song dimension
    cur.execute(song_table_insert)

    # load artist dimension
    cur.execute(artist_table_insert)

def process_log_file(cur, dataset):
     # load log staging table
    dataset = dataset[(dataset['page']=='NextSong')].astype({'ts': 'datetime64[ms]'})
    dataset.to_csv("log_file.csv", sep = '|',index=False, header=False)

    with open('log_file.csv',encoding='utf-8') as f:
        cur.copy_from(f, 'log_staging', sep = '|', null='')

    # load time dimension
    t = pd.to_datetime(dataset['ts'])
    time_data = list((t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday))
    column_labels = list(('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'))
    time_df =  pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user dimension
    cur.execute(user_table_insert)

    # load fact songplay table
    cur.execute(songplay_table_insert)

def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # iterate over files and process
    dfs = []
    for i, datafile in enumerate(all_files, 1):
        df = pd.read_json(datafile, lines=True)
        dfs.append(df)
        
    dataset = pd.concat(dfs, ignore_index=True) 
    func(cur, dataset)


def main():
    host = os.environ['PGHOSTADDR'] 
    password = os.environ['PGPASSWORD'] 
    conn = psycopg2.connect(host=host, port="5432", dbname="sparkifydb",  user="postgres",  password=password)
    cur = conn.cursor()
    conn.autocommit = True

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()