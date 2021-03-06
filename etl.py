import logging
import sys
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


logging.basicConfig(
    stream=sys.stdout, level=logging.INFO, format="%(asctime)s %(message)s"
)


def process_song_data(cur, dataset):
    '''' process the song data '''
    logging.info("Loading song staging table")
    dataset.to_csv("song_file.csv", sep="|", index=False, header=False)
    with open("song_file.csv", encoding="utf-8") as f:
        cur.copy_from(f, "song_staging", sep="|", null="")

    # load song dimension
    logging.info("Loading song dimension")
    cur.execute(song_table_insert)

    # load artist dimension
    logging.info("Loading artist dimension")
    cur.execute(artist_table_insert)


def process_log_data(cur, dataset):
    ''' process the log data '''
    logging.info("Loading log staging table")
    dataset = dataset[(dataset["page"] == "NextSong")].astype({"ts": "datetime64[ms]"})
    dataset.to_csv("log_file.csv", sep="|", index=False, header=False)

    with open("log_file.csv", encoding="utf-8") as f:
        cur.copy_from(f, "log_staging", sep="|", null="")

    # load time dimension
    # would be more efficient creating the records for the tim dim from the log staging table in Postgres
    logging.info("Loading time dimension")
    t = pd.to_datetime(dataset["ts"])
    time_data = list(
        (
            t,
            t.dt.hour,
            t.dt.day,
            t.dt.isocalendar().week,
            t.dt.month,
            t.dt.year,
            t.dt.weekday,
        )
    )
    column_labels = list(
        ("start_time", "hour", "day", "week", "month", "year", "weekday")
    )
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user dimension
    logging.info("Loading user dimension")
    cur.execute(user_table_insert)

    # load fact songplay table
    logging.info("Loading songplay fact table")
    cur.execute(songplay_table_insert)


def process_data(cur, conn, filepath, func):
    ''' gets all files matching extension from directory '''
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    logging.info("%i files found in %s", num_files, filepath)

    # iterate over files and process
    dfs = []
    for i, datafile in enumerate(all_files, 1):
        df = pd.read_json(datafile, lines=True)
        dfs.append(df)

    dataset = pd.concat(dfs, ignore_index=True)
    func(cur, dataset)
    conn.commit()


def main():
    host = os.environ["PGHOSTADDR"]
    password = os.environ["PGPASSWORD"]
    conn = psycopg2.connect(
        host=host, port="5432", dbname="sparkifydb", user="postgres", password=password
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_data)
    process_data(cur, conn, filepath="data/log_data", func=process_log_data)
    logging.info("Finished!")

    if conn:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
