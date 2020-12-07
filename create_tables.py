import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import os

host = os.environ['PGHOSTADDR'] 
password = os.environ['PGPASSWORD'] 

def create_database():
    # connect to default database
    conn = psycopg2.connect(host=host, port="5432", dbname="postgres",  user="postgres",  password=password)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb WITH (FORCE);")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0;")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect(host=host, port="5432", dbname="sparkifydb",  user="postgres",  password=password)
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()