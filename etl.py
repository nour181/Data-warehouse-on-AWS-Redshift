import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
     Args:
        cur: the cursor that we will use to execute queries.
        conn: connection to database.

    Returns:
        load data from s3 bucket on staging tables (staging_events, staging_songs) """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
     Args:
        cur: the cursor that we will use to execute queries.
        conn: connection to database.

    Returns:
        insert data in fact and dimensional tables from staging tables """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
     Args:
        cur: the cursor that we will use to execute queries.
        conn: connection to database.

    Returns:
        read data from .cfg file, execute previous functions, then close the connection  """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()