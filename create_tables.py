import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
     """
     Args:
        cur: the cursor that we will use to execute queries.
        conn: connection to database.

    Returns:
        drop tables from the database if previously created """
        
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
     """
     Args:
        cur: the cursor that we will use to execute queries.
        conn: connection to database.

    Returns:
        create tables """
        
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
         """
     Args:
        config: module that we will use to read data from .cfg file .
        conn: connection to database.
        cur: the cursor that we will use to execute queries.
        
    Returns:
        read data from .cfg file, execute previous functions then close the connection """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()