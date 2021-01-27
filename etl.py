import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    load data from s3 into staging tables.
    
        Parameters:
            cur (Object):  cursor of psycopg2 connect 
            conn (Object): psycopg2 instance
            
        Returns:
            Null
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    insert data from staging tables to Redshift tables.
    
        Parameters:
            cur (Object):  cursor of psycopg2 connect 
            conn (Object): psycopg2 instance
            
        Returns:
            Null
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()