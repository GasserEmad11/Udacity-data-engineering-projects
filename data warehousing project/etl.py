import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    this function executes the copying queries availible in the sql_queries file 
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    this function executes the insert statments/queries availible in the sql_queries file
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print(1)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print (2)
    
    load_staging_tables(cur, conn)
    print(3)
    insert_tables(cur, conn)
    print (4)

    conn.close()


if __name__ == "__main__":
    main()