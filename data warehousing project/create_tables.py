import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    this function executes queries in the sql_queries file that drop the intended tables
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    this functions executes the queries in the sql_queries file that creates the tables intended 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print(1)
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print(2)
   

    drop_tables(cur, conn)
    print(3)
    create_tables(cur, conn)
    print(4)

    conn.close()


if __name__ == "__main__":
    main()