#!/usr/bin/env python
import logging
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def drop_tables(cur):
    for query in drop_table_queries:
        logging.info('Running query: %s', query)
        cur.execute(query)


def create_tables(cur):
    for query in create_table_queries:
        logging.info('Running query: %s', query)
        cur.execute(query)


def main():
    logging.info('Reading configuration dwh.cfg')
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    logging.info('Connecting to redshift cluster')
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config["CLUSTER"].values()
        )
    )
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    drop_tables(cur)
    create_tables(cur)

    conn.close()
    logging.info('Table creation done!')

if __name__ == "__main__":
    main()
