#!/usr/bin/env python
import logging
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')


def run_table_queries(cur, table_queries):
    for query in table_queries:
        logging.info('Running query %s', query)
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

    run_table_queries(cur, copy_table_queries)
    run_table_queries(cur, insert_table_queries)

    conn.close()

    logging.info('Data inserting into tables successfully')


if __name__ == "__main__":
    main()
