#!/usr/bin/env python
import logging
import configparser
import psycopg2
from prettytable import PrettyTable

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

total_song_plays = (
'''
    SELECT
        level,
        COUNT(*) AS total_song_plays
    FROM songplay
    GROUP BY level;
''',
'Total Number of Song Plays Per User Level',
['Level', 'Total Song Plays']
)

total_play_count = (
'''
    SELECT
        s.title AS song_title,
        COUNT(*) AS play_count
    FROM songplay sp
    JOIN song s ON sp.song_id = s.song_id
    GROUP BY s.title
    ORDER BY play_count DESC
    LIMIT 10;
''',
'Top 10 Songs by Play Count:',
['Song Title', 'Play Count']
)


user_dist = (
'''
    SELECT
        gender,
        COUNT(DISTINCT user_id) AS user_count
    FROM users
    GROUP BY gender;

''',
'User Distribution by Gender:',
['Gender', 'User count']
)


top_artists = (
'''
    SELECT
        a.name AS artist_name,
        COUNT(*) AS play_count
    FROM songplay sp
    JOIN artist a ON sp.artist_id = a.artist_id
    GROUP BY a.name
    ORDER BY play_count DESC
    LIMIT 5;
''',
'Top 5 Artists by Song Play Count:',
['Artist Name', 'Play Counts']
)

busiest_day_hours = (
'''
    SELECT
        EXTRACT(HOUR FROM start_time) AS hour_of_day,
        COUNT(*) AS song_plays
    FROM songplay
    GROUP BY hour_of_day
    ORDER BY song_plays DESC
    LIMIT 5;
''',
'Busiest Hours of the Day for Song Plays:',
['Hour of Day', 'Song Plays',]
)




QUERIES = [total_song_plays, total_play_count, user_dist, top_artists, busiest_day_hours]

def run_analyical_queries(cur):
    '''
    Runs a set of analytical queries and prints the result 
    in a table
    '''
    for query, title, column_names in QUERIES:
        logging.info('Running query: %s', title)

        cur.execute(query)
        rows = cur.fetchall()

        t = PrettyTable(column_names)
        t.title = title
        for row in rows:
            t.add_row(row)
        print(t, '\n')

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

    run_analyical_queries(cur)

    conn.close()


if __name__ == "__main__":
    main()
