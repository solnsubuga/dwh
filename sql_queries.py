import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")


dwh_role = config.get("IAM_ROLE", "ARN")
log_data = config.get("S3", "LOG_DATA")
log_json_path = config.get("S3", "LOG_JSONPATH")
song_data = config.get("S3", "SONG_DATA")

NEXT_SONG_STATUS = "NextSong"

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = """
    CREATE TABLE staging_events(
        artist TEXT NULL,
        auth   VARCHAR NULL,
        firstName VARCHAR  NULL,
        gender VARCHAR NULL,
        itemInsession INTEGER  NULL,
        lastName VARCHAR  NULL,
        length FLOAT  NULL,
        level VARCHAR NULL,
        location text NULL,
        method VARCHAR NULL,
        page VARCHAR NULL,
        registration FLOAT  NULL,
        sessionId INTEGER  sortkey distkey,
        song TEXT NULL,
        status INTEGER NULL,
        ts BIGINT NOT NULL,
        userAgent TEXT NULL,
        userId INTEGER NULL
    )

"""

staging_songs_table_create = """
    CREATE TABLE staging_songs(
        num_songs INTEGER  NULL,
        artist_id VARCHAR NOT NULL  distkey,
        artist_latitude VARCHAR,
        artist_longitude VARCHAR,
        artist_location TEXT,
        artist_name VARCHAR,
        song_id VARCHAR NOT NULL sortkey,
        title  text  NULL,
        duration FLOAT  NULL,
        year INTEGER NULL 
    )
"""

songplay_table_create = """
  CREATE TABLE songplay (
    songplay_id INT IDENTITY(1, 1) PRIMARY KEY NOT NULL,
    start_time TIMESTAMP NOT NULL distkey,
    user_id  INTEGER NOT NULL,
    song_id  VARCHAR NOT NULL sortkey,
    artist_id  VARCHAR NOT NULL,
    session_id  INTEGER NOT NULL,
    level  VARCHAR NOT NULL,
    location   text NULL,
    user_agent text NULL
  )
"""

user_table_create = """
  CREATE TABLE users(
      user_id INTEGER PRIMARY KEY NOT NULL sortkey,
      first_name  VARCHAR  NULL,
      last_name VARCHAR  NULL,
      gender VARCHAR,
      level VARCHAR NULL
  ) diststyle all;
"""

song_table_create = """
   CREATE TABLE song(
       song_id  VARCHAR PRIMARY KEY NOT NULL,
       title  text NOT NULL,
       artist_id  VARCHAR NOT NULL sortkey,
       year  INTEGER NOT NULL,
       duration FLOAT NOT NULL
   ) diststyle all;
"""

artist_table_create = """
    CREATE TABLE artist(
       artist_id  VARCHAR PRIMARY KEY NOT NULL sortkey,
       name  TEXT,
       location  TEXT,
       longitude DECIMAL(9),
       latitude DECIMAL(9)
    ) diststyle all;
"""

time_table_create = """
 CREATE TABLE time(
    time_key INT IDENTITY(1,1) PRIMARY KEY NOT NULL sortkey,
    start_time TIMESTAMP NOT NULL,
    hour SMALLINT NOT NULL,
    day  SMALLINT NOT NULL,
    week  SMALLINT NOT NULL,
    month   SMALLINT NOT NULL,
    year   SMALLINT NOT NULL,
    weekday SMALLINT NOT NULL
 ) diststyle all;
"""

# STAGING TABLES

staging_events_copy = (
    """
  COPY staging_events FROM '{}'
    credentials 'aws_iam_role={}'
    format as json '{}';
    """
).format(log_data, dwh_role, log_json_path)

staging_songs_copy = (
    """
    COPY staging_songs FROM '{}' 
    credentials 'aws_iam_role={}'
    format as json 'auto';"""
).format(song_data, dwh_role)

# FINAL TABLES

songplay_table_insert = (
    """
   INSERT INTO songplay (start_time, user_id, song_id, artist_id, session_id, level, location, user_agent)
    SELECT 
        TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' AS start_time,
        e.userid AS user_id,
        s.song_id,
        s.artist_id,
        e.sessionid AS session_id,
        e.level,
        e.location,
        e.useragent AS user_agent
    FROM staging_events e  JOIN staging_songs s ON (e.song = s.title)
    WHERE e.page = '{}';
"""
).format(NEXT_SONG_STATUS)

user_table_insert = (
    """
   INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT 
        DISTINCT e.userid,
        e.firstname as first_name,
        e.lastname AS last_name,
        e.gender,
        e.level
    FROM staging_events e  WHERE e.page = '{}';
"""
).format(NEXT_SONG_STATUS)

song_table_insert = """
   INSERT INTO song (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs;
"""

artist_table_insert = """
   INSERT INTO artist (artist_id, name, location, longitude, latitude)
    SELECT 
        DISTINCT artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM staging_songs;
"""

time_table_insert = (
    """
   INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' AS start_time,
        EXTRACT(hour FROM start_time)                              AS hour,
        EXTRACT(day  FROM start_time)                              AS day,
        EXTRACT(week FROM start_time)                              AS week,
        EXTRACT(month FROM start_time)                             AS month,
        EXTRACT(month FROM start_time)                             AS year,
        EXTRACT(DOW FROM start_time)                               AS weekday
    FROM staging_events WHERE page='{}';
"""
).format(NEXT_SONG_STATUS)

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
