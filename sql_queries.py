import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_drop = "DROP TABlE IF EXISTS staging_events;"
staging_songs_drop = "DROP TABlE IF EXISTS staging_songs;"
fact_songplays_drop = "DROP TABlE IF EXISTS fact_songplays;"
dim_users_drop = "DROP TABLE IF EXISTS dim_users;"
dim_songs_drop = "DROP TABLE IF EXISTS dim_songs;"
dim_artists_drop = "DROP TABLE IF EXISTS dim_artists;"
dim_time_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR(50),
    gender CHAR,
    itemInSession INTEGER,
    lastName VARCHAR(50),
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year FLOAT
);
""")






fact_songplays_create= ("""
CREATE TABLE IF NOT EXISTS fact_songplays 
(
    songplay_id INTEGER IDENTITY(1, 1) PRIMARY KEY,
    start_time INTEGER NOT NULL,
    userId INTEGER NOT NULL,
    level VARCHAR,
    song_id VARCHAR NOT NULL,
    artist_id VARCHAR,
    sessionId INTEGER NOT NULL,
    location VARCHAR,
    userAgent VARCHAR
);

""")

dim_users_create = ("""
CREATE TABLE IF NOT EXISTS dim_users
(
    userId INTEGER PRIMARY KEY,
    firstName VARCHAR,
    lastName VARCHAR,
    gender CHAR NOT NULL,
    level VARCHAR
)
    SORTKEY (userId);

""")

dim_songs_create = ("""
CREATE TABLE IF NOT EXISTS dim_songs
(
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR NOT NULL,
    year INTEGER,
    duration FLOAT
)
    SORTKEY (song_id);
""")

dim_artists_create = ("""
CREATE TABLE IF NOT EXISTS dim_artists
(
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT
)
    SORTKEY (artist_id);
""")

dim_time_create = ("""
CREATE TABLE IF NOT EXISTS dim_time
(
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER NOT NULL
)
    SORTKEY (start_time);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
iam_role {}
FORMAT AS json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs
FROM {}
iam_role {}
FORMAT AS json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplays_table_insert = ("""
INSERT INTO songplays (start_time, userId, level, song_id, artist_id, sessionId, location, userAgent)
SELECT DISTINCT
    TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
    se.userId,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId,
    se.location,
    se.userAgent
FROM staging_songs ss
JOIN staging_events se
ON (ss.title = se.song AND se.artist = ss.artist_name)
AND se.page = 'NextSong';
""")

users_table_insert = ("""
INSERT INTO users
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
WHERE userId IS NOT NULL
AND page = 'NextSong';
""")

songs_table_insert = ("""
INSERT INTO songs
SELECT
DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artists_table_insert = ("""
INSERT INTO artists
SELECT
DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time
SELECT DISTINCT
       TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       DATE_PART(dayofweek, TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second') AS weekday
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, fact_songplays_create, dim_users_create, dim_songs_create, dim_artists_create, dim_time_create]
drop_table_queries = [staging_events_drop, staging_songs_drop, fact_songplays_drop, dim_users_drop, dim_songs_drop, dim_artists_drop, dim_time_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplays_table_insert, users_table_insert, songs_table_insert, artists_table_insert, time_table_insert]
