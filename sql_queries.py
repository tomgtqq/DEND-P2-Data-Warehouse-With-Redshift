import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# staging table
staging_events_table_create= """
CREATE TABLE IF NOT EXISTS staging_events (
    event_id         INTEGER IDENTITY(0,1) PRIMARY KEY,
    artist           VARCHAR(255),
    auth             VARCHAR(20),
    firstName        VARCHAR(255),
    gender           VARCHAR(1),
    itemInSession    INTEGER,
    lastName         VARCHAR(255),
    length           FLOAT,
    level            VARCHAR(10),
    location         VARCHAR(255),
    method           VARCHAR(20),
    page             VARCHAR(20),
    registration     FLOAT,
    sessionId        INTEGER,
    song             VARCHAR(255),
    status           INTEGER,
    ts               VARCHAR(255),
    userAgent        VARCHAR(255),
    userId           INTEGER
)
diststyle auto;
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs        INTEGER,
    artist_id        VARCHAR(20),
    artist_latitude  FLOAT,
    artist_longitude FLOAT,
    artist_location  VARCHAR(255),
    artist_name      VARCHAR(255),
    song_id          VARCHAR(20),
    title            VARCHAR(255),
    duration         FLOAT,
    year             INTEGER
)
diststyle auto;
"""

# fact table
songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id         INTEGER IDENTITY(0,1) PRIMARY KEY,
    user_id             INTEGER       NOT NULL,
    song_id             VARCHAR(20)   NOT NULL,
    artist_id           VARCHAR(20)   NOT NULL,
    start_time          TIMESTAMP     NOT NULL, 
    session_id          VARCHAR(255),
    user_agent          VARCHAR(255),
    level               VARCHAR(20)   NOT NULL,
    location            VARCHAR(255)
)
diststyle auto;
"""

# dimension table
user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    user_id             INTEGER PRIMARY KEY DISTKEY,
    first_name          VARCHAR(255),
    last_name           VARCHAR(255),
    gender              VARCHAR(1),
    level               VARCHAR(10) NOT NULL
);
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id             VARCHAR(20) PRIMARY KEY DISTKEY,
    title               VARCHAR(255) NOT NULL,
    artist_id           VARCHAR(20)  NOT NULL,
    year                INTEGER,
    duration            FLOAT        NOT NULL
);

"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id           VARCHAR(20) PRIMARY KEY DISTKEY,
    name                VARCHAR(255) NOT NULL SORTKEY,
    location            VARCHAR(255),
    latitude            FLOAT,
    longitude           FLOAT
);
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time (
    start_time          TIMESTAMP PRIMARY KEY SORTKEY DISTKEY,
    hour                INTEGER,
    day                 INTEGER,
    week                INTEGER,
    month               INTEGER,
    year                INTEGER,
    weekday             INTEGER
);
"""

# STAGING TABLES
staging_events_copy = ("""
   copy staging_events from {}
   credentials 'aws_iam_role={}'
   json {}
   region 'us-west-2'
   
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2'
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))


# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays (user_id, song_id, artist_id, start_time, session_id, user_agent, level, location)
SELECT DISTINCT
    e.userId,
    s.song_id,
    s.artist_id,
    TIMESTAMP 'epoch' + e.ts/1000*INTERVAL '1 second',
    e.sessionId,
    e.userAgent,
    e.level,
    e.location
FROM 
    staging_events e, staging_songs s
WHERE 
    e.page='NextSong' AND
    e.song=s.title AND
    e.userId NOT IN (
        SELECT DISTINCT
            sp.user_id
        FROM
            songplays sp
        WHERE
            sp.user_id=e.userId AND
            sp.session_id=e.sessionId
    )
""")

# clear duplicate data and useId is null 
user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    CAST(userId AS INTEGER) AS user_id,
    firstName,
    lastName,
    gender,
    level
FROM 
    staging_events
WHERE 
    page='NextSong' AND
    user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

#  insert unique song_id
song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM 
    staging_songs
WHERE
    song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

# keep unique record with artist_id
artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM
    staging_songs
WHERE
    artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

# convert 'epoch' value back to a time stamp with second as the unit
time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT
    ts,
    EXTRACT(hr FROM ts) AS hour,
    EXTRACT(d FROM ts) AS day,
    EXTRACT(w FROM ts) AS week,
    EXTRACT(mon FROM ts) AS month,
    EXTRACT(yr FROM ts) AS year,
    EXTRACT(weekday FROM ts) AS weekday
FROM
    (
        SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as ts
        FROM staging_events
    )
WHERE
    ts NOT IN (SELECT DISTINCT start_time FROM time)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
