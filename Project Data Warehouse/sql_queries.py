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

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
        event_id int IDENTITY(0,1),
        artist varchar,
        auth varchar, 
        firstName varchar,
        gender varchar,   
        itemInSession int,
        lastName varchar,
        length decimal,
        level varchar, 
        location varchar,
        method varchar,
        page varchar,
        registration varchar,
        sessionId int,
        song varchar,
        status int,
        ts timestamp,
        userAgent varchar,
        userId int
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        num_songs int,
        artist_id varchar,
        artist_latitude decimal,
        artist_longitude decimal,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration decimal,
        year int
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int IDENTITY(0,1) NOT NULL,
        start_time timestamp NOT NULL, 
        user_id varchar NOT NULL, 
        level varchar NOT NULL, 
        song_id varchar, 
        artist_id varchar,  
        session_id int, 
        location varchar, 
        user_agent varchar
        );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id int PRIMARY KEY NOT NULL, 
        first_name varchar, 
        last_name varchar, 
        gender varchar, 
        level varchar NOT NULL
        );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar PRIMARY KEY NOT NULL, 
        title varchar, 
        artist_id varchar, 
        year int, 
        duration decimal
        );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar PRIMARY KEY NOT NULL, 
        name varchar, 
        location varchar, 
        latitude decimal, 
        longitude decimal
        );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp PRIMARY KEY NOT NULL, 
        hour int, 
        day int, 
        week int, 
        month int, 
        year int, 
        weekday int
        );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    timeformat 'epochmillisecs'
    region 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    region 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT DISTINCT se.ts, 
                    se.userId, 
                    se.level, 
                    ss.song_id, 
                    ss.artist_id, 
                    se.sessionId, 
                    se.location, 
                    se.userAgent
    FROM staging_events se 
    INNER JOIN staging_songs ss 
        ON se.song = ss.title AND se.artist = ss.artist_name
    WHERE 1=1
    AND se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT se.userId, 
                    se.firstName, 
                    se.lastName, 
                    se.gender, 
                    se.level
    FROM staging_events se
    WHERE 1=1
    AND se.userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration) 
    SELECT DISTINCT ss.song_id, 
                    ss.title, 
                    ss.artist_id, 
                    ss.year, 
                    ss.duration
    FROM staging_songs ss
    WHERE 1=1
    AND ss.song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT ss.artist_id, 
                    ss.artist_name, 
                    ss.artist_location,
                    ss.artist_latitude,
                    ss.artist_longitude
    FROM staging_songs ss
    WHERE 1=1
    AND ss.artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT se.ts,
                    EXTRACT(hour from se.ts),
                    EXTRACT(day from se.ts),
                    EXTRACT(week from se.ts),
                    EXTRACT(month from se.ts),
                    EXTRACT(year from se.ts),
                    EXTRACT(weekday from se.ts)
    FROM staging_events se
    WHERE 1=1
    AND se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
