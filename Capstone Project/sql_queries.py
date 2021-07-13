import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_questions_table_drop = "DROP TABLE IF EXISTS staging_questions"
staging_users_table_drop = "DROP TABLE IF EXISTS staging_users"
staging_tags_table_drop = "DROP TABLE IF EXISTS staging_tags"

question_table_drop = "DROP TABLE IF EXISTS question_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
location_table_drop = "DROP TABLE IF EXISTS location_table"
tag_table_drop = "DROP TABLE IF EXISTS tag_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

staging_questions_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_questions
    (
        question_id BIGINT,
        title varchar,
        view_count BIGINT,
        creation_date BIGINT,
        user_id BIGINT,
        is_answered BOOLEAN,
        tags varchar
    );
""")

staging_users_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_users
    (
        user_id BIGINT,
        display_name varchar,
        reputation BIGINT,
        user_type varchar,
        location varchar
    );
""")

staging_tags_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_tags
    (
        count BIGINT,
        name varchar
    );
""")

question_table_create = ("""
    CREATE TABLE IF NOT EXISTS question_table (
        question_id BIGINT NOT NULL,
        title varchar NOT NULL, 
        view_count BIGINT NOT NULL, 
        time_id BIGINT NOT NULL, 
        user_id BIGINT, 
        is_answered BOOLEAN,  
        tag_id BIGINT,
        location_id BIGINT
        );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS user_table (
        user_id BIGINT PRIMARY KEY NOT NULL, 
        display_name varchar, 
        reputation BIGINT, 
        user_type varchar
        );
""")

location_table_create = ("""
    CREATE TABLE IF NOT EXISTS location_table (
        location_id BIGINT IDENTITY(0,1) NOT NULL, 
        location varchar
        );
""")

tag_table_create = ("""
    CREATE TABLE IF NOT EXISTS tag_table (
        tag_id BIGINT IDENTITY(0,1) NOT NULL, 
        count BIGINT,
        name varchar NOT NULL
        );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time_table (
        time_id BIGINT PRIMARY KEY NOT NULL,
        creation_date_ts timestamp NOT NULL, 
        hour int, 
        day int, 
        week int, 
        month int, 
        year int, 
        weekday int
        );
""")

# STAGING TABLES

staging_questions_copy = ("""
    COPY staging_questions 
    FROM {0}
    credentials 'aws_iam_role={1}'
    format as PARQUET;
""").format(config.get('DATA', 'QUESTIONS_DATA_S3'), config.get('IAM_ROLE', 'ARN'))

staging_users_copy = ("""
    COPY staging_users 
    FROM {0}
    credentials 'aws_iam_role={1}'
    format as PARQUET;
""").format(config.get('DATA', 'USERS_DATA_S3'), config.get('IAM_ROLE', 'ARN'))

staging_tags_copy = ("""
    COPY staging_tags
    FROM {0}
    credentials 'aws_iam_role={1}'
    format as PARQUET;
""").format(config.get('DATA', 'TAGS_DATA_S3'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

question_table_insert = ("""
    INSERT INTO question_table (question_id, title, view_count, time_id, user_id, is_answered, tag_id, location_id) 
    SELECT DISTINCT sq.question_id, 
                    sq.title, 
                    sq.view_count, 
                    sq.creation_date, 
                    sq.user_id, 
                    sq.is_answered, 
                    tt.tag_id, 
                    lt.location_id
    FROM staging_questions sq
    INNER JOIN tag_table tt
        ON sq.tags = tt.name
    INNER JOIN staging_users su 
        ON sq.user_id = su.user_id
    INNER JOIN location_table lt
        ON su.location = lt.location
    WHERE 1=1;
""")

user_table_insert = ("""
    INSERT INTO user_table (user_id, display_name, reputation, user_type)
    SELECT DISTINCT su.user_id, 
                    su.display_name, 
                    su.reputation, 
                    su.user_type
    FROM staging_users su
    WHERE 1=1;
""")

location_table_insert = ("""
    INSERT INTO location_table (location) 
    SELECT DISTINCT su.location
    FROM staging_users su
    WHERE 1=1;
""")

tag_table_insert = ("""
    INSERT INTO tag_table (count, name)
    SELECT DISTINCT st.count, 
                    st.name
    FROM staging_tags st
    WHERE 1=1;
""")

time_table_insert = ("""
    INSERT INTO time_table (time_id, creation_date_ts, hour, day, week, month, year, weekday)
    SELECT DISTINCT creation_date,
                    timestamp 'epoch' + creation_date * interval '1 second' as creation_ts,
                    EXTRACT(hour from creation_ts),
                    EXTRACT(day from creation_ts),
                    EXTRACT(week from creation_ts),
                    EXTRACT(month from creation_ts),
                    EXTRACT(year from creation_ts),
                    EXTRACT(weekday from creation_ts)
    FROM staging_questions sq
    WHERE 1=1;
""")

# QUERY LISTS

create_table_queries = [staging_questions_table_create, staging_users_table_create, staging_tags_table_create, question_table_create, user_table_create, location_table_create, tag_table_create, time_table_create]
drop_table_queries = [staging_questions_table_drop, staging_users_table_drop, staging_tags_table_drop, question_table_drop, user_table_drop, location_table_drop, tag_table_drop, time_table_drop]

copy_table_queries = [staging_questions_copy, staging_users_copy, staging_tags_copy]
insert_table_queries = [time_table_insert, tag_table_insert, location_table_insert, user_table_insert, question_table_insert]
