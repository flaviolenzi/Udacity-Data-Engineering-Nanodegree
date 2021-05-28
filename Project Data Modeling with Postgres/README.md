# Project: Data Modeling with Postgres

## Project Overview

This project has the objective of analyzing the data of the startup Sparkify, developer of a music streaming app. Their need is to understand what songs users are listening using JSON logs on user activity and a JSON metadata on the songs. For this Business Intelligence project, we create a Postres database in a star schema and an etl pipeline to extract data from the json files, transform the data and load in the fact and dimension tables.

## Project Datasets

### Song Dataset

The song dataset is a subset of real data from the Million Song Dataset used to load the song and artist dimension tables. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.

Filepath:
```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

Example of what a song file (TRAABJL12903CDCF1A.json) looks like:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

### Log Dataset

The log dataset simulate activity logs from a music streaming app and is used to load the time and user dimensions and the songplays fact table.

The log files in the dataset are partitioned by year and month.

Filepath:
```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

Run this code to look the JSON data within the log files. 
```python
df = pd.read_json('data/log_data/2018/11/2018-11-01-events.json', lines=True) would read the data file 2018-11-01-events.json.
```

## Data Modeling

The modeling created was a star schema to optimize queries on song play analysis.

### Fact Table

songplays - records in log data associated with song plays

| Column | Data Type | Nullable | Constraint |
| ------ | ---- | ---- | ------ |
| `songplay_id` | `SERIAL` | `NO` | `PRIMARY KEY` |
| `start_time` | `TIMESTAMP` | `NO` |  |
| `user_id` | `VARCHAR` | `NO` |  |
| `level` | `VARCHAR` | `NO` |  |
| `song_id` | `VARCHAR` | `YES` |  |
| `artist_id` | `VARCHAR` | `YES` |  |
| `session_id` | `INT` | `YES` |  |
| `location` | `VARCHAR` | `YES` |  |
| `user_agent` | `VARCHAR` | `YES` |  |

### Dimension Tables

users - users in the app

| Column | Data Type | Nullable | Constraint |
| ------ | ---- | ---- | ------ |
| `user_id` | `INT` | `NO` | `PRIMARY KEY` |
| `first_name` | `VARCHAR` | `YES` |  |
| `last_name` | `VARCHAR` | `YES` |  |
| `gender` | `VARCHAR` | `YES` |  |
| `level` | `VARCHAR` | `NO` |  |

songs - songs in music database

| Column | Data Type | Nullable | Constraint |
| ------ | ---- | ---- | ------ |
| `song_id` | `VARCHAR` | `NO` | `PRIMARY KEY` |
| `title` | `VARCHAR` | `YES` |  |
| `artist_id` | `VARCHAR` | `YES` |  |
| `year` | `INT` | `YES` |  |
| `duration` | `DECIMAL` | `YES` |  |

artists - artists in music database

| Column | Data Type | Nullable | Constraint |
| ------ | ---- | ---- | ------ |
| `artist_id` | `VARCHAR` | `NO` | `PRIMARY KEY` |
| `name` | `VARCHAR` | `YES` |  |
| `location` | `VARCHAR` | `YES` |  |
| `latitude` | `DECIMAL` | `YES` |  |
| `longitude` | `DECIMAL` | `YES` |  |

time - timestamps of records in songplays broken down into specific units

| Column | Data Type | Nullable | Constraint |
| ------ | ---- | ---- | ------ |
| `start_time` | `TIMESTAMP` | `NO` | `PRIMARY KEY` |
| `hour` | `INT` | `YES` |  |
| `day` | `INT` | `YES` |  |
| `week` | `INT` | `YES` |  |
| `month` | `INT` | `YES` |  |
| `year` | `INT` | `YES` |  |
| `weekday` | `INT` | `YES` |  |

## Operating instructions

Run the script create_tables.py  in terminal to create the database and tables.

```
python3 create_tables.py
```

Run the script etl.py in terminal to connects to the Sparkify database, extracts and processes the log_data and song_data, and loads data into the five tables (songplays, users ,songs, artists, time).

```
python3 etl.py
```

Use the script test.ipynb to confirm the creation of your tables with the correct columns after running create_tables.py and to confirm that records were successfully inserted into each table after running etl.py.

```
Since this is a subset of the much larger dataset, the solution dataset will only have 1 row with values for value containing ID for both songid and artistid in the fact table. Those are the only 2 values that the query in the sql_queries.py will return that are not-NONE. The rest of the rows will have NONE values for those two variables.
```

## Files Included

In addition to the data files, the project workspace includes six files:

**test.ipynb** - used to test the process of creating and loading the tables.

**create_tables.py** - drops and creates your tables.

**etl.ipynb** - loads data from song_data and log_data into dimensions and fact tables in small scale to test the process.

**etl.py** - loads data from song_data and log_data into dimensions and fact tables.

**sql_queries.py** - contains sql queries to create, load and select the tables.

**README.md** - contains Project Overview, Project Datasets, Data Modeling and Operating instructions.