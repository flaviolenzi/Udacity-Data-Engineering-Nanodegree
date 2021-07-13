# Project: Data Modeling with Postgres

## Project Overview

This project has the objective of analyzing the data of the startup Sparkify, developer of a music streaming app. Since their user and song database growth, they want to move their process to AWS. Their data is located on S3 in JSON format. For this project, we created an ETL pipeline to extract the data from S3 into staging tables on Redshift, and transform that data into analytic tables so they can continue finding valuable insights.

## Project Datasets

For this project, we'll be using two datasets located in amazon S3.

### Song Dataset

The song dataset is a subset of real data from the Million Song Dataset used to load the song and artist dimension tables. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.

Song data:
```
s3://udacity-dend/song_data
```

Example of what a song file looks like:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

### Log Dataset

The log dataset simulate activity logs from a music streaming app and is used to load the time and user dimensions and the songplays fact table.

The log files in the dataset are partitioned by year and month.

Log Data:
```
s3://udacity-dend/log_data
```

Log data json path
```
s3://udacity-dend/log_json_path.json
```

## Data Modeling

The modeling created was a star schema to optimize queries on song play analysis.

### Staging Tables

staging_events - records in log dataset associated with app activity log

| Column | Data Type | Nullable | Constraint |
| ------ | ---- | ---- | ------ |
| `event_id` | `INT` | `YES` | `IDENTITY(0,1)` |
| `artist` | `VARCHAR` | `YES` |  |
| `auth` | `VARCHAR` | `YES` |  |
| `firstName` | `VARCHAR` | `YES` |  |
| `gender` | `VARCHAR` | `YES` |  |
| `itemInSession` | `INT` | `YES` |  |
| `lastName` | `VARCHAR` | `YES` |  |
| `length` | `DECIMAL` | `YES` |  |
| `level` | `VARCHAR` | `YES` |  |
| `location` | `VARCHAR` | `YES` |  |
| `method` | `VARCHAR` | `YES` |  |
| `page` | `VARCHAR` | `YES` |  |
| `registration` | `VARCHAR` | `NO` |  |
| `sessionId` | `INT` | `YES` |  |
| `song` | `VARCHAR` | `YES` |  |
| `status` | `INT` | `YES` |  |
| `ts` | `TIMESTAMP` | `YES` |  |
| `userAgent` | `VARCHAR` | `YES` |  |
| `userId` | `INT` | `YES` |  |
        
staging_songs - records in song dataset from the Million Song Dataset

| Column | Data Type | Nullable | Constraint |
| ------ | ---- | ---- | ------ |
| `num_songs` | `INT` | `YES` |  |
| `artist_id` | `VARCHAR` | `YES` |  |
| `artist_latitude` | `DECIMAL` | `YES` |  |
| `artist_longitude` | `DECIMAL` | `YES` |  |
| `artist_location` | `VARCHAR` | `YES` |  |
| `artist_name` | `VARCHAR` | `YES` |  |
| `title` | `VARCHAR` | `YES` |  |
| `duration` | `DECIMAL` | `YES` |  |
| `year` | `INT` | `YES` |  |


### Fact Table

songplays - records in log data associated with song plays

| Column | Data Type | Nullable | Constraint |
| ------ | ---- | ---- | ------ |
| `songplay_id` | `INT` | `NO` | `IDENTITY(0,1)` |
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

Run the script etl.py in terminal to load data from Amazon S3 into staging tables on Redshift and the process the data into the Fact and Dimension tables also located on Redshift

```
python3 etl.py
```

Use the script create_services.ipynb to create the services EC2, IAM and Redshift in AWS. 

Edit the file `dwh.cfg` in the same folder as this notebook and inform your AWS secret and access key and information about database and IAM user (this must have `AdministratorAccess`, from `Attach existing policies directly` tab).


## Files Included

In addition to the data files, the project workspace includes six files:

**create_services.ipynb** - used to create the services EC2, IAM and Redshift in AWS and to test the data in the staging, fact and dimension tables.

**create_tables.py** - drops and creates your tables in Redshift.

**dwh.cfg** - contains information about AWS secret and access key, IAM Role, S3 (source data) and Redshift Cluster.

**etl.py** - loads data from song_data and log_data into staging tables and then into dimensions and fact tables.

**sql_queries.py** - contains sql queries to create/delete al tables, load from S3 to staging tables in Redshift and load from staging tables in Redshift to analytic tables in Redshift.

**README.md** - contains Project Overview, Project Datasets, Data Modeling and Operating instructions.