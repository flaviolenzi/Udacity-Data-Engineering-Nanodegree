# Project: Data Lake

## Project Overview

This project has the objective of analyzing the data of the startup Sparkify, developer of a music streaming app. Their data is located on S3 in JSON format. For this project, we created an ETL pipeline to load data from S3, process the data into analytics tables using Spark, and load them back into S3, using a cluster in AWS to deploy the spark process.

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


### Fact Table

songplays - records in log data associated with song plays

| Column | Data Type | Nullable |
| ------ | ---- | ---- | 
| `songplay_id` | `long` | `true` |
| `start_time` | `timestamp` | `true` | 
| `user_id` | `string` | `true` | 
| `level` | `string` | `true` | 
| `song_id` | `string` | `true` | 
| `artist_id` | `string` | `true` | 
| `session_id` | `long` | `true` | 
| `location` | `string` | `true` | 
| `user_agent` | `string` | `true` | 
| `month` | `integer` | `true` | 
| `year` | `integer` | `true` | 

### Dimension Tables

users - users in the app

| Column | Data Type | Nullable | 
| ------ | ---- | ---- | 
| `user_id` | `string` | `true` | 
| `first_name` | `string ` | `true` | 
| `last_name` | `string ` | `true` | 
| `gender` | `string ` | `true` | 
| `level` | `string ` | `true` | 

songs - songs in music database

| Column | Data Type | Nullable | 
| ------ | ---- | ---- | 
| `song_id` | `string ` | `true` |
| `title` | `string ` | `true` | 
| `artist_id` | `string ` | `true` | 
| `year` | `long ` | `true ` | 
| `duration` | `double ` | `true` | 

artists - artists in music database

| Column | Data Type | Nullable |
| ------ | ---- | ---- |
| `artist_id` | `string ` | `true` | 
| `name` | `string ` | `true` | 
| `location` | `string ` | `true` | 
| `latitude` | `double` | `true` | 
| `longitude` | `double` | `true` | 

time - timestamps of records in songplays broken down into specific units

| Column | Data Type | Nullable | 
| ------ | ---- | ---- | 
| `start_time` | `timestamp` | `true` | 
| `hour` | `integer` | `true` | 
| `day` | `integer` | `true` | 
| `week` | `integer` | `true` | 
| `month` | `integer` | `true` | 
| `year` | `integer` | `true` | 
| `weekday` | `integer` | `true` | 

## Operating instructions

Run the script etl.py in terminal to read data from S3, processes that data using Spark, and writes them back to S3

```
python3 etl.py
```

Edit the file `dl.cfg` in the same folder as this notebook and inform your AWS credentials


## Files Included

In addition to the data files, the project workspace includes six files:

**create_services.ipynb** - read data from S3, processes that data using Spark, and writes them back to S3, step by step

**dl.cfg** - contains information about AWS credentials.

**etl.py** - read data from S3, processes that data using Spark, and writes them back to S3

**README.md** - contains Project Overview, Project Datasets, Data Modeling and Operating instructions.