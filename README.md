# Sparkify Data Pipelines
## Project 5 for Udacity Data Engineering Nano Degree

This project contains airflow DAG and plugins to load log and songs data
from s3 buckets and loads them in a DataWarehouse in a AWS Redshift instance.

### Build & Configuration

#### Database
aws_utils.py has utilities to create a Redshift instance.
After instance is created, run create_tables.sql in order to create the needed tables.

#### Airflow Connections
The below connections need to be setup in the airflow instance:
**redshift_sparkify** - Postgres connection pointing to your Redshift cluster/database.

**credentials** - AWS connection to configure key/secret

#### Airflow Variables
**log_bucket**  - Bucket containing sparkify log files, usually: s3://udacity-dend/log_data

**song_bucket** - Bucket containing json song data files, usually: s3://udacity-dend/song_data

#### Pipeline Setup
Place dags and plugins under airflow installation directory.

### Tasks
**udacity_dend_data_pipelines** dag has four types of tasks, each one has an implemented operator

#### Staging Tasks
Operator *StageToRedshiftOperator* - Copy data from S3 to staging tables,
optional argument s3_date in format yyyy/mm/yyyymmdd is used to process files for a particular day.

#### Fact Tasks
Operator *LoadFactOperator* - Loads data from a query, present in helpers/sql_queries
and loads in the table received as parameter.

#### Dimension Tables
Operator *LoadDimensionOperator* - Loads data from a query, present in helpers/sql_queries
and loads in the table received as parameter. Parameter truncate_table is received, set to false in incremental tables
users and time, set to true in tables where full data is processed each execution, tables songs and artists.

#### Data Quality

Operator *DataQualityOperator* - Validates a list of tables based on a minimum records threshold, 
received as parameter *validation_threshold*.