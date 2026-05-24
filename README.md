# Hudi-ETL
A Hudi ETL project that build data pipelines to process data from bronze to silver zone in medallion architect. The bronze zone is a S3 bucket named 'staging' in which raw data synchronized using Oracle Golden Gate are stored. The silver zone refer to a bucket called 'warehouse' store the data that are transformed and cleansed.

## Introduction

- This project contains scripts, jobs, pipelines to process and transform datas that are streamed by Oracle Golden Gate and then load in to Hudi's format type reside on MinIO or any S3 compatible system.


## Description

- src: Main source codes
- src/configs: Contain config settings and CCP's SM settings
- src/dags: Contain Airflow DAGs codes
- src/helpers: Contain utilities for scripts
- src/scripts: Contain logical handler for each data source.
- src/scripts/hash: Contain handler to build and apply incremental updates on mapped tables. (No longer used)
- src/scripts/live: Contain handler to process source tables.
- .env: Represent environment variables.

## Step to Deploy

- No build stage, deploy via Scan and Deploy stage from Git CI/CD pipeline.
- Deploy path is followed by path naming convention of the pipeline.

## Testing

- MUST HAVE: Virtual environment
    Install some packages:
    
        pip install "apache-airflow[celery]==2.5.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.3/constraints-3.7.txt"
        pip install psycopg2-binary