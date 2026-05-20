USE DATABASE AIRLINE_DW;
USE SCHEMA TELEMETRY;

-- 1. Create the IAM Storage Integration
CREATE OR REPLACE STORAGE INTEGRATION s3_airline_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = '{{AWS_IAM_ROLE_ARN}}'
    STORAGE_ALLOWED_LOCATIONS = ('{{AWS_S3_BRONZE_URL}}');

-- 2. Define the File Format
CREATE OR REPLACE FILE FORMAT parquet_format
    TYPE = PARQUET
    COMPRESSION = SNAPPY;

-- 3. Create the External Stage
CREATE OR REPLACE STAGE s3_bronze_stage
    STORAGE_INTEGRATION = s3_airline_integration
    URL = '{{AWS_S3_BRONZE_URL}}'
    FILE_FORMAT = parquet_format;