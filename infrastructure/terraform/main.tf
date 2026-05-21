# 1. Ensure the Database exists
resource "snowflake_database" "airline_dw" {
  name                        = "AIRLINE_DW"
  data_retention_time_in_days = 1
}

# 2. Ensure the Schema exists
resource "snowflake_schema" "telemetry" {
  database            = snowflake_database.airline_dw.name
  name                = "TELEMETRY"
  data_retention_days = 1
}

# 3. Create the IAM Storage Integration (No changes needed here)
resource "snowflake_storage_integration" "s3_airline_integration" {
  name    = "S3_AIRLINE_INTEGRATION"
  type    = "EXTERNAL_STAGE"
  enabled = true

  storage_provider          = "S3"
  storage_aws_role_arn      = var.aws_iam_role_arn
  storage_allowed_locations = [var.s3_bronze_url]
}

# 4. Define the File Format
resource "snowflake_file_format" "parquet_format" {
  name           = "PARQUET_FORMAT"
  database       = snowflake_database.airline_dw.name
  schema         = snowflake_schema.telemetry.name
  format_type    = "PARQUET"
  compression    = "SNAPPY"
  binary_as_text = true
}

# 5. Create the External Stage
resource "snowflake_stage" "s3_bronze_stage" {
  name                = "S3_BRONZE_STAGE"
  database            = snowflake_database.airline_dw.name
  schema              = snowflake_schema.telemetry.name
  url                 = var.s3_bronze_url
  storage_integration = snowflake_storage_integration.s3_airline_integration.name
  
  # Updated: Fully qualified path so Snowflake never loses it
  file_format         = "FORMAT_NAME = ${snowflake_database.airline_dw.name}.${snowflake_schema.telemetry.name}.${snowflake_file_format.parquet_format.name}"
}