variable "snowflake_account" {
  type = string
}

variable "snowflake_user" {
  type = string
}

variable "snowflake_password" {
  type      = string
  sensitive = true
}

variable "aws_iam_role_arn" {
  type = string
}

variable "s3_bronze_url" {
  type = string
}