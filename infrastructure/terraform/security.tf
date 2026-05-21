# 1. Create the Dedicated Account Role
resource "snowflake_role" "dbt_prod_role" {
  name    = "DBT_PROD_ROLE"
  comment = "Role used by GitHub Actions to run dbt models in production"
}

# 2. Grant the Role to SYSADMIN
resource "snowflake_grant_account_role" "dbt_role_to_sysadmin" {
  role_name        = snowflake_role.dbt_prod_role.name
  parent_role_name = "SYSADMIN"
}

# 3. Grant Warehouse Access
resource "snowflake_grant_privileges_to_account_role" "dbt_wh_usage" {
  privileges        = ["USAGE","OPERATE"]
  account_role_name = snowflake_role.dbt_prod_role.name
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = "COMPUTE_WH" # Change if your warehouse has a different name
  }
}

# 4. Grant Database & Schema Access
resource "snowflake_grant_privileges_to_account_role" "dbt_db_usage" {
  privileges        = ["USAGE", "CREATE SCHEMA"]
  account_role_name = snowflake_role.dbt_prod_role.name
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.airline_dw.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_schema_usage" {
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  account_role_name = snowflake_role.dbt_prod_role.name
  on_schema {
    schema_name = "\"${snowflake_database.airline_dw.name}\".\"${snowflake_schema.telemetry.name}\""
  }
}

# 5. Grant Access to Existing and Future Tables/Views
resource "snowflake_grant_privileges_to_account_role" "dbt_table_grants" {
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]
  account_role_name = snowflake_role.dbt_prod_role.name
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.airline_dw.name}\".\"${snowflake_schema.telemetry.name}\""
    }
  }
}

# 6. Generate a Secure Password for the Bot
resource "random_password" "svc_password" {
  length           = 24
  special          = true
  override_special = "!@#$%^&*"
}

# 7. Create the Service User
resource "snowflake_user" "svc_github_actions" {
  name         = "SVC_GITHUB_ACTIONS"
  password     = random_password.svc_password.result
  default_role = snowflake_role.dbt_prod_role.name
  default_warehouse = "COMPUTE_WH"
  comment      = "Service account for GitHub Actions CI/CD"
}

# 8. Grant the Role to the Service User
resource "snowflake_grant_account_role" "dbt_role_to_svc_user" {
  role_name = snowflake_role.dbt_prod_role.name
  user_name = snowflake_user.svc_github_actions.name
}

# 9. Output the Password (so you can copy it to GitHub)
output "svc_github_actions_password" {
  value     = random_password.svc_password.result
  sensitive = true
}