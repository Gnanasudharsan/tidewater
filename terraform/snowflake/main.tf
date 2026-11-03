# Snowflake objects for the week-9 benchmark: one X-Small warehouse that suspends
# after 60 s idle, a database with the same schema layout as the Postgres warehouse,
# least-privilege roles, a key-pair service user, and a resource monitor so the
# trial can never run away with credits.

provider "snowflake" {
  role = "ACCOUNTADMIN"
  # Key-pair login for the TERRAFORM_ADMIN service user (works with SSO-only accounts:
  # no password anywhere). Falls back to SNOWFLAKE_PASSWORD if no key path is given.
  authenticator = var.private_key_path == "" ? null : "SNOWFLAKE_JWT"
  private_key   = var.private_key_path == "" ? null : file(var.private_key_path)
}

resource "snowflake_resource_monitor" "tidewater" {
  name            = "TIDEWATER_MONITOR"
  credit_quota    = var.monthly_credit_quota
  frequency       = "MONTHLY"
  start_timestamp = "IMMEDIATELY"
  notify_triggers = [50, 80]
  suspend_trigger = 100
}

resource "snowflake_warehouse" "tidewater" {
  name                = "TIDEWATER_WH"
  warehouse_size      = "XSMALL"
  auto_suspend        = 60
  auto_resume         = "true"
  initially_suspended = true
  resource_monitor    = snowflake_resource_monitor.tidewater.name
  comment             = "Tidewater benchmarks (X-Small, 60 s auto-suspend)"
}

resource "snowflake_database" "tidewater" {
  name    = "TIDEWATER"
  comment = "Tidewater lakehouse benchmark target"
}

resource "snowflake_schema" "layer" {
  for_each = toset(["SILVER", "STAGING", "MARTS", "SNAPSHOTS"])
  database = snowflake_database.tidewater.name
  name     = each.key
}

resource "snowflake_account_role" "loader" {
  name    = "TIDEWATER_LOADER"
  comment = "Writes silver tables"
}

resource "snowflake_account_role" "transformer" {
  name    = "TIDEWATER_TRANSFORMER"
  comment = "Runs dbt: reads silver, writes staging/marts/snapshots"
}

resource "snowflake_grant_privileges_to_account_role" "warehouse_usage" {
  for_each          = toset([snowflake_account_role.loader.name, snowflake_account_role.transformer.name])
  account_role_name = each.key
  privileges        = ["USAGE", "OPERATE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.tidewater.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "database_usage" {
  for_each          = toset([snowflake_account_role.loader.name, snowflake_account_role.transformer.name])
  account_role_name = each.key
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.tidewater.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_silver" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE STAGE"]
  on_schema {
    schema_name = "\"${snowflake_database.tidewater.name}\".\"${snowflake_schema.layer["SILVER"].name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_schemas" {
  for_each          = toset(["STAGING", "MARTS", "SNAPSHOTS"])
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  on_schema {
    schema_name = "\"${snowflake_database.tidewater.name}\".\"${snowflake_schema.layer[each.key].name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_reads_silver" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = "\"${snowflake_database.tidewater.name}\".\"${snowflake_schema.layer["SILVER"].name}\""
  }
}

# The cost mart reads SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY.
resource "snowflake_grant_privileges_to_account_role" "transformer_account_usage" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["IMPORTED PRIVILEGES"]
  on_account_object {
    object_type = "DATABASE"
    object_name = "SNOWFLAKE"
  }
}

resource "snowflake_user" "service" {
  name              = "SVC_TIDEWATER"
  login_name        = "SVC_TIDEWATER"
  comment           = "Tidewater pipeline (key-pair auth only)"
  default_warehouse = snowflake_warehouse.tidewater.name
  default_role      = snowflake_account_role.loader.name
  rsa_public_key    = var.service_user_rsa_public_key
}

resource "snowflake_grant_account_role" "service_roles" {
  for_each  = toset([snowflake_account_role.loader.name, snowflake_account_role.transformer.name])
  role_name = each.key
  user_name = snowflake_user.service.name
}

# The loader owns the SILVER tables it creates; dbt (transformer) must be able to read
# every one of them, including tables created after this apply.
resource "snowflake_grant_privileges_to_account_role" "transformer_selects_silver" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.tidewater.name}\".\"${snowflake_schema.layer["SILVER"].name}\""
    }
  }
}
