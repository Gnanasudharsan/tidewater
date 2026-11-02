output "warehouse" { value = snowflake_warehouse.tidewater.name }
output "database" { value = snowflake_database.tidewater.name }
output "service_user" { value = snowflake_user.service.name }
