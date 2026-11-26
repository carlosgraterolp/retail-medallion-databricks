output "ci_service_principal_id" {
  description = "Numeric workspace ID of the CI service principal (used to create its OAuth secret)."
  value       = databricks_service_principal.ci.id
}

output "ci_service_principal_application_id" {
  description = "OAuth client ID of the CI service principal (stored as the DATABRICKS_CLIENT_ID GitHub secret)."
  value       = databricks_service_principal.ci.application_id
}

output "schemas" {
  description = "Fully qualified schema names."
  value       = { for k, s in databricks_schema.this : k => s.id }
}

output "landing_volume_path" {
  description = "Path of the landing volume."
  value       = "/Volumes/${var.catalog}/${var.schemas.bronze.name}/${var.landing_volume_name}"
}
