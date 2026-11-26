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
  value = merge(
    { for k, s in databricks_schema.this : k => s.id },
    { for k, s in databricks_schema.dev : "${k}_dev" => s.id },
    { marts_dev = databricks_schema.marts_dev.id },
  )
}

output "landing_volume_path" {
  description = "Path of the landing volume."
  value       = "/Volumes/${var.catalog}/${var.schemas.bronze.name}/${var.landing_volume_name}"
}

output "checkpoints_volume_paths" {
  description = "Volumes holding streaming checkpoints and Auto Loader schema locations."
  value = {
    prod = "/Volumes/${var.catalog}/${var.schemas.bronze.name}/${databricks_volume.checkpoints.name}"
    dev  = "/Volumes/${var.catalog}/${databricks_schema.dev["bronze"].name}/${databricks_volume.checkpoints_dev.name}"
  }
}
