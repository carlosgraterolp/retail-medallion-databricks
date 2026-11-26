# Platform resources owned by Terraform. The Declarative Automation Bundle (databricks.yml)
# owns the application resources: the job and the dashboard. Nothing is managed by both.

# --- Service principal for CI/CD ---------------------------------------------------------
# Its OAuth secret is NOT created here: a secret in Terraform would be stored in plain text in
# the state file. It is created with the Databricks CLI and piped straight into GitHub secrets.
resource "databricks_service_principal" "ci" {
  display_name          = var.ci_service_principal_name
  workspace_access      = true
  databricks_sql_access = true
}

# --- Unity Catalog schemas and landing volume ---------------------------------------------
resource "databricks_schema" "this" {
  for_each = var.schemas

  catalog_name = var.catalog
  name         = each.value.name
  comment      = each.value.comment

  lifecycle {
    prevent_destroy = true # holds the pipeline's tables
  }
}

resource "databricks_volume" "landing" {
  catalog_name = var.catalog
  schema_name  = databricks_schema.this["bronze"].name
  name         = var.landing_volume_name
  volume_type  = "MANAGED"
  comment      = "Raw CSV files uploaded with the Databricks CLI"

  lifecycle {
    prevent_destroy = true # holds the raw files
  }
}

# These objects were first created by the bundle; import adopts them without recreating them.
import {
  for_each = var.schemas
  to       = databricks_schema.this[each.key]
  id       = "${var.catalog}.${each.value.name}"
}

import {
  to = databricks_volume.landing
  id = "${var.catalog}.${var.schemas.bronze.name}.${var.landing_volume_name}"
}

# --- Least-privilege grants for the CI service principal ------------------------------------
# databricks_grant manages only this principal's privileges on each securable; it never
# touches grants held by other principals. USE CATALOG on the catalog and CAN USE on the SQL
# warehouse come from the workspace "users" group, which the service principal belongs to.
resource "databricks_grant" "ci_schema" {
  for_each = databricks_schema.this

  schema     = each.value.id
  principal  = databricks_service_principal.ci.application_id
  privileges = ["USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
}

resource "databricks_grant" "ci_landing_volume" {
  volume     = databricks_volume.landing.id
  principal  = databricks_service_principal.ci.application_id
  privileges = ["READ_VOLUME"]
}
