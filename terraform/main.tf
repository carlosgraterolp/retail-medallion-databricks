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

# Developer schema for local dbt runs (profiles.example.yml). The CI service principal gets no
# grants here, and the objects in it are owned by whoever builds them locally. No
# prevent_destroy: everything in it can be rebuilt with `dbt build`.
resource "databricks_schema" "marts_dev" {
  catalog_name = var.catalog
  name         = var.marts_dev_schema_name
  comment      = "Local dbt development output (not used by the pipeline or the dashboard)"
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

# --- Incremental ingestion: Auto Loader / Structured Streaming checkpoints ------------------
# One folder per stream inside this volume; only the CI service principal writes here.
resource "databricks_volume" "checkpoints" {
  catalog_name = var.catalog
  schema_name  = databricks_schema.this["bronze"].name
  name         = "checkpoints"
  volume_type  = "MANAGED"
  comment      = "Auto Loader and streaming checkpoints and schema locations (prod)"
}

resource "databricks_grant" "ci_checkpoints" {
  volume     = databricks_volume.checkpoints.id
  principal  = databricks_service_principal.ci.application_id
  privileges = ["READ_VOLUME", "WRITE_VOLUME"]
}

# --- Development environment -----------------------------------------------------------------
# The bundle's dev target writes to these *_dev schemas (owned by the developer), so a dev run
# never needs write access to tables owned by the CI service principal. No CI grants here.
resource "databricks_schema" "dev" {
  for_each = { for k, s in var.schemas : k => s if k != "marts" }

  catalog_name = var.catalog
  name         = "${each.value.name}_dev"
  comment      = "Development copy: ${each.value.comment}"
}

resource "databricks_volume" "checkpoints_dev" {
  catalog_name = var.catalog
  schema_name  = databricks_schema.dev["bronze"].name
  name         = "checkpoints"
  volume_type  = "MANAGED"
  comment      = "Auto Loader and streaming checkpoints for the dev target"
}

# --- Read-only access to prod for the developer ------------------------------------------------
# Objects recreated by the pipeline are owned by the CI service principal, and owning a schema
# does not give access to objects owned by others. SELECT on the schema applies to all current
# and future tables in it, whoever owns them.
# Unity Catalog on this workspace accepts neither the workspace "admins" group, nor the
# system-generated admins group, nor a group created through the workspace API (it stays
# workspace-local; account groups need the account console, which Free Edition does not have).
# So the grant goes to the person running Terraform, read at plan time: no user name in the code.
data "databricks_current_user" "me" {}

resource "databricks_grant" "developer_read_prod" {
  for_each = databricks_schema.this

  schema     = each.value.id
  principal  = data.databricks_current_user.me.user_name
  privileges = ["USE_SCHEMA", "SELECT"]
}
