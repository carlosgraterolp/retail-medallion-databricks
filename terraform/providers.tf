# Workspace-level provider. Authentication comes from a Databricks CLI profile
# (OAuth U2M via `databricks auth login`), so no host, token or secret is in this code.
provider "databricks" {
  profile = var.databricks_profile
}
