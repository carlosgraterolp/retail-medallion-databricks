variable "databricks_profile" {
  description = "Databricks CLI profile in ~/.databrickscfg used to authenticate (OAuth)."
  type        = string
}

variable "catalog" {
  description = "Unity Catalog catalog. Free Edition provides \"workspace\"."
  type        = string
  default     = "workspace"
}

variable "schemas" {
  description = "Medallion and marts schemas, keyed by layer."
  type        = map(object({ name = string, comment = string }))
  default = {
    bronze = { name = "retail_bronze", comment = "Raw data as ingested, plus ingestion metadata" }
    silver = { name = "retail_silver", comment = "Cleaned, typed and deduplicated invoice lines; quarantine table" }
    gold   = { name = "retail_gold", comment = "Kimball star schema, aggregates and data quality results" }
    marts  = { name = "retail_marts", comment = "Business marts built by dbt on top of the gold layer" }
  }
}

variable "landing_volume_name" {
  description = "Managed volume in the bronze schema that receives the raw CSV files."
  type        = string
  default     = "landing"
}

variable "ci_service_principal_name" {
  description = "Display name of the service principal used by GitHub Actions."
  type        = string
  default     = "github-ci"
}
