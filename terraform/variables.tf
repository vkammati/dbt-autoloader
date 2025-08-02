variable "auto_loader_wheel_version" {
  description = "The version of the Auto Loader wheel the workflow should use (x.x.x)"
  type        = string
}

variable "az_storage_account" {
  description = "Azure storage account"
  type        = string
}

variable "dbx_unity_catalog" {
  description = "Databricks Unity catalog name"
  type        = string
}

variable "dbx_raw_schema" {
  description = "Databricks Unity catalog name of the schema to write the data to"
  type        = string
}

variable "project_name" {
  description = "Project/Application name within landing container to read from. Will also be used to determine path on raw to write to in case external tables are used."
  type        = string
}

variable "run_spn_tenant_id" {
  description = "Tenant ID of the spn used as 'run as' user of the workflow"
  type        = string
}

variable "run_spn_client_id" {
  description = "Client ID of the spn used as 'run as' user of the workflow"
  type        = string
}

variable "run_spn_client_secret" {
  description = "Client secret of the spn used as 'run as' user of the workflow"
  type        = string
}
