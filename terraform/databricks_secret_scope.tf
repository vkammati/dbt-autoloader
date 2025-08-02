# This resource creates a secret scope for EDP Auto Loader
resource "databricks_secret_scope" "this" {
  name                     = "${var.project_name}_${terraform.workspace}_edp_auto_loader"
  # initial_manage_principal = "users"
  backend_type             = "DATABRICKS"
}

# This resource grants READ permission to the Run SPN on the secret scope
resource "databricks_secret_acl" "secret_scope_read_run_spn" {
  principal  = var.run_spn_client_id
  permission = "READ"
  scope      = databricks_secret_scope.this.id
}

# This resource creates a secret for the Azure Run SPN Tenant ID
resource "databricks_secret" "az_run_spn_tenant_id" {
  key          = "AZ-RUN-SPN-TENANT-ID"
  string_value = var.run_spn_tenant_id
  scope        = databricks_secret_scope.this.id
}

# This resource creates a secret for the Azure Run SPN Client ID
resource "databricks_secret" "az_run_spn_client_id" {
  key          = "AZ-RUN-SPN-CLIENT-ID"
  string_value = var.run_spn_client_id
  scope        = databricks_secret_scope.this.id
}

# This resource creates a secret for the Azure Run SPN Client Secret
resource "databricks_secret" "az_run_spn_client_secret" {
  key          = "AZ-RUN-SPN-CLIENT-SECRET"
  string_value = var.run_spn_client_secret
  scope        = databricks_secret_scope.this.id
}
