terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.47.0"
    }
  }

  # Use the 'backend' attribute instead of the 'cloud' attribute to be able to
  # use the prefix property of workspaces. This way, a workspace per environment
  # can be used.
  backend "remote" {
    hostname = "app.terraform.io"
    organization = "ecp-shell-prod"

    workspaces {
      prefix = "data-autoloader-"
    }
  }
}


provider "databricks" {}
