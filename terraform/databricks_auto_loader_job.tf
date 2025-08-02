data "local_file" "databricks_jobs" {
  filename = "../config/${terraform.workspace}/databricks_job.yml"
}
locals {
  job_config = yamldecode(data.local_file.databricks_jobs.content).jobs
}

data "databricks_spark_version" "latest_job_lts" {
  long_term_support = true
}

resource "databricks_job" "terraform_jobs" {
  for_each = {
    for index, job in local.job_config :
    job.name => job
  }

  name                = each.value.name
  max_concurrent_runs = lookup(each.value, "max_concurrent_runs", 1)

  # Set the run_as service principal. By default the Run SPN client id stored in
  # github is passed in and used but it can be overruled from the config yaml. It is
  # not possible to set this to a user account.
  run_as {
    service_principal_name = lookup(each.value, "run_as_service_principal_id", var.run_spn_client_id)
  }

  # set schedule if specified. This is mutually exclusive with 'continuous' and 'trigger'
  dynamic "schedule" {
    for_each = contains(keys(each.value), "schedule") ? [each.value.schedule] : []

    content {
      quartz_cron_expression = schedule.value.quartz_cron_expression
      timezone_id            = lookup(schedule.value, "timezone_id", "UTC")
      pause_status           = lookup(schedule.value, "pause_status", "UNPAUSED")
    }
  }
  # set continuous if specified. This is mutually exclusive with 'schedule' and 'trigger'
  dynamic "continuous" {
    for_each = contains(keys(each.value), "continuous") ? [each.value.continuous] : []

    content {
      pause_status           = lookup(continuous.value, "pause_status", "UNPAUSED")
    }
  }
  # set (file) trigger if specified. This is mutually exclusive with 'schedule' and 'continuous'
  dynamic "trigger" {
    for_each = contains(keys(each.value), "trigger") ? [each.value.trigger] : []

    content {
      pause_status           = lookup(trigger.value, "pause_status", "UNPAUSED")

      file_arrival {
        url = lookup(trigger.value, "file_arrival_url", null)
        min_time_between_triggers_seconds = lookup(trigger.value, "file_arrival_min_time_between_triggers_seconds", null)
        wait_after_last_change_seconds = lookup(trigger.value, "file_arrival_wait_after_last_change_seconds", null)
      }
    }
  }

  # This is the job cluster that will be used to run auto_loader tasks. Change
  # this to reflect the cluster you need.
  job_cluster {
    job_cluster_key = "job_cluster"
    new_cluster {
      spark_version = data.databricks_spark_version.latest_job_lts.id
      spark_conf = {
        "spark.databricks.delta.preview.enabled" : "true"
      }
      azure_attributes {
        first_on_demand    = 1
        availability       = "ON_DEMAND_AZURE"
        spot_bid_max_price = -1
      }
      node_type_id = "Standard_F16s_v2"
      spark_env_vars = {
        PYSPARK_PYTHON : "/databricks/python3/bin/python3"
        ENVIRONMENT: terraform.workspace
        AZ_STORAGE_ACCOUNT: var.az_storage_account
        DBX_UNITY_CATALOG: var.dbx_unity_catalog
        DBX_RAW_SCHEMA: var.dbx_raw_schema
        PROJECT_NAME: var.project_name
      }
      enable_elastic_disk = true
      data_security_mode  = "SINGLE_USER"
      runtime_engine      = "STANDARD"
      autoscale {
        min_workers = 2
        max_workers = 12
      }

      custom_tags={
        for index, tag in lookup(each.value, "cluster_tags", []) :
        (tag.name) => tag.value
      }
    }
  }

  dynamic "notification_settings" {
    for_each = contains(keys(each.value), "notification_settings") ? [each.value.notification_settings] : []
    content {
        no_alert_for_skipped_runs = lookup(notification_settings.value, "no_alert_for_skipped_runs", false)
        no_alert_for_canceled_runs = lookup(notification_settings.value, "no_alert_for_canceled_runs", false)
    }
  }

  # Set job level email notifications, if specified
  dynamic "email_notifications" {
    for_each = contains(keys(each.value), "email_notifications") ? [each.value.email_notifications] : []
    content {
        on_start = lookup(email_notifications.value, "on_start", null)
        on_success = lookup(email_notifications.value, "on_success", null)
        on_failure = lookup(email_notifications.value, "on_failure", null)
        on_duration_warning_threshold_exceeded = lookup(email_notifications.value, "on_duration_warning_threshold_exceeded", null)
        no_alert_for_skipped_runs = lookup(email_notifications.value, "no_alert_for_skipped_runs", null)
    }
  }


  # Here all tasks will be added to the job
  dynamic "task" {
    for_each = { for index, task in each.value.tasks : task.task_key => task }

    content {
      task_key        = task.value.task_key
      job_cluster_key = "job_cluster"
      timeout_seconds = lookup(task.value, "timeout_seconds", 10800)
      run_if          = lookup(task.value, "run_if", "ALL_SUCCESS")
      max_retries     = lookup(task.value, "max_retries", 0)

      # This subsection is specific for auto loader tasks
      python_wheel_task {
        package_name = lookup(task.value, "package_name", "edp_auto_loader")
        entry_point  = lookup(task.value, "entry_point", "run")
        parameters   = lookup(task.value, "parameters", [])
      }
      library {
        whl = "/Workspace/Shared/edp_auto_loader/edp_auto_loader-${var.auto_loader_wheel_version}-py3-none-any.whl"
      }
      dynamic "depends_on" {
        for_each = contains(keys(task.value), "depends_on") ? task.value.depends_on : []

        content {
          task_key = depends_on.value
        }
      }

      # Set task level email notifications, if specified
      dynamic "email_notifications" {
       for_each = contains(keys(task.value), "email_notifications") ? [task.value.email_notifications] : []
       content {
           on_start = lookup(email_notifications.value, "on_start", null)
           on_success = lookup(email_notifications.value, "on_success", null)
           on_failure = lookup(email_notifications.value, "on_failure", null)
           on_duration_warning_threshold_exceeded = lookup(email_notifications.value, "on_duration_warning_threshold_exceeded", null)
        }
      }
    }
  }

  # If the job has a tags node, all of it its members should be in the form "key:value"
  # This is where the strings are split and added as tag to the job
  tags = {
    for index, tag in lookup(each.value, "tags", ["creator:terraform"]) :
    trim(split(":", tag)[0], " ") => trim(split(":", tag)[1], " ")
  }
}
