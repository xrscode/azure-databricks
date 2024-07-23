# Access local file:
data "local_file" "ingestion_job_file" {
  filename = "../src/jobs/ingestion.json"
}

# Parse local file:
locals {
  job_definition = jsondecode(data.local_file.ingestion_job_file.content)
}

# Create job from configuration:
resource "databricks_job" "ingestion_job" {
  name                  = local.job_definition.settings.name
  max_concurrent_runs   = local.job_definition.settings.max_concurrent_runs
  timeout_seconds       = local.job_definition.settings.timeout_seconds

  email_notifications {
    no_alert_for_skipped_runs = local.job_definition.settings.email_notifications.no_alert_for_skipped_runs
  }

  # Add webhook_notifications if necessary

  task {
    task_key            = local.job_definition.settings.tasks[0].task_key
    existing_cluster_id = databricks_cluster.cluster.id
    timeout_seconds     = local.job_definition.settings.tasks[0].timeout_seconds

    notebook_task {
      notebook_path    = local.job_definition.settings.tasks[0].notebook_task.notebook_path
      base_parameters  = local.job_definition.settings.tasks[0].notebook_task.base_parameters
    }

    # Add other task-specific fields as necessary

    notification_settings {
      no_alert_for_skipped_runs = local.job_definition.settings.tasks[0].notification_settings.no_alert_for_skipped_runs
      no_alert_for_canceled_runs = local.job_definition.settings.tasks[0].notification_settings.no_alert_for_canceled_runs
      alert_on_last_attempt = local.job_definition.settings.tasks[0].notification_settings.alert_on_last_attempt
    }

    # Add webhook_notifications if necessary
  }

  # Add other job-specific fields as necessary
}