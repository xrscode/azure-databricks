# Access local file:
data "local_file" "ingestion_job_file" {
  filename = "../src/jobs/ingestion.json"
}

# Parse local file:
locals {
  job_definition_transform = jsondecode(data.local_file.ingestion_job_file.content)
}

# Create job from configuration:
resource "databricks_job" "ingestion_job" {
  name                  = local.job_definition_transform.settings.name
  max_concurrent_runs   = local.job_definition_transform.settings.max_concurrent_runs
  timeout_seconds       = local.job_definition_transform.settings.timeout_seconds

  email_notifications {
    no_alert_for_skipped_runs = local.job_definition_transform.settings.email_notifications.no_alert_for_skipped_runs
  }

  # Add webhook_notifications if necessary

  task {
    task_key            = local.job_definition_transform.settings.tasks[0].task_key
    existing_cluster_id = databricks_cluster.cluster.id
    timeout_seconds     = local.job_definition_transform.settings.tasks[0].timeout_seconds

    notebook_task {
      notebook_path    = local.job_definition_transform.settings.tasks[0].notebook_task.notebook_path
      base_parameters  = local.job_definition_transform.settings.tasks[0].notebook_task.base_parameters
    }

    # Add other task-specific fields as necessary

    notification_settings {
      no_alert_for_skipped_runs = local.job_definition_transform.settings.tasks[0].notification_settings.no_alert_for_skipped_runs
      no_alert_for_canceled_runs = local.job_definition_transform.settings.tasks[0].notification_settings.no_alert_for_canceled_runs
      alert_on_last_attempt = local.job_definition_transform.settings.tasks[0].notification_settings.alert_on_last_attempt
    }

    # Add webhook_notifications if necessary
  }

  # Add other job-specific fields as necessary
}


# TRANSFORMATION JOB:
# Access local file:
data "local_file" "transformation_job_file" {
  filename = "../src/jobs/transformation.json"
}

# Parse local file:
locals {
  job_definition_transformation = jsondecode(data.local_file.transformation_job_file.content)
}

# Create job from configuration:
# Create job from configuration:
resource "databricks_job" "transformation_job" {
  name                = local.job_definition_transformation.settings.name
  max_concurrent_runs = local.job_definition_transformation.settings.max_concurrent_runs
  timeout_seconds     = local.job_definition_transformation.settings.timeout_seconds

  email_notifications {
    no_alert_for_skipped_runs = local.job_definition_transformation.settings.email_notifications.no_alert_for_skipped_runs
  }

  # Add webhook_notifications if necessary

  task {
    task_key            = local.job_definition_transformation.settings.tasks[0].task_key
    existing_cluster_id = databricks_cluster.cluster.id
    timeout_seconds     = local.job_definition_transformation.settings.tasks[0].timeout_seconds

    notebook_task {
      notebook_path    = local.job_definition_transformation.settings.tasks[0].notebook_task.notebook_path
      source           = local.job_definition_transformation.settings.tasks[0].notebook_task.source
      
      # Uncomment and edit the following section if base_parameters are required
      # base_parameters = local.job_definition_transformation.settings.tasks[0].notebook_task.base_parameters
    }

    # Add other task-specific fields as necessary

    notification_settings {
      no_alert_for_skipped_runs  = local.job_definition_transformation.settings.tasks[0].notification_settings.no_alert_for_skipped_runs
      no_alert_for_canceled_runs = local.job_definition_transformation.settings.tasks[0].notification_settings.no_alert_for_canceled_runs
      alert_on_last_attempt      = local.job_definition_transformation.settings.tasks[0].notification_settings.alert_on_last_attempt
    }

    # Add webhook_notifications if necessary
  }

  # Add other job-specific fields as necessary
}