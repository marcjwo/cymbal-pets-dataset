provider "google" {
  project = var.project_id
  region  = var.location
}

module "google_project_service" {
  source     = "terraform-google-modules/project-factory/google//modules/project_services"
  version    = "~> 15.0"
  project_id = var.project_id
  activate_apis = [
    # "aiplatform.googleapis.com",
    "bigquery.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "cloudscheduler.googleapis.com",
    # "cloudfunctions.googleapis.com",
    # "run.googleapis.com",
    "serviceusage.googleapis.com",
    # "storage.googleapis.com"
  ]
  disable_services_on_destroy = false
}

resource "time_sleep" "wait_after_apis_activate" {
  depends_on      = [module.google_project_service]
  create_duration = "60s"
}

module "bigquery" {
  source            = "./bigquery"
  dataset_id        = var.dataset_id
  project_id        = var.project_id
  table_ids         = var.table_ids
  partition_columns = var.partition_columns
  cluster_columns   = var.cluster_columns
  depends_on        = [time_sleep.wait_after_apis_activate]
}

module "cloud_function" {
  source     = "./cloud_function"
  project_id = var.project_id
  dataset_id = var.dataset_id
  depends_on = [time_sleep.wait_after_apis_activate]
}
