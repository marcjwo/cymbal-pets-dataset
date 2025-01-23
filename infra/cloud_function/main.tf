resource "random_id" "default" {
  byte_length = 4
}

data "archive_file" "default" {
  type = "zip"
  # source_file = "${path.module}/../../script/main.py"
  output_path = "tmp/gcf_source.zip"
  source {
    content  = file("${path.module}/../../script/main.py")
    filename = "main.py"
  }
  source {
    content  = file("${path.module}/../../script/requirements.txt")
    filename = "requirements.txt"
  }
}

# data "archive_file" "data" {
#   type        = "zip"
#   source_file = "../../data/*.json"
#   output_path = "tmp/gcf_data.zip"
# }

resource "google_storage_bucket" "gcf-source-bucket" {
  name                        = "${random_id.default.hex}-${var.project_id}-pets-gcf-source-bucket"
  location                    = var.location
  uniform_bucket_level_access = true
  depends_on                  = [random_id.default]
  force_destroy               = true
}


resource "google_storage_bucket_object" "gcf-source-object" {
  name   = "gcf-source.zip"
  bucket = google_storage_bucket.gcf-source-bucket.name
  source = data.archive_file.default.output_path
}

resource "google_storage_bucket" "gcf-data-bucket" {
  name                        = "${random_id.default.hex}-${var.project_id}-pets-gcf-source-data-bucket"
  location                    = var.location
  uniform_bucket_level_access = true
  depends_on                  = [random_id.default]
  force_destroy               = true
}

resource "google_storage_bucket_object" "gcf-source-data-object" {
  for_each = toset(["products_data.json", "stores_data.json", "suppliers_data.json"])
  name     = "data/${each.value}"
  bucket   = google_storage_bucket.gcf-data-bucket.name
  source   = "${path.module}/../../data/${each.value}"
}

resource "google_service_account" "account" {
  account_id   = "gcf-sa-${random_id.default.hex}"
  display_name = "Service Account to be used for invoking Cloud Functions"
}

resource "google_project_iam_member" "invoking" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.account.email}"
}

resource "google_cloudfunctions2_function" "default" {
  project     = var.project_id
  location    = var.region
  name        = "cymbal_pets_generator_function"
  description = "Python function to create cymbal_pets dataset"
  build_config {
    runtime     = "python312"
    entry_point = "hello_http"
    environment_variables = {
      SOURCE_HASH = data.archive_file.default.output_sha
    }
    source {
      storage_source {
        bucket = google_storage_bucket.gcf-source-bucket.name
        object = google_storage_bucket_object.gcf-source-object.name
      }
    }
  }
  service_config {
    max_instance_count = 2
    min_instance_count = 0
    available_memory   = "8Gi"
    timeout_seconds    = 3600
    available_cpu      = "6"
    environment_variables = {
      DATASET_ID   = var.dataset_id
      BUCKET_NAME  = google_storage_bucket.gcf-data-bucket.name
      DAILY_ORDERS = var.daily_orders
      # MIN_LOCATIONS    = var.min_locations
      # MAX_LOCATIONS    = var.max_locations
      NUM_OF_CUSTOMERS = var.num_of_customers
      START_DATE       = var.start_date
    }
  }
  depends_on = [google_storage_bucket.gcf-data-bucket, google_storage_bucket.gcf-source-bucket]
}

resource "google_cloud_scheduler_job" "default" {
  project     = var.project_id
  name        = "cymbal_pets_generator_schedule_job"
  description = "Schedule job to trigger cymbal pets generation"
  schedule    = "0 5 * * *"
  retry_config {
    retry_count = 1
  }
  http_target {
    http_method = "POST"
    uri         = google_cloudfunctions2_function.default.url
    oidc_token {
      service_account_email = google_service_account.account.email
      audience              = google_cloudfunctions2_function.default.url
    }
  }
  depends_on = [google_cloudfunctions2_function.default]
}
