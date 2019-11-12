terraform {
  backend "gcs" {
    bucket = "tf-state-gcp-imdb-processing"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = "matsf-cloud-graph"
  region = "us-central1"
}

variable "project_number" {
  default = "251342811543"
}

variable "backend_name" {
  default = "graph-backend"
}

variable "input_name" {
  default = "graph-input"
}

variable "config_files" {
  default = "../config/imdb-urls.tsv"
}


# Not enough rights for the build user. Enabled manually
#resource "google_project_service" "project" {
#  service = "storagetransfer.googleapis.com"
#}

resource "google_storage_bucket" "backend-bucket" {
  name = var.backend_name
  storage_class = "REGIONAL"
  location = "us-central1"
}

resource "google_storage_bucket_object" "imdbconfig" {
  name = var.config_files
  bucket = google_storage_bucket.backend-bucket.name
  source = var.config_files
  depends_on = [google_storage_bucket.backend-bucket]
}

resource "google_storage_object_acl" "imdbconfig_acl" {
  bucket = google_storage_bucket.backend-bucket.name
  object = google_storage_bucket_object.imdbconfig.output_name
  role_entity = ["READER:allUsers", "OWNER:project-owners-251342811543", "OWNER:project-editors-251342811543"]
  depends_on = [google_storage_bucket_object.imdbconfig]
}

resource "google_storage_bucket" "input-bucket" {
  name = var.input_name
  storage_class = "REGIONAL"
  location = "us-central1"

}

resource "google_storage_bucket_iam_binding" "input-bucket-iam-legacyBucketReader" {
  bucket = google_storage_bucket.input-bucket.name
  members = ["serviceAccount:project-${var.project_number}@storage-transfer-service.iam.gserviceaccount.com"]
  role = "roles/storage.legacyBucketReader"
  depends_on = [google_storage_bucket.input-bucket]
}

resource "google_storage_bucket_iam_binding" "input-bucket-iam-objectAdmin" {
  bucket = google_storage_bucket.input-bucket.name
  members = ["serviceAccount:project-${var.project_number}@storage-transfer-service.iam.gserviceaccount.com"]
  role = "roles/storage.objectAdmin"
  depends_on = [google_storage_bucket.input-bucket]
}

resource "google_storage_transfer_job" "imdb_nightly" {
  description = "Nightly download of IMDB data"
  depends_on = [
    google_storage_bucket_iam_binding.input-bucket-iam-legacyBucketReader,
    google_storage_bucket_iam_binding.input-bucket-iam-objectAdmin,
    google_storage_object_acl.imdbconfig_acl
  ]
  schedule {
    start_time_of_day {
      hours = 3
      minutes = 0
      nanos = 0
      seconds = 0
    }
    schedule_start_date {
      day = 11
      month = 11
      year = 2019
    }
  }
  transfer_spec {
    http_data_source {
      list_url = "https://storage.googleapis.com/graph-backend/config/imdb-urls.tsv"
    }
    transfer_options {
      overwrite_objects_already_existing_in_sink = true
    }
    gcs_data_sink {
      bucket_name = google_storage_bucket.input-bucket.name
    }
  }
}

data "archive_file" "gcs_trigger" {
  type = "zip"
  output_path = "${path.root}/../files/gcs_trigger.zip"
  source_dir = "${path.root}/../cloud-function/gcstrigger/"
}

resource "google_storage_bucket_object" "gcs_trigger_pkg" {
  name = "/functions/gcs_trigger.zip"
  bucket = google_storage_bucket.backend-bucket.name
  source = data.archive_file.gcs_trigger.output_path
  depends_on = [google_storage_bucket.backend-bucket, data.archive_file.gcs_trigger]
}

resource "google_cloudfunctions_function" "gcs_trigger_func" {
  name = "gcs_trigger_dataflow"
  source_archive_bucket = google_storage_bucket_object.gcs_trigger_pkg.bucket
  source_archive_object = google_storage_bucket_object.gcs_trigger_pkg.name
  runtime = "nodejs8"
  entry_point = "gcs_trigger_dataflow"
  event_trigger {
    event_type = "google.storage.object.finalize"
    resource = google_storage_bucket.input-bucket.name
  }
  depends_on = [google_storage_bucket_object.gcs_trigger_pkg]
}