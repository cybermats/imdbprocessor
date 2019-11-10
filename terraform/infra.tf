terraform {
  backend "gcs" {
    bucket = "tf-state-gcp-imdb-processing"
    region = "us-central1"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = "matsf-cloud-graph"
  region = "us-central1"
}

resource "google_storage_bucket" "backend-bucket" {
  name = "graph-backend"
  storage_class = "REGIONAL"
  location = "us-central1"
}