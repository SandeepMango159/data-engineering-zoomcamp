# Zoek op terraform registry naar juiste provider code
# Search on terraform registry for correct the provider code
terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.33.0"
    }
  }
}

# On the registry the config options are also listed 
provider "google" {
  # Configuration options
  # Can put credentials here too
#   credentials = file(var.credentials)
  project     = var.project
  region      = var.location

}

# Create a resource on GCP 
# - with variable name "examplename"
resource "google_storage_bucket" "data-lake-bucket" {
  # name must be unique across all of the entirity of GCP, so including every user and every project ever
  # Make unique by using projectid in name, because that's unique... 
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      # Age is specified in days, from creation time for an object, or initiated time for multi-part upload
      age = 1
    }
    action {
      # For literally what the name says, multipart uploads that have failed...
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "demo_dataset" {
    # The dataset name we want to use
  dataset_id = var.bq_dataset_name
  location = var.location
}