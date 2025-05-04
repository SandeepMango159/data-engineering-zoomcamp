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
  project     = "de-zoomcamp-course-458518"
  region      = "europe-west1"

}

