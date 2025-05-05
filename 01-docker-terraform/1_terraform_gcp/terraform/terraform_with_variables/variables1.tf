variable "credentials" {
    description = "My Credentials"
    default = "/path to keyfile"
  
}

variable "project" {
  description = "Project name"
  default = "de-zoomcamp-course-458518"
}

variable "location" {
  description = "Project Location"
  default = "europe-west1"
}

# Defines the id/name for the dataset
variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  default = "demo_dataset"
}

variable "gcs_bucket_name" {
    description = "My STorage Bucket Name"
    default = "de-zoomcamp-course-458518-data-lake-bucket"
  
}

# Defines the storage class
variable "gcs_storage_class" {
    description = "Bucket Storage Class"
    default = "STANDARD"
  
}