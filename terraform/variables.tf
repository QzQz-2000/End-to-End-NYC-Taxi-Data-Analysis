variable "project" {
  description = "Project"
  default     = "<your-project-id>"
}

variable "credentials" {
  description = "My Credentials"
  default     = "./keys/credential.json"
}

variable "region" {
  description = "Region"
  default     = "europe-west4-a"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "nyc_taxi_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "nyc-taxi-storage-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}