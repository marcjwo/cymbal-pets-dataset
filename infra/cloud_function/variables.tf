variable "project_id" {
  type        = string
  description = "Google Cloud Project ID "
}

variable "location" {
  type        = string
  description = "GCP Location"
  default     = "US"
}

variable "region" {
  type        = string
  description = "GCP region"
  default     = "us-east1"
}

variable "dataset_id" {
  type        = string
  description = "Name of the dataset for the tables containing data extracts"
}

variable "daily_orders" {
  type    = string
  default = "2500"
}

# variable "min_locations" {
#   type    = string
#   default = "3"
# }

# variable "max_locations" {
#   type    = string
#   default = "6"
# }

variable "num_of_customers" {
  type    = string
  default = "92000"
}

variable "start_date" {
  type    = string
  default = "date(2022-01-01)"
}
