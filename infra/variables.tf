variable "project_id" {
  type        = string
  description = "Google Cloud Project ID "
}

variable "location" {
  type        = string
  description = "Google Cloud Region"
  default     = "us-east1"
}

variable "dataset_id" {
  type        = string
  description = "Name of the dataset for the tables containing data extracts"
}

variable "table_ids" {
  type        = list(string)
  description = "List of tables to be created with their corresponding schema"
}

variable "partition_columns" {
  description = "Map of table IDs to partition columns."
  type        = map(string)
  default     = {}
}

variable "cluster_columns" {
  description = "Map of table IDs to cluster columns."
  type        = map(list(string))
  default     = {}
}
