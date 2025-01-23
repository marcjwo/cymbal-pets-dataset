locals {
  table_configurations = {
    for table_id in var.table_ids :
    table_id => {
      clustering = lookup(var.cluster_columns, table_id, [])
    }
  }
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id                 = var.dataset_id
  location                   = var.location
  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "table" {
  for_each            = toset(var.table_ids)
  dataset_id          = google_bigquery_dataset.dataset.dataset_id
  table_id            = each.value
  schema              = file("${path.module}/schema/${each.value}_schema.json")
  deletion_protection = false
  clustering          = local.table_configurations[each.value].clustering != null ? local.table_configurations[each.value].clustering : []

  dynamic "time_partitioning" {
    for_each = lookup(var.partition_columns, each.value, null) != null ? [lookup(var.partition_columns, each.value)] : []
    content {
      type  = "DAY"
      field = time_partitioning.value
    }
  }


  depends_on = [google_bigquery_dataset.dataset]
}
