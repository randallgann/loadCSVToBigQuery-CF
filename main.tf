provider "google" {
    project = var.project_id
    region = var.region
}

data "google_storage_bucket" "source-bucket" {
    name = "cvs-by-zip"
}

resource "google_storage_bucket" "functions_bucket"{
    name = "real_estate_data_functions_bucket"
    location = var.region
    uniform_bucket_level_access = true
}

data "google_storage_project_service_account" "gcs-service-account" {
}

resource "google_project_iam_member" "gcs-pubsub-publishing" {
    project = var.project_id
    role = "roles/pubsub.publisher"
    member = "serviceAccount:${data.google_storage_project_service_account.gcs-service-account.email_address}"
}

resource "google_storage_bucket_object" "object" {
    name = "LoadCSVToBigQuery.zip"
    bucket = google_storage_bucket.functions_bucket.name
    source = "LoadCsvToBigQuery/LoadCSVToBigQuery.zip"
}

resource "google_storage_bucket" "trigger-bucket" {
    name = "real_estate_data_trigger_bucket"
    location = var.region
    uniform_bucket_level_access = true

}

resource "google_service_account" "account" {
    account_id = "csv2bigquerysvc"
    display_name = "Service Account used for both the cloud function and eventarc trigger"
}

resource google_project_iam_member "invoking" {
    project = var.project_id
    role = "roles/run.invoker"
    member = "serviceAccount:${google_service_account.account.email}"
    depends_on = [google_project_iam_member.gcs-pubsub-publishing]
}

resource "google_project_iam_member" "event-receiving" {
    project = var.project_id
    role = "roles/eventarc.eventReceiver"
    member = "serviceAccount:${google_service_account.account.email}"
    depends_on = [ google_project_iam_member.invoking ]
}

resource "google_project_iam_member" "artifactregistry-reader" {
    project = var.project_id
    role = "roles/artifactregistry.reader"
    member = "serviceAccount:${google_service_account.account.email}"
    depends_on = [google_project_iam_member.event-receiving]
}

resource google_project_iam_member "event-logging" {
    project = var.project_id
    role ="roles/logging.logWriter"
    member = "serviceAccount:${google_service_account.account.email}"
    depends_on = [ google_project_iam_member.artifactregistry-reader ]
}

resource google_project_iam_member "bg-admin" {
    project = var.project_id
    role ="roles/bigquery.admin"
    member = "serviceAccount:${google_service_account.account.email}"
    depends_on = [ google_project_iam_member.event-logging ]
}

resource google_project_iam_member "storage-admin" {
    project = var.project_id
    role ="roles/storage.admin"
    member = "serviceAccount:${google_service_account.account.email}"
    depends_on = [ google_project_iam_member.bg-admin ]
}

resource "google_cloudfunctions2_function" "load_csv_to_bigquery" {
    name = "load_csv_to_bigquery"
    location = var.region
    description = "A function this is triggered by a Cloud Storage bucket and loads a CSV file into BigQuery"

    build_config {
      runtime = "dotnet6"
      entry_point = "LoadCsvToBigQuery.LoadCSVToBigQueryFunction"
      source {
        storage_source {
            bucket = google_storage_bucket.functions_bucket.name
            object = "LoadCSVToBigQuery.zip"
        }
      }
    }

    service_config {
        max_instance_count = 1
        min_instance_count = 1
        available_memory = "256M"
        timeout_seconds = 540
        ingress_settings = "ALLOW_INTERNAL_ONLY"
        all_traffic_on_latest_revision = true
        service_account_email = google_service_account.account.email
        environment_variables = {
          SERVICE_CONFIG_TEST = "config_test"
          GCP_PROJECT = var.project_id
          DATASET_ID = var.bigquery_dataset
          TABLE_ID = var.bigquery_table
        }
    }
    

    event_trigger {
        trigger_region = "us-central1"
        event_type = "google.cloud.storage.object.v1.finalized"
        retry_policy = "RETRY_POLICY_RETRY"
        service_account_email = google_service_account.account.email
        event_filters {
            attribute = "bucket"
            value = data.google_storage_bucket.source-bucket.name
        }
    }

    depends_on = [
        google_project_iam_member.bg-admin,
        google_project_iam_member.artifactregistry-reader,
        google_project_iam_member.event-receiving,
        google_project_iam_member.invoking,
        google_project_iam_member.gcs-pubsub-publishing,
        google_project_iam_member.storage-admin,
        google_storage_bucket_object.object
    ]
}

resource "google_bigquery_dataset" "dataset" {
    dataset_id = var.bigquery_dataset
    location = var.region
}

resource "google_bigquery_table" "table_by_zipcode" {
    dataset_id = google_bigquery_dataset.dataset.dataset_id
    table_id = var.bigquery_table
    deletion_protection = false

    schema = <<EOF
    [
        {
            "name": "mls",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "class",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "property_type",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "status",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "price",
            "type": "NUMERIC",
            "mode": "REQUIRED"
        },
        {
            "name": "county",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "address",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "city",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "zip",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "beds",
            "type": "INT64",
            "mode": "REQUIRED"
        },
        {
            "name": "baths",
            "type": "INT64",
            "mode": "REQUIRED"
        },
        {
            "name": "half_baths",
            "type": "INT64",
            "mode": "REQUIRED"
        },
        {
            "name": "garage",
            "type": "INT64",
            "mode": "REQUIRED"
        },
        {
            "name": "sq_feet",
            "type": "INT64",
            "mode": "REQUIRED"
        },
        {
            "name": "list_agent",
            "type": "STRING",
            "mode": "REQUIRED"
        },
        {
            "name": "list_office",
            "type": "STRING",
            "mode": "REQUIRED"
        }
    ]
    EOF
}

variable "project_id" {
    description = "The project ID to deploy resources" 
}

variable "region" {
    description = "The region to deploy resources"
  
}

variable "bigquery_dataset" {
    description = "The BigQuery dataset to load the CSV file into"
}

variable "bigquery_table" {
    description = "The BigQuery table to load the CSV file into"
  
}