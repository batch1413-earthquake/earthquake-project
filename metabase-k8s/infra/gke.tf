resource "google_container_cluster" "gke_cluster" {
  name                = "${var.cluster_name}-${terraform.workspace}"
  location            = var.location
  project             = var.project_id
  initial_node_count  = 1
  enable_autopilot    = true
  deletion_protection = false

  node_config {
    preemptible  = true
    machine_type = "e2-standard-4"
  }

}

resource "null_resource" "get_gke_credentials" {
  # Make this resource depend on the google_container_cluster resource
  depends_on = [google_container_cluster.gke_cluster]

  provisioner "local-exec" {
    # Fetch credentials after the cluster has been created
    command = "gcloud container clusters get-credentials ${google_container_cluster.gke_cluster.name} --region ${google_container_cluster.gke_cluster.location} --project ${google_container_cluster.gke_cluster.project}"
  }
}

resource "google_service_account" "metabase_iam_service_account" {
    account_id   = "metabase-sa-${terraform.workspace}"
    display_name = "metabase-sa-${terraform.workspace}"
    project      = var.project_id
  }

resource "google_project_iam_member" "metabase_project_iam_member" {
    project = var.project_id
    role    = "roles/cloudsql.client"
    member  = "serviceAccount:${google_service_account.metabase_iam_service_account.email}"
  }

resource "google_service_account_iam_member" "metabase_service_account_iam_member" {
    service_account_id = google_service_account.metabase_iam_service_account.name
    role               = "roles/iam.workloadIdentityUser"
    member             = "serviceAccount:${var.project_id}.svc.id.goog[default/metabase-cloudsql-proxy]"
  }

resource "google_sql_database_instance" "metabase_sql_instance" {
  name                = "metabase-data-${terraform.workspace}"
  region              = var.location
  database_version    = "POSTGRES_13"
  project             = var.project_id
  deletion_protection = false

  settings {
    tier = "db-g1-small"
  }
}

resource "google_sql_user" "metabase_sql_user" {
  name     = "metabase_user"
  instance = google_sql_database_instance.metabase_sql_instance.name
  password = var.metabase_sql_password
  project  = var.project_id
}

resource "google_sql_database" "metabase_sql_database" {
  name     = "metabase-data-${terraform.workspace}"
  instance = google_sql_database_instance.metabase_sql_instance.name
  project  = var.project_id
}
