
variable "location" {
  type = string 
  default = "europe-west1"
}
variable "metabase_sql_password" {
  description = "The SQL password for Metabase"
  type        = string
  sensitive   = true
}

variable "project_id" {
  description = "The GCP project ID"
  type        = string
  sensitive   = true
}


variable "cluster_name" {

}
