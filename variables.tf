variable "subscription_id" {
  description = "Azure Subscription ID"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "westeurope"
}

variable "sql_admin" {
  description = "Admin username for the SQL server"
  type        = string
  sensitive   = true
}

variable "sql_password" {
  description = "Admin password for the SQL server"
  type        = string
  sensitive   = true
}
