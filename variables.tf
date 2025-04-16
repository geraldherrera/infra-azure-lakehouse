# Identifiant de l'abonnement Azure
variable "subscription_id" {
  description = "Azure Subscription ID utilisé pour le déploiement"
  type        = string
}

# Région par défaut pour les ressources générales (Databricks, groupes, etc.)
variable "location" {
  description = "Région Azure pour Databricks et autres ressources générales"
  type        = string
  default     = "westeurope"
}

# Région dédiée au SQL Server (ex: Switzerland North ou autre)
variable "location_sql" {
  description = "Région Azure pour le déploiement du serveur SQL"
  type        = string
  default     = "Switzerland North"
}

# Nom d'utilisateur admin pour le serveur SQL
variable "sql_admin" {
  description = "Nom d'utilisateur administrateur pour le serveur SQL"
  type        = string
  sensitive   = true
}

# Mot de passe associé au compte admin SQL
variable "sql_password" {
  description = "Mot de passe administrateur pour le serveur SQL"
  type        = string
  sensitive   = true
}
