provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

data "azurerm_client_config" "current" {}

# Groupe de ressources pour SQL Server, DB et Key Vault
resource "azurerm_resource_group" "rg_datasource" {
  name     = "rg-datasource-dev-ghe"
  location = var.location_sql
}

# Groupe de ressources pour Databricks
resource "azurerm_resource_group" "rg_dataplatform" {
  name     = "rg-dataplatform-dev-ghe"
  location = var.location
}

# SQL Server
resource "azurerm_mssql_server" "sql_server" {
  name                         = "sql-datasource-dev-ghe"
  resource_group_name          = azurerm_resource_group.rg_datasource.name
  location                     = azurerm_resource_group.rg_datasource.location
  version                      = "12.0"
  administrator_login          = var.sql_admin
  administrator_login_password = var.sql_password

  identity {
    type = "SystemAssigned"
  }
}

# SQL Database avec configuration compatible abonnement étudiant
resource "azurerm_mssql_database" "sql_db" {
  name                            = "sqldb-adventureworks-dev-ghe"
  server_id                       = azurerm_mssql_server.sql_server.id
  sku_name                        = "Basic"
  max_size_gb                     = 2
  sample_name                     = "AdventureWorksLT"
  storage_account_type            = "Local"
  collation                       = "SQL_Latin1_General_CP1_CI_AS"
  transparent_data_encryption_enabled = true

  tags = {
    environment = "dev"
  }
}

# Azure Key Vault
resource "azurerm_key_vault" "kv" {
  name                        = "kv-jdbc-secrets-dev-ghe"
  location                    = azurerm_resource_group.rg_datasource.location
  resource_group_name         = azurerm_resource_group.rg_datasource.name
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  sku_name                    = "standard"

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id
    secret_permissions = ["Get", "Set", "List"]
  }
}

# Secrets dans le Key Vault
resource "azurerm_key_vault_secret" "sql_username" {
  name         = "sql-username"
  value        = var.sql_admin
  key_vault_id = azurerm_key_vault.kv.id
}

resource "azurerm_key_vault_secret" "sql_password" {
  name         = "sql-password"
  value        = var.sql_password
  key_vault_id = azurerm_key_vault.kv.id
}

# Databricks Workspace
resource "azurerm_databricks_workspace" "databricks" {
  name                          = "dbw-dataplatform-dev-ghe"
  location                      = azurerm_resource_group.rg_dataplatform.location
  resource_group_name           = azurerm_resource_group.rg_dataplatform.name
  managed_resource_group_name   = "mg-dataplatform-dev-ghe"
  sku                           = "premium"

  tags = {
    environment = "dev"
  }
}
