provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "rg_datasource" {
  name     = "rg-datasource-dev-ghe"
  location = var.location
}

resource "azurerm_sql_server" "sql_server" {
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

resource "azurerm_sql_database" "sql_db" {
  name                = "sqldb-adventureworks-dev-ghe"
  resource_group_name = azurerm_resource_group.rg_datasource.name
  location            = azurerm_resource_group.rg_datasource.location
  server_name         = azurerm_sql_server.sql_server.name

  sku_name = "Basic"

  tags = {
    environment = "dev"
  }
}

resource "azurerm_key_vault" "kv" {
  name                        = "kv-jdbc-secrets-dev-ghe"
  location                    = azurerm_resource_group.rg_datasource.location
  resource_group_name         = azurerm_resource_group.rg_datasource.name
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  sku_name                    = "standard"
  soft_delete_enabled         = true
  purge_protection_enabled    = false

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id
    secret_permissions = ["get", "set", "list"]
  }
}

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
