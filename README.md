# Azure Lakehouse Infrastructure with Terraform

This repository contains the Terraform code to provision the infrastructure for a Lakehouse project on Microsoft Azure.

## 🧱 Infrastructure Overview
This Terraform configuration sets up the following resources:

- Azure Resource Group: `rg-datasource-dev-ghe`
- Azure SQL Server: `sql-datasource-dev-ghe`
- Azure SQL Database: `sqldb-adventureworks-dev-ghe`
- Azure Key Vault: `kv-jdbc-secrets-dev-ghe`
  - Secrets stored:
    - `sql-username`
    - `sql-password`

## 🚀 Getting Started

### Prerequisites
- [Terraform](https://developer.hashicorp.com/terraform/downloads)
- [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)
- An Azure subscription

### Steps

1. Clone the repository:
```bash
git clone https://github.com/ton-utilisateur/infra-azure-lakehouse.git
cd infra-azure-lakehouse
```

2. Authenticate with Azure:
```bash
az login
az account set --subscription "<your-subscription-id>"
```

3. Create a `secrets.auto.tfvars` file with the following content:
```hcl
subscription_id = "<your-subscription-id>"
sql_admin       = "<your-admin-name>"
sql_password    = "<your-password>"
```
> ⚠️ This file is **ignored by Git** and must not be committed.

4. Initialize and apply Terraform:
```bash
terraform init
terraform plan
terraform apply
```

## 📁 File Structure
```
infra-azure-lakehouse/
├── main.tf               # Terraform resource definitions
├── variables.tf          # Variables used in the configuration
├── outputs.tf            # Optional outputs (can be expanded)
├── .gitignore            # Files to exclude from Git
└── secrets.auto.tfvars   # Sensitive data (excluded from Git)
```

## ✅ To Do (Next Steps)
- [ ] Add Databricks workspace
- [ ] Create Databricks cluster
- [ ] Deploy notebooks and workflows
- [ ] Automate Unity Catalog and governance setup
- [ ] Optional CI/CD via GitHub Actions

---

Made with ❤️ by Gerald Herrera
