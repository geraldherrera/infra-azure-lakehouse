# 📦 Infrastructure Lakehouse – Déploiement via Terraform

Ce dépôt contient l'automatisation complète de l'infrastructure pour un projet de type Lakehouse sur Azure. Le déploiement est basé sur Terraform, avec un script Python de post-déploiement permettant de finaliser la configuration du workspace Databricks et d'y importer les notebooks.

## ✅ Fonctionnalités

- Création de groupes de ressources dédiés pour SQL et Databricks
- Déploiement d'un SQL Server avec base de données AdventureWorksLT (format étudiant)
- Création d'un Azure Key Vault contenant les identifiants SQL
- Provisionnement d'un Azure Databricks Workspace avec managed resource group
- Génération d'outputs Terraform réutilisables dans les scripts
- Stockage des notebooks `.dbc` versionnés dans le dossier `/notebooks/` :
  - `1. Initialisation.dbc`
  - `2. Bronze layer – Ingestion.dbc`
  - `2.5 Bronze layer – Test.dbc`
  - `3. Silver layer – Transformation.dbc`
  - `3.5 Silver layer – Test.dbc`
  - `4. Gold layer – Aggregation.dbc`

## 🚀 Déploiement en 3 étapes

### 1. Initialisation et création de l'infrastructure

```bash
git clone https://github.com/geraldherrera/infra-azure-lakehouse.git
cd infra-azure-lakehouse
```

Créer un fichier `secrets.auto.tfvars` (non versionné) avec les variables sensibles :

```hcl
subscription_id     = "<votre-subscription-id>"
sql_admin           = "<nom-utilisateur-sql>"
sql_password        = "<mot-de-passe-sql>"
aad_admin_login     = "<votre-email@domain.com>"
aad_admin_object_id = "<object-id-de-votre-utilisateur>"
```

Et un fichier `terraform.tfvars` pour les noms de ressources :

```hcl
location                    = "westeurope"
location_sql                = "switzerlandnorth"
rg_datasource_name          = "rg-datasource-dev-ghe"
rg_dataplatform_name        = "rg-dataplatform-dev-ghe"
sql_server_name             = "sql-datasource-dev-ghe"
sql_database_name           = "sqldb-adventureworks-dev-ghe"
key_vault_name              = "kv-jdbc-secrets-dev-ghe"
databricks_workspace_name   = "dbw-dataplatform-dev-ghe"
databricks_managed_rg_name  = "mg-dataplatform-dev-ghe"
```

Puis lancer l'initialisation et l'application du plan :

```bash
terraform init
terraform apply
```

### 2. Création manuelle du scope Azure Key Vault dans Databricks

Une seule étape manuelle est nécessaire pour lier le Key Vault à Databricks :

```bash
# Récupérer un token AAD
export DATABRICKS_AAD_TOKEN=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv)

# Configurer la CLI Databricks avec AAD
databricks configure --aad-token

# Créer le scope kv-jdbc
# Remplacer les valeurs avec celles de vos outputs Terraform

databricks secrets create-scope \
  --scope kv-jdbc \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id "/subscriptions/<subscription-id>/resourceGroups/<nom-rg>/providers/Microsoft.KeyVault/vaults/<nom-vault>" \
  --dns-name "https://<nom-vault>.vault.azure.net/"
```

> ⚠️ Cette suite de commande est à exécuter **une seule fois**. Elle lie Databricks à votre Key Vault de façon permanente.

### 3. Lancement du post-déploiement

Le script `post_deploy.py` effectue les actions suivantes :
- Crée la cluster policy "Personal Policy - GHE"
- Crée un cluster "Personal Compute - Gerald Herrera"
- Importe les notebooks `.dbc` dans Databricks
- Exécute automatiquement le notebook `1. Initialisation`

```bash
python post_deploy.py
```

> Il vous sera demandé de saisir votre token d'accès personnel Databricks lors de l'exécution du script.

## 📁 Structure du dépôt

```
infra-azure-lakehouse/
├── main.tf                  # Déploiement de l'infrastructure Azure
├── variables.tf             # Variables globales
├── outputs.tf               # Infos extraites automatiquement
├── secrets.auto.tfvars      # ⚠️ Fichier local, non versionné
├── terraform.tfvars         # Valeurs des noms de ressources
├── notebooks/               # Notebooks Databricks (.dbc)
│   ├── 1. Initialisation.dbc
│   ├── 2. Bronze layer – Ingestion.dbc
│   ├── 2.5 Bronze layer – Test.dbc
│   ├── 3. Silver layer – Transformation.dbc
│   ├── 3.5 Silver layer – Test.dbc
│   └── 4. Gold layer – Aggregation.dbc
├── post_deploy.py           # Script Python de post-déploiement
├── README.md                # Ce fichier
```

## 💬 Notes

- Le déploiement est à la fois modulaire et réutilisable pour d'autres projets similaires
- L'étape manuelle de création du secret scope est volontaire, car elle nécessite une authentification AAD non automatisable
- Le script Python est conçu pour fonctionner avec **zéro modification** si les fichiers `.tfvars` sont correctement remplis

---

🛠️ Ce projet a été conçu pour minimiser les manipulations manuelles et garantir la reproductibilité du déploiement sur Azure + Databricks. Il peut servir de base à toute architecture de type Lakehouse en environnement académique ou professionnel.

---

README généré à l'aide de ChatGPT