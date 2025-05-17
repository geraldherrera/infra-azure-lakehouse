# 📦 Infrastructure Lakehouse – Déploiement via Terraform

Ce dépôt contient l'automatisation complète de l'infrastructure pour un projet de type Lakehouse sur Azure. Le déploiement est basé sur Terraform, avec un script Python de post-déploiement permettant de finaliser la configuration du workspace Databricks et d'y importer les notebooks.

## ✅ Fonctionnalités

- Création de groupes de ressources dédiés pour SQL et Databricks
- Déploiement d'un SQL Server avec base de données AdventureWorksLT (format étudiant)
- Création d'un Azure Key Vault contenant les identifiants SQL
- Provisionnement d'un Azure Databricks Workspace avec managed resource group
- Génération d'outputs Terraform réutilisables dans les scripts
- Stockage des notebooks `.ipynb` versionnés dans le dossier `/notebooks/` :
  - `1.0_initialisation.ipynb`
  - `2.0_bronze_layer_ingestion.ipynb`
  - `2.5_bronze_layer_test.ipynb`
  - `3.0_silver_layer_transformation.ipynb`
  - `3.5_silver_layer_test.ipynb`
  - `4.0_gold_layer_aggregation.ipynb`

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

> Cette suite de commande est à exécuter **une seule fois**. Elle lie Databricks à votre Key Vault de façon permanente.

### 3. Lancement du post-déploiement

Le script `post_deploy.py` effectue les actions suivantes :
- Crée la cluster policy "Personal Policy - GHE"
- Crée un cluster "Personal Compute - Gerald Herrera"
- Importe les notebooks `.ipynb` dans Databricks
- Exécute automatiquement le notebook `1.0_initialisation`

```bash
python post_deploy.py
```

> ⚠️ Il vous sera demandé de saisir votre token d'accès personnel Databricks lors de l'exécution du script.

## 🎯 Orchestration ETL dans Databricks (Workflow)

Une fois l’infrastructure créée et les notebooks importés, un workflow Databricks est automatiquement généré via le script `post_deploy.py`.

Ce workflow s'appelle **`lakehouse_etl_pipeline-ghe`** et s’exécute automatiquement tous les jours à **2h00 du matin**. Il suit une architecture à trois couches, avec les tâches suivantes :

```
| Tâche              | Chemin notebook                                                              | Dépendance            |
|--------------------|------------------------------------------------------------------------------|-----------------------|
| `task_bronze_ghe`  | `/Workspace/Users/gerald.herrera@he-arc.ch/2.0_bronze_layer_ingestion`       | Aucune                |
| `task_silver_ghe`  | `/Workspace/Users/gerald.herrera@he-arc.ch/3.0_silver_layer_transformation`  | `task_bronze_ghe`     |
| `task_gold_ghe`    | `/Workspace/Users/gerald.herrera@he-arc.ch/4.0_gold_layer_aggregation`       | `task_silver_ghe`     |
```

Chaque tâche est exécutée sur le cluster personnel nommé **`Personal Compute - Gerald Herrera`**, préconfiguré automatiquement par le script.

> Ce workflow assure l’enchaînement cohérent des étapes du pipeline de données de bronze à gold.

---


## ⚖️ Accès Power BI via entrepôt Serverless
Le script post_deploy.py crée également un SQL Warehouse Serverless nommé "Serverless SQL". Celui-ci est configuré automatiquement avec les paramètres suivants :

```
Type : Serverless

Taille : 2X-Small (XXS)

Arrêt automatique : 10 minutes

Min/Max clusters : 1

Canal : Current
```

Ce warehouse peut être utilisé immédiatement depuis Power BI en tant que source de données directe, sans manipulation supplémentaire.

Pour se connecter, utilisez le connecteur Databricks dans Power BI et sélectionnez l'entrepôt "Serverless SQL" comme cible.

🖍️ Tableau de bord Power BI

Le fichier business_sales_dashboard.pbix est un tableau de bord interactif Power BI fournissant une vue d’ensemble complète des ventes, des produits, des clients et des opérations commerciales.

Il s’appuie sur un modèle en étoile construit à partir des données traitées en couche Gold sur Databricks, et couvre les thématiques suivantes :
```
Vue d’ensemble commerciale : KPIs clés, tendances mensuelles, ventes par région.

Produits et catalogue : Analyse des meilleures ventes, produits non vendus, performance par catégorie.

Analyse client : Répartition géographique, commandes par client, clients les plus rentables.
```

Ce fichier est prévu pour être connecté directement au SQL Warehouse "Serverless SQL" déployé automatiquement. Pour cela il suffit d'ajouter la source de données.

## 📁 Structure du dépôt

```
infra-azure-lakehouse/
├── main.tf                     # Déploiement de l'infrastructure Azure
├── variables.tf                # Variables globales
├── outputs.tf                  # Infos extraites automatiquement
├── secrets.auto.tfvars         # ⚠️ Fichier local, non versionné
├── terraform.tfvars            # Valeurs des noms de ressources
├── notebooks/                  # Notebooks Databricks (.ipynb)
│   ├── 1.0_initialisation.ipynb
│   ├── 2.0_bronze_layer_ingestion.ipynb
│   ├── 2.5_bronze_layer_test.ipynb
│   ├── 3.0_silver_layer_transformation.ipynb
│   ├── 3.5_silver_layer_test.ipynb
│   └── 4.0_gold_layer_aggregation.ipynb
├── post_deploy.py                # Script Python de post-déploiement
├── alteration_donee_source.sql   # Code SQL d'altération de la base de données source pour tester la couche silver
├── rollback_donee_source.sql     # Code SQL remise par défaut de la base de données source
├── gold_ddl_diagram.md           # Diagram DDL de la couche gold en mermaid
├── gold_ddl_diagram.svg          # Export svg du Diagram DDL de la couche gold
├── business_sales_dashboard.pbix # Dashboard ventes, produits et clients (couche Gold).
├── README.md                     # Ce fichier
```

## 💬 Notes

- Le déploiement est à la fois modulaire et réutilisable pour d'autres projets similaires
- L'étape manuelle de création du secret scope est volontaire, car elle nécessite une authentification AAD non automatisable
- Le script Python est conçu pour fonctionner avec **zéro modification** si les fichiers `.tfvars` sont correctement remplis

---

🛠️ Ce projet a été conçu pour minimiser les manipulations manuelles et garantir la reproductibilité du déploiement sur Azure + Databricks. Il peut servir de base à toute architecture de type Lakehouse en environnement académique ou professionnel.

---

README généré à l'aide de ChatGPT