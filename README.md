# 📦 Infrastructure Lakehouse – Déploiement via Terraform

Ce dépôt contient l'automatisation complète de l'infrastructure pour un projet de type Lakehouse sur Azure. Le déploiement est basé sur Terraform, avec un script Python de post-déploiement obligatoire pour compléter la configuration du workspace Databricks.

## ✅ Fonctionnalités

- Création de groupes de ressources dédiés
- Déploiement d'un SQL Server + base de données avec exemple AdventureWorksLT
- Création d'un Azure Key Vault contenant les identifiants SQL
- Provisionnement d'un Azure Databricks Workspace avec son managed resource group
- Génération d'outputs Terraform réutilisables pour automatisation
- Stockage des notebooks `.dbc` versionnés dans `/notebooks/` :
  - `1. Initialisation.dbc`
  - `2. Bronze layer – Ingestion.dbc`
  - `3. Silver layer – Transformation.dbc`
  - `4. Gold layer – Aggregation.dbc`

## 🚀 Déploiement en 3 étapes

### 1. Initialisation et création de l'infrastructure

```bash
git clone https://github.com/geraldherrera/infra-azure-lakehouse.git
cd infra-azure-lakehouse
```

Créer un fichier `secrets.auto.tfvars` avec les variables sensibles :

```hcl
subscription_id     = "<votre-subscription-id>"
sql_admin           = "<nom-utilisateur-sql>"
sql_password        = "<mot-de-passe-sql>"
databricks_token    = "<token-databricks>"
```

> ⚠️ Ce fichier ne doit **jamais être versionné**. Il est ignoré par `.gitignore`.

Puis lancer l’init et l’application du plan Terraform :

```bash
terraform init
terraform apply
```

### 2. Génération manuelle du token Databricks

Après la création du workspace Databricks :
1. Connectez-vous à l’interface Databricks
2. Allez dans `User Settings > Access Tokens`
3. Cliquez sur "Generate New Token"
4. Copiez ce token dans `secrets.auto.tfvars` sous la clé `databricks_token`

### 3. Lancement du post-déploiement

Un script `post_deploy.py` est requis pour :
- Importer automatiquement les notebooks `.dbc` dans le workspace Databricks
- (À venir) créer les jobs et orchestrer le workflow complet

## 📁 Structure du dépôt

```
infra-azure-lakehouse/
├── main.tf                  # Déploiement de l'infrastructure Azure
├── variables.tf             # Variables globales
├── outputs.tf               # Infos utiles extraites après apply
├── secrets.auto.tfvars      # ⚠️ Fichier local, non versionné
├── notebooks/               # Notebooks .dbc prêts à importer
│   ├── 1. Initialisation.dbc
│   ├── 2. Bronze layer – Ingestion.dbc
│   ├── 3. Silver layer – Transformation.dbc
│   └── 4. Gold layer – Aggregation.dbc
├── post_deploy.py           # Script Python pour automatisation Databricks
├── README.md                # Ce fichier
```

## 💬 À venir

- Création d’un cluster et d’une policy "Personal Compute"
- Intégration d’un workflow Databricks (jobs)
- Gestion d’un train/test en sandbox ML

---

🛠️ Ce projet a été conçu pour minimiser les manipulations manuelles et garantir la reproductibilité du déploiement sur Azure + Databricks. Il peut servir de base à toute architecture de type Lakehouse en environnement académique ou professionnel.
