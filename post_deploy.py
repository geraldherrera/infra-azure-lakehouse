import os
import json
import requests
import subprocess
import time
from base64 import b64encode

# ========================== #
# CONFIGURATION UTILISATEUR  #
# ========================== #

# Remplacez ces valeurs par celles de votre environnement

NOTEBOOKS_DIR = "./notebooks"
DATABRICKS_NOTEBOOK_FOLDER = "/Workspace/Users/gerald.herrera@he-arc.ch"
NOTEBOOK_TO_RUN = "1.0_initialisation"

CLUSTER_NAME = "Personal Compute - Gerald Herrera"
POLICY_NAME = "Personal Policy - GHE"

# ======================= #
# FONCTIONS PRINCIPALES   #
# ======================= #

def load_terraform_outputs():
    """
    Charge les outputs Terraform pour récupérer l'URL du workspace Databricks.

    Retourne:
        dict: Contient l'URL du workspace Databricks sous la clé 'host'.
    """
    print("Lecture des outputs Terraform...")
    raw = subprocess.check_output(["terraform", "output", "-json"])
    data = json.loads(raw)
    url = data["databricks_workspace_url"]["value"].strip()
    if not url.startswith("https://"):
        url = f"https://{url}"
    return {"host": url.rstrip("/")}

def create_databricks_client(host, token):
    """
    Crée un client API pour interagir avec Databricks.

    Paramètres:
        host (str): URL du workspace Databricks.
        token (str): Jeton d'authentification Databricks.

    Retourne:
        function: Fonction permettant d'effectuer des appels API à Databricks.
    """
    def call(method, endpoint, **kwargs):
        url = f"{host}/api/2.0{endpoint}"
        headers = {
            "Authorization": f"Bearer {token}"
        }
        response = requests.request(method, url, headers=headers, **kwargs)
        if not response.ok:
            print(f"Erreur Databricks {response.status_code} - {response.text}")
            response.raise_for_status()
        return response.json() if response.text else {}
    return call

def ensure_cluster_policy(client):
    """
    Vérifie l'existence d'une policy de cluster et la crée si nécessaire.

    Paramètres:
        client (function): Fonction pour effectuer des appels API à Databricks.

    Retourne:
        str: ID de la policy de cluster.
    """
    print(f"Création de la policy de cluster : {POLICY_NAME}")
    policies = client("get", "/policies/clusters/list").get("policies", [])
    for p in policies:
        if p["name"] == POLICY_NAME:
            print("Policy déjà existante.")
            return p["policy_id"]

    # Définition de la policy standardisée
    definition = {
        "node_type_id": {
            "type": "allowlist",
            "values": ["Standard_F4"],
            "defaultValue": "Standard_F4"
        },
        "spark_version": {
            "type": "unlimited",
            "defaultValue": "auto:latest-ml"
        },
        "runtime_engine": {
            "type": "fixed",
            "value": "STANDARD",
            "hidden": True
        },
        "num_workers": {
            "type": "fixed",
            "value": 0,
            "hidden": True
        },
        "data_security_mode": {
            "type": "allowlist",
            "values": [
                "SINGLE_USER",
                "LEGACY_SINGLE_USER",
                "LEGACY_SINGLE_USER_STANDARD"
            ],
            "defaultValue": "SINGLE_USER",
            "hidden": True
        },
        "driver_instance_pool_id": {
            "type": "forbidden",
            "hidden": True
        },
        "cluster_type": {
            "type": "fixed",
            "value": "all-purpose"
        },
        "instance_pool_id": {
            "type": "forbidden",
            "hidden": True
        },
        "azure_attributes.availability": {
            "type": "fixed",
            "value": "ON_DEMAND_AZURE",
            "hidden": True
        },
        "spark_conf.spark.databricks.cluster.profile": {
            "type": "fixed",
            "value": "singleNode",
            "hidden": True
        },
        "autotermination_minutes": {
            "type": "fixed",
            "value": 10,
            "hidden": True
        }
    }

    policy = client("post", "/policies/clusters/create", json={
        "name": POLICY_NAME,
        "definition": json.dumps(definition)
    })
    return policy["policy_id"]

def ensure_cluster(client, policy_id):
    """
    Vérifie l'existence d'un cluster personnel et le crée si nécessaire.

    Paramètres:
        client (function): Fonction pour effectuer des appels API à Databricks.
        policy_id (str): ID de la policy de cluster à appliquer.

    Retourne:
        str: ID du cluster Databricks.
    """
    print(f"Vérification ou création du cluster : {CLUSTER_NAME}")
    clusters = client("get", "/clusters/list").get("clusters", [])
    for c in clusters:
        if c["cluster_name"] == CLUSTER_NAME:
            print("Cluster déjà existant.")
            return c["cluster_id"]

    # Création d'un nouveau compute avec la policy définie
    cluster = client("post", "/clusters/create", json={
        "cluster_name": CLUSTER_NAME,
        "spark_version": "15.4.x-scala2.12",
        "node_type_id": "Standard_F4",
        "policy_id": policy_id,
        "autotermination_minutes": 10,
        "num_workers": 1,
        "data_security_mode": "SINGLE_USER"
    })
    return cluster["cluster_id"]

def import_ipynb_files(client, folder=DATABRICKS_NOTEBOOK_FOLDER):
    """
    Importe les fichiers .ipynb dans le workspace Databricks.

    Paramètres:
        client (function): Fonction pour effectuer des appels API à Databricks.
        folder (str): Chemin du dossier cible dans le workspace Databricks.
    """
    print(f"Import des fichiers ipynb depuis {NOTEBOOKS_DIR} vers {folder}")
    for file in os.listdir(NOTEBOOKS_DIR):
        if file.endswith(".ipynb"):
            filepath = os.path.join(NOTEBOOKS_DIR, file)
            print(f"Import de {file}")
            with open(filepath, "rb") as f:
                data = f.read()
            encoded = b64encode(data).decode("utf-8")
            client("post", "/workspace/import", json={
                "path": f"{folder}/{file.replace('.ipynb', '')}",
                "format": "ipynb",
                "content": encoded
            })

def run_notebook_and_wait(client, notebook_path, cluster_id, retry_on_quota_error=True):
    """
    Exécute un notebook Databricks et attend la fin de son exécution.

    Paramètres:
        client (function): Fonction pour effectuer des appels API à Databricks.
        notebook_path (str): Chemin du notebook dans le workspace Databricks.
        cluster_id (str): ID du cluster sur lequel exécuter le notebook.
        retry_on_quota_error (bool): Indique s'il faut réessayer en cas d'erreur de quota.

    Retourne:
        None
    """
    print(f"Exécution du notebook : {notebook_path}")
    payload = {
        "run_name": "Initialisation automatique",
        "existing_cluster_id": cluster_id,
        "notebook_task": {
            "notebook_path": notebook_path
        }
    }
    response = client("post", "/jobs/runs/submit", json=payload)
    run_id = response.get("run_id")
    print(f"Job soumis avec ID : {run_id}")

    while True:
        time.sleep(10)
        status = client("get", f"/jobs/runs/get?run_id={run_id}")
        life_cycle = status["state"]["life_cycle_state"]
        result_state = status["state"].get("result_state")
        print(f"Statut : {life_cycle} / Résultat : {result_state}")
        if life_cycle in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            break

    if result_state == "SUCCESS":
        print("Notebook exécuté avec succès.")
    else:
        print("Le notebook a échoué. Détails :")
        print(status.get("state", {}))

def ensure_sql_warehouse(client):
    """
    Vérifie l'existence d'un SQL Warehouse Serverless et le crée si nécessaire.

    Paramètres:
        client (function): Fonction pour effectuer des appels API à Databricks.

    Retourne:
        str: ID du SQL warehouse.
    """
    print("Vérification ou création du SQL Warehouse Serverless...")

    # Vérifie s'il existe déjà un entrepôt nommé "Serverless SQL"
    warehouses = client("get", "/sql/warehouses").get("warehouses", [])
    for w in warehouses:
        if w["name"] == "Serverless SQL":
            print("Warehouse déjà existant.")
            return w["id"]

    # Crée un nouveau SQL warehouse avec les paramètres définis
    payload = {
        "name": "Serverless SQL",
        "cluster_size": "2X-Small",
        "enable_serverless_compute": True,
        "auto_stop_mins": 10,
        "min_num_clusters": 1,
        "max_num_clusters": 1,
        "channel": {
            "name": "CHANNEL_NAME_CURRENT"
        }
    }

    response = client("post", "/sql/warehouses", json=payload)
    print(f"SQL Warehouse créé avec ID : {response['id']}")
    return response["id"]

def ensure_etl_workflow(client, cluster_id):
    """
    Crée un job Databricks nommé lakehouse_etl_pipeline-ghe avec une exécution planifiée à 2h00 chaque jour.

    Paramètres:
        client (function): Client Databricks pour effectuer les appels API.
        cluster_id (str): ID du cluster existant utilisé pour les tâches.

    Retourne:
        str: ID du job créé ou existant.
    """
    print("Vérification ou création du job ETL planifié...")

    # Vérifie si le job existe déjà
    jobs = client("get", "/jobs/list").get("jobs", [])
    for job in jobs:
        if job["settings"]["name"] == "lakehouse_etl_pipeline-ghe":
            print("Job déjà existant.")
            return job["job_id"]

    # Définition des tâches
    bronze_task = {
        "task_key": "task_bronze_ghe",
        "notebook_task": {
            "notebook_path": "/Workspace/Users/gerald.herrera@he-arc.ch/2.0_bronze_layer_ingestion"
        },
        "existing_cluster_id": cluster_id
    }

    silver_task = {
        "task_key": "task_silver_ghe",
        "depends_on": [{"task_key": "task_bronze_ghe"}],
        "notebook_task": {
            "notebook_path": "/Workspace/Users/gerald.herrera@he-arc.ch/3.0_silver_layer_transformation"
        },
        "existing_cluster_id": cluster_id
    }

    gold_task = {
        "task_key": "task_gold_ghe",
        "depends_on": [{"task_key": "task_silver_ghe"}],
        "notebook_task": {
            "notebook_path": "/Workspace/Users/gerald.herrera@he-arc.ch/4.0_gold_layer_aggregation"
        },
        "existing_cluster_id": cluster_id
    }

    # Définition du job avec planification quotidienne à 2h00
    payload = {
        "name": "lakehouse_etl_pipeline-ghe",
        "tasks": [bronze_task, silver_task, gold_task],
        "schedule": {
            "quartz_cron_expression": "0 0 2 * * ?",
            "timezone_id": "Europe/Zurich",
            "pause_status": "UNPAUSED"
        },
        "format": "MULTI_TASK"
    }

    response = client("post", "/jobs/create", json=payload)
    print(f"Job créé avec ID : {response['job_id']}")
    return response["job_id"]

# ========================== #
# POINT D'ENTRÉE DU SCRIPT   #
# ========================== #

if __name__ == "__main__":
    outputs = load_terraform_outputs()
    token = input("Entrez votre token Databricks : ").strip()
    client = create_databricks_client(outputs["host"], token)

    policy_id = ensure_cluster_policy(client)
    cluster_id = ensure_cluster(client, policy_id)
    import_ipynb_files(client)
    run_notebook_and_wait(client, f"{DATABRICKS_NOTEBOOK_FOLDER}/{NOTEBOOK_TO_RUN}", cluster_id)
    warehouse_id = ensure_sql_warehouse(client)
    workflow_id = ensure_etl_workflow(client, cluster_id)
