import os
import json
import requests
import subprocess
import time
from base64 import b64encode

# =======================
# 🔧 CONFIGURATION UTILISATEUR
# =======================

NOTEBOOKS_DIR = "./notebooks"
DATABRICKS_NOTEBOOK_FOLDER = "/Workspace/Users/gerald.herrera@he-arc.ch"
NOTEBOOK_TO_RUN = "1. Initialisation"

CLUSTER_NAME = "Personal Compute - Gerald Herrera"
POLICY_NAME = "Personal Policy - GHE"

# =======================
# 🔁 LOGIQUE DE DÉPLOIEMENT
# =======================

def load_terraform_outputs():
    print("🔍 Lecture des outputs Terraform...")
    raw = subprocess.check_output(["terraform", "output", "-json"])
    data = json.loads(raw)
    url = data["databricks_workspace_url"]["value"].strip()
    if not url.startswith("https://"):
        url = f"https://{url}"
    return {"host": url.rstrip("/")}

def create_databricks_client(host, token):
    def call(method, endpoint, **kwargs):
        url = f"{host}/api/2.0{endpoint}"
        headers = {
            "Authorization": f"Bearer {token}"
        }
        response = requests.request(method, url, headers=headers, **kwargs)
        if not response.ok:
            print(f"❌ Erreur Databricks {response.status_code} - {response.text}")
            response.raise_for_status()
        return response.json() if response.text else {}
    return call

def ensure_cluster_policy(client):
    print(f"⚙️ Création de la policy de cluster : {POLICY_NAME}")
    policies = client("get", "/policies/clusters/list").get("policies", [])
    for p in policies:
        if p["name"] == POLICY_NAME:
            print("✅ Policy déjà existante.")
            return p["policy_id"]

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
            "value": 60,
            "hidden": True
        }
    }

    policy = client("post", "/policies/clusters/create", json={
        "name": POLICY_NAME,
        "definition": json.dumps(definition)
    })
    return policy["policy_id"]

def ensure_cluster(client, policy_id):
    print(f"🖥️ Vérification ou création du cluster : {CLUSTER_NAME}")
    clusters = client("get", "/clusters/list").get("clusters", [])
    for c in clusters:
        if c["cluster_name"] == CLUSTER_NAME:
            print("✅ Cluster déjà existant.")
            return c["cluster_id"]

    cluster = client("post", "/clusters/create", json={
        "cluster_name": CLUSTER_NAME,
        "spark_version": "15.4.x-scala2.12",
        "node_type_id": "Standard_F4",
        "policy_id": policy_id,
        "autotermination_minutes": 60,
        "num_workers": 1,
        "data_security_mode": "SINGLE_USER"
    })
    return cluster["cluster_id"]

def import_dbc_files(client, folder=DATABRICKS_NOTEBOOK_FOLDER):
    print(f"📦 Import des fichiers DBC depuis {NOTEBOOKS_DIR} vers {folder}")
    for file in os.listdir(NOTEBOOKS_DIR):
        if file.endswith(".dbc"):
            filepath = os.path.join(NOTEBOOKS_DIR, file)
            print(f"→ Import de {file}")
            with open(filepath, "rb") as f:
                data = f.read()
            encoded = b64encode(data).decode("utf-8")
            client("post", "/workspace/import", json={
                "path": f"{folder}/{file.replace('.dbc', '')}",
                "format": "DBC",
                "content": encoded
            })

def run_notebook_and_wait(client, notebook_path, cluster_id, retry_on_quota_error=True):
    print(f"🚀 Exécution du notebook : {notebook_path}")
    payload = {
        "run_name": "Initialisation automatique",
        "existing_cluster_id": cluster_id,
        "notebook_task": {
            "notebook_path": notebook_path
        }
    }
    response = client("post", "/jobs/runs/submit", json=payload)
    run_id = response.get("run_id")
    print(f"📡 Job soumis avec ID : {run_id}")

    while True:
        time.sleep(10)
        status = client("get", f"/jobs/runs/get?run_id={run_id}")
        life_cycle = status["state"]["life_cycle_state"]
        result_state = status["state"].get("result_state")
        print(f"⌛ Statut : {life_cycle} / Résultat : {result_state}")
        if life_cycle in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            break

    if result_state == "SUCCESS":
        print("✅ Notebook exécuté avec succès.")
    else:
        print("❌ Le notebook a échoué. Détails :")
        print(status.get("state", {}))

# =======================
# 🚀 LANCEMENT DU SCRIPT
# =======================

if __name__ == "__main__":
    outputs = load_terraform_outputs()
    token = input("🔑 Entrez votre token Databricks : ").strip()
    client = create_databricks_client(outputs["host"], token)

    policy_id = ensure_cluster_policy(client)
    cluster_id = ensure_cluster(client, policy_id)
    import_dbc_files(client)
    run_notebook_and_wait(client, f"{DATABRICKS_NOTEBOOK_FOLDER}/{NOTEBOOK_TO_RUN}", cluster_id)
