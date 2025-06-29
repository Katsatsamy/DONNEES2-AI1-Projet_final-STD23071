from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Permet d'importer les modules du dossier scripts/
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "scripts"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from extraction import fetch_forecast
from merge import merge_forecast_files
from transform import transform_forecast_to_scores

# Configuration par défaut du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 29),
}

# Liste des villes à traiter
CITIES = ['Paris', 'Rome', 'Tokyo']

with DAG(
    dag_id='forecast_weather_pipeline',
    default_args=default_args,
    schedule='@daily',  # exécution quotidienne
    catchup=False,
    max_active_runs=1,
) as dag:

    # ========= Tâches d'extraction (prévisions météo par ville) =========
    extract_tasks = [
        PythonOperator(
            task_id=f'fetch_forecast_{city.lower()}',
            python_callable=fetch_forecast,
            op_args=[city, "{{ var.value.API_KEY_WEATHER_API }}"],
        )
        for city in CITIES
    ]

    # ========= Tâche de fusion des fichiers =========
    merge_task = PythonOperator(
        task_id='merge_forecast_files',
        python_callable=merge_forecast_files,
    )

    # ========= Tâche de transformation des données =========
    transform_task = PythonOperator(
        task_id='transform_forecast_to_scores',
        python_callable=transform_forecast_to_scores,
    )

    # ========= Dépendances entre tâches =========
    extract_tasks >> merge_task >> transform_task
