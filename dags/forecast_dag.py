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
from extraction import fetch_historical_weather
from merge import merge_forecast_files
from transform import transform_forecast_to_scores

# Configuration par défaut du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 29),
}

# Liste des villes à traiter
CITIES = ['Paris', 'Rome', 'Tokyo', 'Antananarivo', 'Mahajanga','Quebec']

with DAG(
    dag_id='forecast_weather_pipeline',
    default_args=default_args,
    schedule='@daily',  # exécution quotidienne
    catchup=False,
    max_active_runs=1,
) as dag:
    
        # ========= Tâches d'extraction historique =========
    historical_extract_tasks = [
        PythonOperator(
            task_id=f'fetch_historical_{city.lower()}',
            python_callable=fetch_historical_weather,
            op_args=[city, "{{ var.value.API_KEY_VISUAL_CROSSING }}"],
        )
        for city in CITIES
    ]

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
    for hist_task, extract_task in zip(historical_extract_tasks, extract_tasks):
        hist_task >> extract_task
    extract_tasks >> merge_task >> transform_task


