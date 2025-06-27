from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts")))


# Import des fonctions depuis tes scripts
from extraction import fetch_forecast
from merge import merge_forecast_files
from transform import transform_forecast_to_scores

# Paramètres du DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 23),
    "retries": 1
}

with DAG(
    dag_id="forecast_weather_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["meteo", "openweather", "tourisme"]
) as dag:

    # Étape 1 : Extraction météo pour plusieurs villes
    cities = ["Paris", "Rome", "Antananarivo", "Mahajanga", "Londre"]
    extraction_tasks = []

    for city in cities:
        task = PythonOperator(
            task_id=f"fetch_forecast_{city.lower()}",
            python_callable=fetch_forecast,
            op_args=[city, "{{ var.value.API_KEY_WEATHER_API }}"],
        )
        extraction_tasks.append(task)

    # Étape 2 : Fusion des fichiers
    merge_task = PythonOperator(
        task_id="merge_forecast_files",
        python_callable=merge_forecast_files
    )

    # Étape 3 : Transformation → score météo
    transform_task = PythonOperator(
        task_id="transform_forecast_to_scores",
        python_callable=transform_forecast_to_scores
    )

    # Dépendances
    extraction_tasks >> merge_task >> transform_task
