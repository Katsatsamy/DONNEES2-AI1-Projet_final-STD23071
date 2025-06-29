import requests
import os
from datetime import datetime

# Récupère le chemin absolu du dossier où se trouve ce script (scripts/)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Remonte d'un dossier pour atteindre la racine examen_weather_pipeline/
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))

def fetch_forecast(city: str, api_key: str) -> bool:
    """
    Récupère les données météo de prévision pour une ville donnée via l'API OpenWeatherMap
    et sauvegarde le résultat dans un fichier CSV.

    Args:
        city (str): nom de la ville (ex: 'Paris')
        api_key (str): clé API OpenWeatherMap

    Returns:
        bool: True si la récupération et sauvegarde ont réussi, False sinon
    """
    url = f"https://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api_key}&units=metric"

    # Envoie la requête GET à l'API
    response = requests.get(url)
    
    # Vérifie si la requête a réussi (code 200)
    if response.status_code == 200:
        data = response.json()

        # Chemin du dossier où stocker les fichiers bruts (raw)
        output_dir = os.path.join(ROOT_DIR, "data", "raw", "forecast_meteo")
        os.makedirs(output_dir, exist_ok=True)  # Création si inexistant

        # Fichier CSV de sortie nommé selon la ville
        output_file = os.path.join(output_dir, f"meteo_{city}.csv")

        # Prépare la liste des données extraites
        rows = []
        for item in data.get("list", []):
            rows.append({
                "datetime": item["dt_txt"],
                "temp": item["main"]["temp"],
                "rain": item.get("rain", {}).get("3h", 0.0),  # pluie sur 3h, 0 si absente
                "wind_speed": item["wind"]["speed"]
            })

        # Sauvegarde les données sous forme CSV
        import pandas as pd
        df = pd.DataFrame(rows)
        df.to_csv(output_file, index=False)

        print(f"Données météo sauvegardées dans {output_file}")
        return True
    else:
        print(f"Erreur lors de la récupération météo pour {city} : {response.status_code}")
        return False
