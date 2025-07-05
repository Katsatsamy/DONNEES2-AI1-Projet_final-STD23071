import requests
import pandas as pd
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

def fetch_historical_weather(city: str, api_key: str = None) -> bool:
    """
    Récupère les données historiques météo pour une ville via Open-Meteo (pas besoin de clé API).
    Sauvegarde les données dans un fichier CSV.
    """

    coords = {
        "Paris": (48.8566, 2.3522),
        "Rome": (41.9028, 12.4964),
        "Tokyo": (35.6895, 139.6917),
        "Antananarivo": (-18.8792, 47.5079),
        "Mahajanga": (-15.7167, 46.3167),
        "Quebec": (46.8139, -71.2080),
    }

    if city not in coords:
        print(f"Ville inconnue : {city}")
        return False

    lat, lon = coords[city]
    start_date = "2024-06-30"
    end_date = datetime.today().strftime('%Y-%m-%d')

    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&daily=temperature_2m_max,precipitation_sum,windspeed_10m_max"
        f"&timezone=auto"
    )

    print(f"[{city}] URL : {url}")
    response = requests.get(url)

    if response.status_code == 200:
        json_data = response.json()
        days = json_data.get("daily", {})
        if not days:
            print(f"Aucune donnée historique pour {city}")
            return False

        df = pd.DataFrame(days)
        df["city"] = city

        output_dir = os.path.join(ROOT_DIR, "data", "raw", "historical_meteo")
        os.makedirs(output_dir, exist_ok=True)

        df.to_csv(os.path.join(output_dir, f"historical_{city}.csv"), index=False)
        print(f"✔ Données historiques sauvegardées pour {city}")
        return True
    else:
        print(f"Erreur Open-Meteo pour {city} : {response.status_code}")
        return False
