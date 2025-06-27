import pandas as pd
import requests
import os


def fetch_forecast(city : str, api_key: str):

    # Url de l'API OpenWeatherMap
    url = "https://api.openweathermap.org/data/2.5/forecast"
    params = {
        "q": city,
        "appid": api_key,
        "units": "metric"
    }

    # Envoyer une requette GET vers l'url pour prendre les données
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        forecasts = []

        for entry in data["list"]:
            forecasts.append({
                "datetime": entry["dt_txt"],
                "temp": entry["main"]["temp"],
                "rain": entry.get("rain", {}).get("3h", 0),
                "wind_speed": entry["wind"]["speed"]
            })

        # Création du dossier de destination
        os.makedirs("data/raw/forecast_meteo", exist_ok=True)

        # Sauvegarde des prévisions météo au format CSV
        pd.DataFrame(forecasts).to_csv(
            f"data/raw/forecast_meteo/meteo_{city}.csv",
            index=False
        )
        return True
    else:
        print(f"Erreur lors de l'appel API : {response.status_code} - {response.text}")

fetch_forecast("Paris", "3748ed23553fd1112e2bae4b80a6b537")