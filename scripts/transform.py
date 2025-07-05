import pandas as pd
import os

# Récupère le chemin absolu du dossier où se trouve ce script (scripts/)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Remonte d'un dossier pour atteindre la racine examen_weather_pipeline/
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))

def transform_forecast_to_scores() -> str:
    # Fichier CSV de prévisions
    forecast_file = os.path.join(ROOT_DIR, "data", "processed", "forecast_meteo_global.csv")

    # Dossier des historiques
    historical_dir = os.path.join(ROOT_DIR, "data", "raw", "historical_meteo")

    # Fichier de sortie
    output_file = os.path.join(ROOT_DIR, "data", "processed", "scores_meteo_global.csv")

    # === Lecture des prévisions ===
    forecast_df = pd.read_csv(forecast_file)
    forecast_df["datetime"] = pd.to_datetime(forecast_df["datetime"])
    forecast_df["date"] = forecast_df["datetime"].dt.date
    forecast_df["source"] = "forecast"

    # === Lecture des historiques ===
    historical_dfs = []
    for file in os.listdir(historical_dir):
        if file.endswith(".csv") and file.startswith("historical_"):
            path = os.path.join(historical_dir, file)
            df = pd.read_csv(path)
            city = file.replace("historical_", "").replace(".csv", "")
            df["ville"] = city
            df.rename(columns={
                "time": "date",
                "temperature_2m_max": "temp",
                "precipitation_sum": "rain",
                "windspeed_10m_max": "wind_speed"
            }, inplace=True)
            df["source"] = "historical"
            historical_dfs.append(df[["date", "temp", "rain", "wind_speed", "ville", "source"]])

    historical_df = pd.concat(historical_dfs, ignore_index=True) if historical_dfs else pd.DataFrame()

    # === Préparation des prévisions ===
    forecast_df = forecast_df[["date", "temp", "rain", "wind_speed", "ville", "source"]]

    # === Fusion des deux ===
    combined_df = pd.concat([forecast_df, historical_df], ignore_index=True)

    # Calcul du score météo
    def compute_score(row):
        score = 0
        if pd.notna(row["temp"]) and 22 <= row["temp"] <= 28:
            score += 1
        if pd.notna(row["rain"]) and row["rain"] < 1:
            score += 1
        if pd.notna(row["wind_speed"]) and row["wind_speed"] < 5:
            score += 1
        return score

    combined_df["score"] = combined_df.apply(compute_score, axis=1)

    # Moyenne par ville et date
    score_df = combined_df.groupby(["ville", "date"])["score"].mean().reset_index()
    score_df.rename(columns={"score": "avg_score"}, inplace=True)
    score_df["avg_score"] = score_df["avg_score"].round(2)

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    score_df.to_csv(output_file, index=False)

    print(f"✅ Scores météo journaliers enregistrés dans {output_file}")
    return output_file