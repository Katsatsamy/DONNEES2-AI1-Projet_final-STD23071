import pandas as pd
import os

# Récupère le chemin absolu du dossier où se trouve ce script (scripts/)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Remonte d'un dossier pour atteindre la racine examen_weather_pipeline/
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))

def transform_forecast_to_scores() -> str:
    # Fichier CSV fusionné en entrée
    input_file = os.path.join(ROOT_DIR, "data", "processed", "forecast_meteo_global.csv")

    # Fichier CSV de sortie avec les scores météo calculés
    output_file = os.path.join(ROOT_DIR, "data", "processed", "scores_meteo_global.csv")

    # Chargement du fichier CSV fusionné dans un DataFrame
    df = pd.read_csv(input_file)

    # Convertit la colonne datetime en type datetime pandas
    df["datetime"] = pd.to_datetime(df["datetime"])

    # Crée une nouvelle colonne date (sans l'heure) à partir de datetime
    df["date"] = df["datetime"].dt.date

    # Fonction qui calcule un score météo pour une ligne donnée
    def compute_score(row):
        score = 0
        # Température agréable entre 22 et 28°C → +1
        if 22 <= row["temp"] <= 28:
            score += 1
        # Peu ou pas de pluie → +1
        if row["rain"] < 1:
            score += 1
        # Vitesse du vent faible → +1
        if row["wind_speed"] < 5:
            score += 1
        return score

    # Applique la fonction compute_score à chaque ligne du DataFrame
    df["score"] = df.apply(compute_score, axis=1)

    # Regroupe par ville et date, calcule la moyenne des scores
    score_df = df.groupby(["ville", "date"])["score"].mean().reset_index()

    # Renomme la colonne score moyenne
    score_df.rename(columns={"score": "avg_score"}, inplace=True)

    # Arrondit à 2 décimales
    score_df["avg_score"] = score_df["avg_score"].round(2)

    # Création du dossier de sortie s'il n'existe pas
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Sauvegarde des scores dans le fichier de sortie
    score_df.to_csv(output_file, index=False)

    print(f"Scores météo journaliers enregistrés dans {output_file}")

    # Renvoie le chemin du fichier des scores
    return output_file
