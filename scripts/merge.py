import pandas as pd
import os
from datetime import datetime

# Récupère le chemin absolu du dossier scripts/
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Remonte à la racine du projet examen_weather_pipeline/
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))

def merge_forecast_files() -> str:
    # Dossier d'entrée contenant les fichiers bruts
    input_dir = os.path.join(ROOT_DIR, "data", "raw", "forecast_meteo")

    # Dossier de sortie pour le fichier global fusionné
    output_file = os.path.join(ROOT_DIR, "data", "processed", "forecast_meteo_global.csv")

    # Crée le dossier d'entrée s’il n’existe pas
    os.makedirs(input_dir, exist_ok=True)

    # Crée le dossier de sortie s’il n’existe pas
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Charge l’ancien fichier s’il existe
    if os.path.exists(output_file):
        global_df = pd.read_csv(output_file)
    else:
        global_df = pd.DataFrame()

    new_data = []

    # Vérifie s’il y a des fichiers CSV à fusionner
    for file in os.listdir(input_dir):
        if file.startswith("meteo_") and file.endswith(".csv"):
            city = file.replace("meteo_", "").replace(".csv", "")
            df = pd.read_csv(os.path.join(input_dir, file))
            df["ville"] = city
            df["date_extraction"] = datetime.utcnow().strftime("%Y-%m-%d")
            new_data.append(df)

    # Si aucun fichier valide trouvé, on lève une erreur
    if not new_data:
        raise ValueError(f"Aucun fichier de prévision trouvé à fusionner dans {input_dir}.")

    # Fusionne les anciens et nouveaux fichiers
    updated_df = pd.concat([global_df] + new_data, ignore_index=True)

    # Supprime les doublons sur ville + datetime
    updated_df = updated_df.drop_duplicates(subset=["ville", "datetime"], keep="last")

    # Sauvegarde le fichier fusionné
    updated_df.to_csv(output_file, index=False)

    print(f"Fichier fusionné enregistré dans {output_file}")
    return output_file
