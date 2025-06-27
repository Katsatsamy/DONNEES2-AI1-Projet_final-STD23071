import pandas as pd
import os
from datetime import datetime

def merge_forecast_files() -> str:
    input_dir = "data/raw/forecast_meteo"
    output_file = "data/processed/forecast_meteo_global.csv"

    # Créer le dossier processed si inexistant
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Charger les données existantes
    if os.path.exists(output_file):
        global_df = pd.read_csv(output_file)
    else:
        global_df = pd.DataFrame()

    # Lire les nouveaux fichiers
    new_data = []
    for file in os.listdir(input_dir):
        if file.startswith("meteo_") and file.endswith(".csv"):
            city = file.replace("meteo_", "").replace(".csv", "")
            df = pd.read_csv(f"{input_dir}/{file}")
            df["ville"] = city
            df["date_extraction"] = datetime.utcnow().strftime("%Y-%m-%d")
            new_data.append(df)

    if not new_data:
        raise ValueError("Aucun fichier de prévision trouvé à fusionner.")

    # Fusion et déduplication
    updated_df = pd.concat([global_df] + new_data, ignore_index=True)
    updated_df = updated_df.drop_duplicates(
        subset=["ville", "datetime"], keep="last"
    )

    updated_df.to_csv(output_file, index=False)
    print(f"Données fusionnées dans {output_file}")
    return output_file
