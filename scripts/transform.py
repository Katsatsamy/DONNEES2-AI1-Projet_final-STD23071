import pandas as pd
import os

def transform_forecast_to_scores() -> str:
    input_file = "data/processed/forecast_meteo_global.csv"
    output_file = "data/processed/scores_meteo_global.csv"

    df = pd.read_csv(input_file)
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["date"] = df["datetime"].dt.date

    # Score météo
    def compute_score(row):
        score = 0
        if 22 <= row["temp"] <= 28:
            score += 1
        if row["rain"] < 1:
            score += 1
        if row["wind_speed"] < 5:
            score += 1
        return score

    df["score"] = df.apply(compute_score, axis=1)

    # Moyenne des scores par jour et par ville
    score_df = df.groupby(["ville", "date"])["score"].mean().reset_index()
    score_df.rename(columns={"score": "avg_score"}, inplace=True)
    score_df["avg_score"] = score_df["avg_score"].round(2)

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    score_df.to_csv(output_file, index=False)

    print(f"Scores météo journaliers enregistrés dans {output_file}")
    return output_file
