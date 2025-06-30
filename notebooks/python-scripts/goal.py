import pandas as pd

df = pd.read_csv("../data/merge/all_weather_data.csv")

# Group by city and month
monthly_stats = (
    df.groupby(["city", "month"])
    .agg(
        avg_comfort_score=("comfort_score", "mean"),
        ideal_day_pct=("is_ideal_day", "mean"),
        total_days=("is_ideal_day", "count")
    )
    .reset_index()
)

# Rank the best travel month per city
monthly_stats["score_rank"] = monthly_stats.groupby("city")["avg_comfort_score"].rank(ascending=False)
best_months = monthly_stats[monthly_stats["score_rank"] == 1]
