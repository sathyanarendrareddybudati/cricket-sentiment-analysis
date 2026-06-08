import pandas as pd
from datetime import date
from utils import fmt_path, usage_path

def combine():
    today = str(date.today())
    matches = pd.read_parquet(fmt_path("cricbuzz","matches",today))
    news = pd.read_parquet(fmt_path("news","sentiment",today))

    # Get all unique team names
    teams = list(set(
        matches["team1"].tolist() + matches["team2"].tolist()
    ))
    teams = [t for t in teams if t and t != "nan"]

    # Step 1 — Sentiment per team (search article titles)
    sent_rows = []
    for team in teams:
        mask = news["title"].str.contains(team, case=False, na=False)
        avg_s = news.loc[mask, "sentiment"].mean()
        sent_rows.append({
            "team": team,
            "avg_sentiment": round(float(avg_s), 4) if pd.notna(avg_s) else 0.0,
            "article_count": int(mask.sum())
        })
    team_sent = pd.DataFrame(sent_rows)

    # Step 2 — Win rate per team from matches
    perf = {t: {"wins":0,"played":0} for t in teams}
    for _, row in matches.iterrows():
        t1, t2, st = row["team1"], row["team2"], row["status"]
        if t1 in perf: perf[t1]["played"] += 1
        if t2 in perf: perf[t2]["played"] += 1
        # Mark winner if team name appears in status string
        for t in [t1, t2]:
            if t and t.lower() in st.lower() and "won" in st.lower():
                if t in perf: perf[t]["wins"] += 1

    perf_rows = [{
        "team": t,
        "win_rate": round(v["wins"]/max(v["played"],1), 4),
        "matches_played": v["played"]
    } for t, v in perf.items()]
    team_perf = pd.DataFrame(perf_rows)

    # Step 3 — Join and compute team_score
    combined = team_sent.merge(team_perf, on="team", how="outer").fillna(0)
    combined["team_score"] = (
        combined["win_rate"]      * 0.6 +
        combined["avg_sentiment"] * 0.4
    ).round(4)
    combined["date"] = today
    combined.sort_values("team_score", ascending=False, inplace=True)

    out = usage_path("team_insights", today)
    combined.to_parquet(out, index=False)
    print(combined[["team","avg_sentiment","win_rate","team_score"]].head(10).to_string())
    print(f"\nSaved → {out}")

if __name__ == "__main__":
    combine()