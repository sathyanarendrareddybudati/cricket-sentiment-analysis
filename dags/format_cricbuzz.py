import pandas as pd, json
from datetime import date
from utils import raw_path, fmt_path

def format_matches():
    today = str(date.today())
    src = raw_path("cricbuzz", "matches", today)
    with open(f"{src}/matches.json") as f:
        raw = json.load(f)

    rows = []
    for type_match in raw.get("typeMatches", []):
        for series in type_match.get("seriesMatches", []):
            wrapper = series.get("seriesAdWrapper", {})
            for match in wrapper.get("matches", []):
                info  = match.get("matchInfo", {})
                score = match.get("matchScore", {})
                t1s   = score.get("team1Score",{}).get("inngs1",{})
                t2s   = score.get("team2Score",{}).get("inngs1",{})
                rows.append({
                    "match_id":    info.get("matchId"),
                    "team1":       info.get("team1",{}).get("teamName",""),
                    "team2":       info.get("team2",{}).get("teamName",""),
                    "status":      info.get("status",""),
                    "venue":       info.get("venueInfo",{}).get("ground",""),
                    "start_date":  pd.to_datetime(
                                     int(info.get("startDate", 0) or 0),
                                     unit="ms", utc=True),
                    "team1_runs":  t1s.get("runs"),
                    "team1_wkts":  t1s.get("wickets"),
                    "team2_runs":  t2s.get("runs"),
                    "team2_wkts":  t2s.get("wickets"),
                    "ingested_at": pd.Timestamp.utcnow()
                })

    df = pd.DataFrame(rows)
    out = fmt_path("cricbuzz", "matches", today)
    df.to_parquet(out, index=False)
    print(f"Formatted {len(df)} matches → {out}")
    print(df[["team1","team2","status"]].head())

if __name__ == "__main__":
    format_matches()