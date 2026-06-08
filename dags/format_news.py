import pandas as pd, json
from datetime import date
from textblob import TextBlob
from utils import raw_path, fmt_path

def format_news():
    today = str(date.today())
    src = raw_path("news", "articles", today)
    with open(f"{src}/articles.json") as f:
        raw = json.load(f)

    rows = []
    for a in raw.get("articles", []):
        title = a.get("title","") or ""
        desc  = a.get("description","") or ""
        blob  = TextBlob(title + " " + desc)
        rows.append({
            "source":       a.get("source",{}).get("name",""),
            "title":        title,
            "url":          a.get("url",""),
            "published_at": pd.to_datetime(
                              a.get("publishedAt"), utc=True, errors="coerce"),
            "sentiment":    round(blob.sentiment.polarity, 4),
            "subjectivity": round(blob.sentiment.subjectivity, 4),
            "ingested_at":  pd.Timestamp.utcnow()
        })

    df = pd.DataFrame(rows).dropna(subset=["published_at"])
    out = fmt_path("news", "sentiment", today)
    df.to_parquet(out, index=False)
    print(f"Formatted {len(df)} articles → {out}")
    print(df[["title","sentiment"]].head())

if __name__ == "__main__":
    format_news()