import requests, json, os
from dotenv import load_dotenv
from utils import raw_path
load_dotenv()

def fetch_cricket_news():
    params = {
        "q": "cricket team match",
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 100,
        "apiKey": os.getenv("NEWSAPI_KEY")
    }
    r = requests.get("https://newsapi.org/v2/everything",
                     params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    path = raw_path("news", "articles")
    with open(f"{path}/articles.json", "w") as f:
        json.dump(data, f, indent=2)
    print(f"NewsAPI saved {data['totalResults']} results → {path}")

if __name__ == "__main__":
    fetch_cricket_news()