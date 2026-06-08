import requests, json, os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from utils import raw_path
load_dotenv()

def ingest_news():
    # Multiple search queries for broader coverage
    search_queries = [
        "cricket team match",
        "cricket tournament",
        "cricket world cup",
        "cricket IPL",
        "cricket test match",
        "cricket T20",
        "cricket players",
        "cricket series"
    ]
    
    all_articles = []
    
    # Get date range for last 7 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    for query in search_queries:
        # Paginate through results (max 5 pages per query to stay within API limits)
        for page in range(1, 6):
            params = {
                "q": query,
                "language": "en",
                "sortBy": "publishedAt",
                "pageSize": 100,
                "page": page,
                "from": start_date.strftime("%Y-%m-%d"),
                "to": end_date.strftime("%Y-%m-%d"),
                "apiKey": os.getenv("NEWSAPI_KEY")
            }
            
            try:
                r = requests.get("https://newsapi.org/v2/everything",
                                params=params, timeout=15)
                r.raise_for_status()
                data = r.json()
                
                if data.get("articles"):
                    all_articles.extend(data["articles"])
                    print(f"  Query '{query}' page {page}: {len(data['articles'])} articles")
                
                # Stop if no more results
                if len(data.get("articles", [])) == 0:
                    break
                    
            except Exception as e:
                print(f"  Error fetching query '{query}' page {page}: {e}")
                continue
    
    # Remove duplicates based on URL
    seen_urls = set()
    unique_articles = []
    for article in all_articles:
        if article.get("url") and article["url"] not in seen_urls:
            seen_urls.add(article["url"])
            unique_articles.append(article)
    
    result = {
        "totalResults": len(unique_articles),
        "articles": unique_articles,
        "queries_used": search_queries,
        "date_range": {
            "from": start_date.strftime("%Y-%m-%d"),
            "to": end_date.strftime("%Y-%m-%d")
        }
    }
    
    path = raw_path("news", "articles")
    with open(f"{path}/articles.json", "w") as f:
        json.dump(result, f, indent=2)
    print(f"NewsAPI saved {len(unique_articles)} unique articles → {path}")

if __name__ == "__main__":
    ingest_news()