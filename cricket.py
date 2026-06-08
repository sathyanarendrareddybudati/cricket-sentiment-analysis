from dotenv import load_dotenv
import os
import requests

load_dotenv()
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
NEWSAPI_KEY  = os.getenv("NEWSAPI_KEY")

r = requests.get(
  "https://cricbuzz-cricket.p.rapidapi.com/matches/v1/recent",
  headers={
    "X-RapidAPI-Key": os.getenv("RAPIDAPI_KEY"),
    "X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
  }
)
print(r.status_code, list(r.json().keys()))

# Test NewsAPI
r2 = requests.get(
  "https://newsapi.org/v2/everything",
  params={"q":"cricket","apiKey": os.getenv("NEWSAPI_KEY"),"pageSize":3}
)
print(r2.status_code, r2.json()["totalResults"])