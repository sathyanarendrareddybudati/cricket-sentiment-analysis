import requests, json, os
from dotenv import load_dotenv
from utils import raw_path
load_dotenv()

def fetch_matches():
    url = "https://cricbuzz-cricket.p.rapidapi.com/matches/v1/recent"
    headers = {
        "X-RapidAPI-Key": os.getenv("RAPIDAPI_KEY"),
        "X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
    }
    r = requests.get(url, headers=headers, timeout=15)
    r.raise_for_status()
    path = raw_path("cricbuzz", "matches")
    with open(f"{path}/matches.json", "w") as f:
        json.dump(r.json(), f, indent=2)
    print(f"Cricbuzz saved → {path}/matches.json")

if __name__ == "__main__":
    fetch_matches()