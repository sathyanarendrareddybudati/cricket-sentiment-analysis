import requests, json, os
from dotenv import load_dotenv
from utils import raw_path
load_dotenv()

def ingest_matches():
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
    print(f"Cricbuzz matches saved → {path}/matches.json")

def ingest_series():
    url = "https://cricbuzz-cricket.p.rapidapi.com/series/v1/international"
    headers = {
        "X-RapidAPI-Key": os.getenv("RAPIDAPI_KEY"),
        "X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
    }
    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        path = raw_path("cricbuzz", "series")
        with open(f"{path}/series.json", "w") as f:
            json.dump(r.json(), f, indent=2)
        print(f"Cricbuzz series saved → {path}/series.json")
    except Exception as e:
        print(f"Warning: Failed to ingest series data: {e}")
        path = raw_path("cricbuzz", "series")
        with open(f"{path}/series.json", "w") as f:
            json.dump({"series": []}, f, indent=2)

def ingest_teams():
    url = "https://cricbuzz-cricket.p.rapidapi.com/teams/v1/international"
    headers = {
        "X-RapidAPI-Key": os.getenv("RAPIDAPI_KEY"),
        "X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
    }
    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        path = raw_path("cricbuzz", "teams")
        with open(f"{path}/teams.json", "w") as f:
            json.dump(r.json(), f, indent=2)
        print(f"Cricbuzz teams saved → {path}/teams.json")
    except Exception as e:
        print(f"Warning: Failed to ingest teams data: {e}")
        path = raw_path("cricbuzz", "teams")
        with open(f"{path}/teams.json", "w") as f:
            json.dump({"team": []}, f, indent=2)

def ingest_players():
    url = "https://cricbuzz-cricket.p.rapidapi.com/players/v1/international"
    headers = {
        "X-RapidAPI-Key": os.getenv("RAPIDAPI_KEY"),
        "X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
    }
    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        path = raw_path("cricbuzz", "players")
        with open(f"{path}/players.json", "w") as f:
            json.dump(r.json(), f, indent=2)
        print(f"Cricbuzz players saved → {path}/players.json")
    except Exception as e:
        print(f"Warning: Failed to ingest players data: {e}")
        # Create empty file to avoid downstream errors
        path = raw_path("cricbuzz", "players")
        with open(f"{path}/players.json", "w") as f:
            json.dump({"player": []}, f, indent=2)

def ingest_all():
    ingest_matches()
    ingest_series()
    ingest_teams()
    ingest_players()

if __name__ == "__main__":
    ingest_all()