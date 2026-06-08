"""
index_elastic.py
----------------
Indexes the team_insights combination layer into Elasticsearch.
Also indexes raw match data and news articles for Kibana dashboarding.
"""

import pandas as pd
from elasticsearch import Elasticsearch, helpers
from datetime import date
from utils import usage_path, fmt_path

ES_URL   = "http://localhost:9200"
IDX_TEAM = "cricket_team_insights_v1"
IDX_MATCH = "cricket_matches_v1"
IDX_NEWS  = "cricket_news_v1"
IDX_SERIES = "cricket_series_v1"
IDX_TEAMS = "cricket_teams_v1"
IDX_PLAYERS = "cricket_players_v1"


def _create_index_if_missing(es: Elasticsearch, index: str, mappings: dict):
    if not es.indices.exists(index=index):
        es.indices.create(index=index, mappings=mappings)
        print(f"Created index: {index}")


def _bulk_index(es: Elasticsearch, actions: list):
    ok, errors = helpers.bulk(es, actions, raise_on_error=False)
    print(f"  Indexed {ok} docs. Errors: {len(errors)}")
    for e in errors:
        print("  ERR:", e)


def index_to_elastic():
    today = str(date.today())
    es    = Elasticsearch(ES_URL)

    # ── 1. Team Insights (combination layer) ──────────────────────────────────
    _create_index_if_missing(es, IDX_TEAM, {
        "properties": {
            "team":           {"type": "keyword"},
            "avg_sentiment":  {"type": "float"},
            "win_rate":       {"type": "float"},
            "team_score":     {"type": "float"},
            "article_count":  {"type": "integer"},
            "matches_played": {"type": "integer"},
            "date":           {"type": "date", "format": "yyyy-MM-dd"},
        }
    })
    df_team = pd.read_parquet(usage_path("team_insights", today))
    _bulk_index(es, [{
        "_index": IDX_TEAM,
        "_id":    f"{r['team']}_{r['date']}",
        "_source": r,
    } for r in df_team.to_dict(orient="records")])

    # ── 2. Match-level data ───────────────────────────────────────────────────
    _create_index_if_missing(es, IDX_MATCH, {
        "properties": {
            "match_id":    {"type": "keyword"},
            "series_name": {"type": "text",    "fields": {"kw": {"type": "keyword"}}},
            "match_type":  {"type": "keyword"},
            "team1":       {"type": "keyword"},
            "team2":       {"type": "keyword"},
            "status":      {"type": "text"},
            "venue":       {"type": "keyword"},
            "city":        {"type": "keyword"},
            "start_date":  {"type": "date"},
            "team1_runs":  {"type": "integer"},
            "team1_wkts":  {"type": "integer"},
            "team2_runs":  {"type": "integer"},
            "team2_wkts":  {"type": "integer"},
            "ingested_at": {"type": "date"},
        }
    })
    df_match = pd.read_parquet(fmt_path("cricbuzz", "matches", today))
    # Convert timestamps to ISO 8601 format for Elasticsearch
    for col in ["start_date", "ingested_at"]:
        if col in df_match.columns:
            df_match[col] = pd.to_datetime(df_match[col], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    _bulk_index(es, [{
        "_index": IDX_MATCH,
        "_id":    str(r.get("match_id", i)),
        "_source": {k: (None if str(v) in ("nan","NaT","None") else v)
                    for k, v in r.items()},
    } for i, r in enumerate(df_match.to_dict(orient="records"))])

    # ── 3. News sentiment data ────────────────────────────────────────────────
    _create_index_if_missing(es, IDX_NEWS, {
        "properties": {
            "source":       {"type": "keyword"},
            "title":        {"type": "text"},
            "description":  {"type": "text"},
            "url":          {"type": "keyword"},
            "published_at": {"type": "date"},
            "sentiment":    {"type": "float"},
            "subjectivity": {"type": "float"},
            "ingested_at":  {"type": "date"},
        }
    })
    df_news = pd.read_parquet(fmt_path("news", "sentiment", today))
    for col in ["published_at", "ingested_at"]:
        if col in df_news.columns:
            # Convert to datetime and then to ISO 8601 format for Elasticsearch
            df_news[col] = pd.to_datetime(df_news[col], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    _bulk_index(es, [{
        "_index": IDX_NEWS,
        "_id":    f"{today}_{i}",
        "_source": {k: (None if str(v) in ("nan","NaT","None") else v)
                    for k, v in r.items()},
    } for i, r in enumerate(df_news.to_dict(orient="records"))])

    # ── 4. Series data ───────────────────────────────────────────────────────
    _create_index_if_missing(es, IDX_SERIES, {
        "properties": {
            "series_id":    {"type": "keyword"},
            "name":         {"type": "text", "fields": {"kw": {"type": "keyword"}}},
            "start_date":   {"type": "date"},
            "end_date":     {"type": "date"},
            "series_type":  {"type": "keyword"},
            "odi_count":    {"type": "integer"},
            "t20_count":    {"type": "integer"},
            "test_count":   {"type": "integer"},
            "ingested_at":  {"type": "date"},
        }
    })
    df_series = pd.read_parquet(fmt_path("cricbuzz", "series", today))
    for col in ["start_date", "end_date", "ingested_at"]:
        if col in df_series.columns:
            df_series[col] = pd.to_datetime(df_series[col], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    _bulk_index(es, [{
        "_index": IDX_SERIES,
        "_id":    str(r.get("series_id", i)),
        "_source": {k: (None if str(v) in ("nan","NaT","None") else v)
                    for k, v in r.items()},
    } for i, r in enumerate(df_series.to_dict(orient="records"))])

    # ── 5. Teams data ────────────────────────────────────────────────────────
    _create_index_if_missing(es, IDX_TEAMS, {
        "properties": {
            "team_id":      {"type": "keyword"},
            "team_name":    {"type": "text", "fields": {"kw": {"type": "keyword"}}},
            "image_id":     {"type": "keyword"},
            "country":      {"type": "keyword"},
            "ingested_at":  {"type": "date"},
        }
    })
    df_teams = pd.read_parquet(fmt_path("cricbuzz", "teams", today))
    for col in ["ingested_at"]:
        if col in df_teams.columns:
            df_teams[col] = pd.to_datetime(df_teams[col], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    _bulk_index(es, [{
        "_index": IDX_TEAMS,
        "_id":    str(r.get("team_id", i)),
        "_source": {k: (None if str(v) in ("nan","NaT","None") else v)
                    for k, v in r.items()},
    } for i, r in enumerate(df_teams.to_dict(orient="records"))])

    # ── 6. Players data ───────────────────────────────────────────────────────
    _create_index_if_missing(es, IDX_PLAYERS, {
        "properties": {
            "player_id":       {"type": "keyword"},
            "name":            {"type": "text", "fields": {"kw": {"type": "keyword"}}},
            "country":         {"type": "keyword"},
            "role":            {"type": "keyword"},
            "batting_style":   {"type": "keyword"},
            "bowling_style":   {"type": "keyword"},
            "ingested_at":     {"type": "date"},
        }
    })
    df_players = pd.read_parquet(fmt_path("cricbuzz", "players", today))
    for col in ["ingested_at"]:
        if col in df_players.columns:
            df_players[col] = pd.to_datetime(df_players[col], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    _bulk_index(es, [{
        "_index": IDX_PLAYERS,
        "_id":    str(r.get("player_id", i)),
        "_source": {k: (None if str(v) in ("nan","NaT","None") else v)
                    for k, v in r.items()},
    } for i, r in enumerate(df_players.to_dict(orient="records"))])

    print(f"\n✅ All data indexed for {today}")


if __name__ == "__main__":
    index_to_elastic()