import pandas as pd
from elasticsearch import Elasticsearch, helpers
from datetime import date
from utils import usage_path

def index_to_elastic():
    today = str(date.today())
    df = pd.read_parquet(usage_path("team_insights", today))

    es = Elasticsearch("http://localhost:9200")

    INDEX = "team_insights_v1"
    if not es.indices.exists(index=INDEX):
        es.indices.create(index=INDEX, body={
            "mappings": { "properties": {
                "team":           {"type": "keyword"},
                "avg_sentiment":  {"type": "float"},
                "win_rate":       {"type": "float"},
                "team_score":     {"type": "float"},
                "article_count":  {"type": "integer"},
                "matches_played": {"type": "integer"},
                "date":           {"type": "date", "format": "yyyy-MM-dd"}
            }}
        })
        print(f"Created index: {INDEX}")

    records = df.to_dict(orient="records")
    actions = [{
        "_index": INDEX,
        "_id": f"{r['team']}_{r['date']}",
        "_source": r
    } for r in records]

    ok, errors = helpers.bulk(es, actions, raise_on_error=False)
    print(f"Indexed {ok} records. Errors: {len(errors)}")

if __name__ == "__main__":
    index_to_elastic()