"""
cricket_sentiment_pipeline
--------------------------
End-to-end daily pipeline (Airflow DAG):

  ingest_cricbuzz  → format_cricbuzz ─┐
                                       ├─→ combine → index_elastic
  ingest_news      → format_news     ─┘

Data Lake folder convention:
  data/<layer>/<group>/<dataEntity>/<dateVersion>/
  - raw/cricbuzz/matches/YYYY-MM-DD/
  - raw/news/articles/YYYY-MM-DD/
  - formatted/cricbuzz/matches/YYYY-MM-DD/
  - formatted/news/sentiment/YYYY-MM-DD/
  - usage/team_insights/YYYY-MM-DD/
"""

import sys, os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Make project root importable from inside the dags/ subdirectory
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Force JAVA_HOME to Java 17 for PySpark/Hadoop compatibility on Java 23+ host
os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home"
# Disable Java security manager to fix "getSubject is not supported" error
os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.driver.extraJavaOptions=-Djava.security.manager=allow --conf spark.executor.extraJavaOptions=-Djava.security.manager=allow pyspark-shell"

# ── task callables ────────────────────────────────────────────────────────────

def _ingest_cricbuzz():
    from ingest_cricbuzz import ingest_all
    ingest_all()

def _ingest_news():
    from ingest_news import ingest_news
    ingest_news()

def _format_cricbuzz():
    from format_cricbuzz import format_all
    format_all()

def _format_news():
    from format_news import format_news
    format_news()

def _combine():
    from combine import combine
    combine()

def _index_elastic():
    from index_elastic import index_to_elastic
    index_to_elastic()

# ── DAG definition ────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="cricket_sentiment_pipeline",
    description=(
        "Cricket Big Data pipeline: "
        "Ingest (Cricbuzz + NewsAPI) → Format (parquet/UTC) "
        "→ Combine (team score) → Index (Elasticsearch)"
    ),
    schedule="@daily",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    default_args=default_args,
    tags=["cricket", "sentiment", "elasticsearch", "big-data"],
) as dag:

    t_ingest_cricbuzz = PythonOperator(
        task_id="ingest_cricbuzz",
        python_callable=_ingest_cricbuzz,
    )

    t_ingest_news = PythonOperator(
        task_id="ingest_news",
        python_callable=_ingest_news,
    )

    t_format_cricbuzz = PythonOperator(
        task_id="format_cricbuzz",
        python_callable=_format_cricbuzz,
    )

    t_format_news = PythonOperator(
        task_id="format_news",
        python_callable=_format_news,
    )

    t_combine = PythonOperator(
        task_id="combine",
        python_callable=_combine,
    )

    t_index = PythonOperator(
        task_id="index_elastic",
        python_callable=_index_elastic,
    )

    # ── task dependencies ─────────────────────────────────────────────────────
    t_ingest_cricbuzz >> t_format_cricbuzz >> t_combine
    t_ingest_news     >> t_format_news     >> t_combine
    t_combine         >> t_index
