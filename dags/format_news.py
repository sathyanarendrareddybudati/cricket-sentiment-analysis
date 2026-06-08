from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_utc_timestamp, current_timestamp
from pyspark.sql.types import FloatType, StructType, StructField, StringType
from textblob import TextBlob
import json
from datetime import date
from utils import raw_path, fmt_path

@udf(returnType=FloatType())
def get_sentiment(text):
    try:
        return float(TextBlob(text or "").sentiment.polarity)
    except:
        return 0.0

@udf(returnType=FloatType())
def get_subjectivity(text):
    try:
        return float(TextBlob(text or "").sentiment.subjectivity)
    except:
        return 0.0

def format_news():
    spark = SparkSession.builder \
        .appName("FormatNews") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    today = str(date.today())
    src = raw_path("news", "articles", today)

    with open(f"{src}/articles.json") as f:
        raw = json.load(f)

    rows = []
    for a in raw.get("articles", []):
        title = a.get("title", "") or ""
        desc  = a.get("description", "") or ""
        rows.append((
            a.get("source", {}).get("name", ""),
            title,
            desc,
            title + " " + desc,
            a.get("url", ""),
            a.get("publishedAt", ""),
        ))

    schema = StructType([
        StructField("source",       StringType()),
        StructField("title",        StringType()),
        StructField("description",  StringType()),
        StructField("full_text",    StringType()),
        StructField("url",          StringType()),
        StructField("published_at_raw", StringType()),
    ])

    df = spark.createDataFrame(rows, schema)

    # Apply sentiment UDF (Spark distributes this across workers)
    df = df.withColumn("sentiment",    get_sentiment(col("full_text"))) \
           .withColumn("subjectivity", get_subjectivity(col("full_text"))) \
           .withColumn("published_at", to_utc_timestamp(
               col("published_at_raw"), "UTC")) \
           .withColumn("ingested_at", current_timestamp()) \
           .drop("full_text", "published_at_raw")

    out = fmt_path("news", "sentiment", today)
    df.write.mode("overwrite").parquet(out)
    print(f"Spark formatted {df.count()} articles with sentiment → {out}")

    spark.stop()

if __name__ == "__main__":
    format_news()