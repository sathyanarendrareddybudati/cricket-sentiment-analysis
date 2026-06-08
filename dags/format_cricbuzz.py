from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_utc_timestamp, lit, current_timestamp
import json
from datetime import date
from utils import raw_path, fmt_path

def format_matches():
    spark = SparkSession.builder \
        .appName("FormatCricbuzz") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    today = str(date.today())
    src = raw_path("cricbuzz", "matches", today)

    with open(f"{src}/matches.json") as f:
        raw = json.load(f)

    rows = []
    for type_match in raw.get("typeMatches", []):
        for series in type_match.get("seriesMatches", []):
            wrapper = series.get("seriesAdWrapper", {})
            for match in wrapper.get("matches", []):
                info  = match.get("matchInfo", {})
                score = match.get("matchScore", {})
                t1s   = score.get("team1Score", {}).get("inngs1", {})
                t2s   = score.get("team2Score", {}).get("inngs1", {})
                rows.append((
                    str(info.get("matchId", "")),
                    info.get("team1", {}).get("teamName", ""),
                    info.get("team2", {}).get("teamName", ""),
                    info.get("status", ""),
                    info.get("venueInfo", {}).get("ground", ""),
                    int(info.get("startDate", 0) or 0),
                    int(t1s.get("runs") or 0),
                    int(t1s.get("wickets") or 0),
                    int(t2s.get("runs") or 0),
                    int(t2s.get("wickets") or 0),
                ))

    from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
    schema = StructType([
        StructField("match_id",    StringType()),
        StructField("team1",       StringType()),
        StructField("team2",       StringType()),
        StructField("status",      StringType()),
        StructField("venue",       StringType()),
        StructField("start_epoch", LongType()),
        StructField("team1_runs",  IntegerType()),
        StructField("team1_wkts",  IntegerType()),
        StructField("team2_runs",  IntegerType()),
        StructField("team2_wkts",  IntegerType()),
    ])

    df = spark.createDataFrame(rows, schema)

    df = df.withColumn(
        "start_date",
        to_utc_timestamp(from_unixtime(col("start_epoch") / 1000), "UTC")
    ).withColumn("ingested_at", current_timestamp()) \
     .drop("start_epoch")

    out = fmt_path("cricbuzz", "matches", today)
    df.write.mode("overwrite").parquet(out)
    print(f"Spark formatted {df.count()} matches → {out}")

    spark.stop()

def format_series():
    spark = SparkSession.builder \
        .appName("FormatSeries") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    today = str(date.today())
    src = raw_path("cricbuzz", "series", today)

    with open(f"{src}/series.json") as f:
        raw = json.load(f)

    rows = []
    for series in raw.get("series", []):
        rows.append((
            str(series.get("id", "")),
            series.get("name", ""),
            series.get("startDate", ""),
            series.get("endDate", ""),
            series.get("seriesType", ""),
            series.get("odi", 0),
            series.get("t20", 0),
            series.get("test", 0),
        ))

    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    schema = StructType([
        StructField("series_id",     StringType()),
        StructField("name",          StringType()),
        StructField("start_date",    StringType()),
        StructField("end_date",      StringType()),
        StructField("series_type",   StringType()),
        StructField("odi_count",     IntegerType()),
        StructField("t20_count",     IntegerType()),
        StructField("test_count",    IntegerType()),
    ])

    df = spark.createDataFrame(rows, schema)
    df = df.withColumn("ingested_at", current_timestamp())

    out = fmt_path("cricbuzz", "series", today)
    df.write.mode("overwrite").parquet(out)
    print(f"Spark formatted {df.count()} series → {out}")

    spark.stop()

def format_teams():
    spark = SparkSession.builder \
        .appName("FormatTeams") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    today = str(date.today())
    src = raw_path("cricbuzz", "teams", today)

    with open(f"{src}/teams.json") as f:
        raw = json.load(f)

    rows = []
    for team in raw.get("team", []):
        rows.append((
            str(team.get("id", "")),
            team.get("teamName", ""),
            team.get("imageId", ""),
            team.get("country", ""),
        ))

    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("team_id",    StringType()),
        StructField("team_name",  StringType()),
        StructField("image_id",   StringType()),
        StructField("country",    StringType()),
    ])

    df = spark.createDataFrame(rows, schema)
    df = df.withColumn("ingested_at", current_timestamp())

    out = fmt_path("cricbuzz", "teams", today)
    df.write.mode("overwrite").parquet(out)
    print(f"Spark formatted {df.count()} teams → {out}")

    spark.stop()

def format_players():
    spark = SparkSession.builder \
        .appName("FormatPlayers") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    today = str(date.today())
    src = raw_path("cricbuzz", "players", today)

    with open(f"{src}/players.json") as f:
        raw = json.load(f)

    rows = []
    for player in raw.get("player", []):
        rows.append((
            str(player.get("id", "")),
            player.get("name", ""),
            player.get("country", ""),
            player.get("role", ""),
            player.get("battingStyle", ""),
            player.get("bowlingStyle", ""),
        ))

    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("player_id",       StringType()),
        StructField("name",            StringType()),
        StructField("country",         StringType()),
        StructField("role",            StringType()),
        StructField("batting_style",   StringType()),
        StructField("bowling_style",   StringType()),
    ])

    df = spark.createDataFrame(rows, schema)
    df = df.withColumn("ingested_at", current_timestamp())

    out = fmt_path("cricbuzz", "players", today)
    df.write.mode("overwrite").parquet(out)
    print(f"Spark formatted {df.count()} players → {out}")

    spark.stop()

def format_all():
    format_matches()
    format_series()
    format_teams()
    format_players()

if __name__ == "__main__":
    format_all()