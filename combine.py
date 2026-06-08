from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as spark_sum, count, lit, round as spark_round
from datetime import date
from utils import fmt_path, usage_path

def combine():
    spark = SparkSession.builder \
        .appName("CombineCricketSentiment") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    today = str(date.today())

    # Read formatted Parquet files
    matches = spark.read.parquet(fmt_path("cricbuzz", "matches", today))
    news    = spark.read.parquet(fmt_path("news", "sentiment", today))

    # Step 1 — compute avg sentiment per team
    # Get all team names from matches
    team1_df = matches.select(col("team1").alias("team"))
    team2_df = matches.select(col("team2").alias("team"))
    all_teams = team1_df.union(team2_df).distinct().filter(col("team") != "")

    # Cross join teams with news, filter where team name appears in title
    from pyspark.sql.functions import lower, instr
    news_teams = all_teams.crossJoin(
        news.select("title", "sentiment")
    ).filter(
        instr(lower(col("title")), lower(col("team"))) > 0
    )

    team_sentiment = news_teams.groupBy("team").agg(
        spark_round(avg("sentiment"), 4).alias("avg_sentiment"),
        count("sentiment").alias("article_count")
    )

    # Step 2 — compute win rate per team
    from pyspark.sql.functions import when
    team1_stats = matches.groupBy("team1").agg(
        count("*").alias("played"),
        spark_sum(when(lower(col("status")).contains(lower(col("team1"))) &
                       lower(col("status")).contains("won"), 1).otherwise(0)).alias("wins")
    ).withColumnRenamed("team1", "team")

    team2_stats = matches.groupBy("team2").agg(
        count("*").alias("played"),
        spark_sum(when(lower(col("status")).contains(lower(col("team2"))) &
                       lower(col("status")).contains("won"), 1).otherwise(0)).alias("wins")
    ).withColumnRenamed("team2", "team")

    from pyspark.sql.functions import col as c
    team_perf = team1_stats.union(team2_stats).groupBy("team").agg(
        spark_sum("played").alias("matches_played"),
        spark_sum("wins").alias("total_wins")
    ).withColumn(
        "win_rate",
        spark_round(col("total_wins") / col("matches_played"), 4)
    )

    # Step 3 — join and compute composite team_score
    combined = team_sentiment.join(team_perf, on="team", how="outer") \
        .fillna(0) \
        .withColumn("team_score",
            spark_round(col("win_rate") * 0.6 + col("avg_sentiment") * 0.4, 4)
        ) \
        .withColumn("date", lit(today)) \
        .orderBy(col("team_score").desc())

    combined.show(15)

    out = usage_path("team_insights", today)
    combined.write.mode("overwrite").parquet(out)
    print(f"Spark combined {combined.count()} teams → {out}")

    spark.stop()

if __name__ == "__main__":
    combine()