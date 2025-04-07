from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc, hour, date_format, when, max as spark_max
from pyspark.sql.window import Window
from pyspark.sql.functions import weekofyear, year
from pyspark.sql import Row

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

logs = spark.read.option("header", True).csv("listening_logs.csv")
songs = spark.read.option("header", True).csv("songs_metadata.csv")

logs = logs.withColumn("duration_sec", col("duration_sec").cast("int"))
logs = logs.withColumn("timestamp", col("timestamp").cast("timestamp"))
week_logs = logs.filter(
    (year("timestamp") == 2025) & (weekofyear("timestamp") == 13)
)

enriched_logs = logs.join(songs, "song_id")

# 1. User’s favorite genre
print("Task 1: User favorite genres")
user_fav_genre = (
    enriched_logs.groupBy("user_id", "genre")
    .agg(count("*").alias("play_count"))
    .withColumn("rank", spark_max("play_count").over(Window.partitionBy("user_id")))
    .filter("play_count == rank")
    .drop("rank")
)
user_fav_genre.write.mode("overwrite").csv("output/user_favorite_genres/", header=True)

# 2. Average listen time per song
print("Task 2: Avg listen time per song")
avg_listen = logs.groupBy("song_id").agg(avg("duration_sec").alias("avg_duration"))
avg_listen.write.mode("overwrite").csv("output/avg_listen_time_per_song/", header=True)

# 3. Top 10 most played songs this week
print("Task 3: Top 10 songs this week")
top_10 = (
    week_logs.groupBy("song_id")
    .agg(count("*").alias("plays"))
    .orderBy(desc("plays"))
    .limit(10)
)
top_10.write.mode("overwrite").csv("output/top_songs_this_week/", header=True)

# 4. Recommend Happy songs to Sad listeners
print("Task 4: Recommending happy songs to sad listeners")
sad_users = (
    enriched_logs.filter(col("mood") == "Sad")
    .groupBy("user_id")
    .agg(count("*").alias("sad_count"))
)

happy_songs = songs.filter(col("mood") == "Happy").select("song_id", "title").distinct()
played = logs.select("user_id", "song_id").distinct()

recommendations = (
    sad_users.join(happy_songs, how="cross")
    .join(played, on=["user_id", "song_id"], how="left_anti")
    .limit(3)
)
recommendations.write.mode("overwrite").csv("output/happy_recommendations/", header=True)

# 5. Genre loyalty score
# 5. Genre loyalty score
print("Task 5: Genre loyalty scores")
user_genre_counts = enriched_logs.groupBy("user_id", "genre").agg(count("*").alias("genre_plays"))
total_plays = enriched_logs.groupBy("user_id").agg(count("*").alias("total_plays"))

loyalty = user_genre_counts.join(total_plays, "user_id")
loyalty = loyalty.withColumn("loyalty_score", col("genre_plays") / col("total_plays"))

loyal_users = loyalty.filter(col("loyalty_score") > 0.8)

if loyal_users.count() == 0:
    print("No users found with genre loyalty score above 0.8.")
    
    message_df = spark.createDataFrame(
        [Row(message="No users found with genre loyalty score above 0.8.")]
    )
    message_df.coalesce(1).write.mode("overwrite").csv("output/genre_loyalty_scores/", header=True)
else:
    loyal_users.coalesce(1).write.mode("overwrite").csv("output/genre_loyalty_scores/", header=True)
    print("Loyal users written to output/genre_loyalty_scores/")

# 6. Night owl users
print("Task 6: Night owl users")
night_users = logs.withColumn("hour", hour("timestamp"))
night_users = night_users.filter((col("hour") >= 0) & (col("hour") < 5))
night_users.select("user_id").distinct().write.mode("overwrite").csv("output/night_owl_users/", header=True)

# 7. Enriched logs
print("Task 7: Saving enriched logs")
enriched_logs.write.mode("overwrite").csv("output/enriched_logs/", header=True)