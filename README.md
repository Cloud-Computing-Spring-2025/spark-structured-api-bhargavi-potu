# 🎶 Spark-Based Music Insights Platform 🎶

This project dives into user listening habits using **PySpark Structured Streaming** and batch APIs. It simulates a music streaming platform, processes event logs, and provides insights about genres, moods, and user behavior.

---

## 📊 Datasets Used

### 🎧 `listening_logs.csv`
Tracks every time a user plays a song.

| Column         | Meaning                             |
|----------------|-------------------------------------|
| `user_id`      | Unique user identifier              |
| `song_id`      | Unique song identifier              |
| `timestamp`    | Datetime of the song play           |
| `duration_sec` | How long the song was played (sec)  |

---

### 🎼 `songs_metadata.csv`
Metadata for the full music catalog.

| Column     | Meaning                        |
|------------|--------------------------------|
| `song_id`  | Song identifier                |
| `title`    | Song title                     |
| `artist`   | Name of the artist             |
| `genre`    | Music genre                    |
| `mood`     | Mood of the song               |

---

## 🧠 Analysis Tasks & Output Samples

All results are saved inside the `output/` folder with subdirectories per task.

---

### 🎯 Task 1: User's Most Preferred Genre

📁 `output/user_favorite_genres/`

```
user_id	genre	play_count
user_1	Rock	5
user_10	Pop	7
user_16	Pop	9
user_18	Jazz	8
```

---

### ⏱️ Task 2: Average Listening Time per Song

📁 `output/avg_listen_time_per_song/`

```
song_id	avg_duration
song_19	180.8
song_6	208.57
song_100	161.17
song_38	188.82
```

---

### 🔝 Task 3: Top 10 Most Played Songs of the Week

📁 `output/top_songs_this_week/`

```
song_id	plays
song_37	9
song_69	9
song_44	9
song_84	7
song_71	7
```

---

### 😊 Task 4: Recommend “Happy” Songs to Sad Listeners

📁 `output/happy_recommendations/`

```
user_id	song_id	sad_count	title
user_14	song_43	2	Title_song_43
user_14	song_15	2	Title_song_15
user_14	song_51	2	Title_song_51
```

---

### 📈 Task 5: Genre Loyalty Score

📁 `output/genre_loyalty_scores/`

```
message
No users found with genre loyalty score above 0.8.
```

---

### 🌙 Task 6: Night Owl Listeners (12AM–5AM)

📁 `output/night_owl_users/`

```
user_id
user_14
user_22
user_5
user_1
user_10
user_28
```

---

### 🧩 Task 7: Enriched Logs (Logs + Metadata)

📁 `output/enriched_logs/`

```
song_id	user_id	timestamp	duration_sec	title	artist	genre	mood
song_49	user_40	2025-03-21T01:09:00Z	150	Title_song_49	Artist_5	Jazz	Sad
song_35	user_13	2025-03-26T05:35:00Z	214	Title_song_35	Artist_3	Rock	Happy
song_89	user_49	2025-03-22T09:34:00Z	122	Title_song_89	Artist_9	Pop	Energetic
```

---

## 🚀 How to Run

### Step 1: Generate the data

```bash
python generate_listening_logs.py
python generate_songs_metadata.py
```

### Step 2: Run the main analysis

```bash
spark-submit analysis.py
```

---

## 🛠️ Common Errors & Fixes

### ❌ Date format 'yyyy-ww' not recognized
✅ Use:
```python
from pyspark.sql.functions import weekofyear, year
```

---

### ❌ No output for loyalty score
✅ Added fallback:
```python
message_df = spark.createDataFrame([Row(message="No users found with genre loyalty score above 0.8.")])
```

---

### ❌ Too many CSV part files
✅ Use `.coalesce(1)` to write a single file:
```python
df.coalesce(1).write.mode("overwrite").csv(...)
```

---

## 🗂️ Project Structure

```
.
├── generate_listening_logs.py
├── generate_songs_metadata.py
├── analysis.py
├── README.md
└── output/
    ├── user_favorite_genres/
    ├── avg_listen_time_per_song/
    ├── top_songs_this_week/
    ├── happy_recommendations/
    ├── genre_loyalty_scores/
    ├── night_owl_users/
    └── enriched_logs/
```

---
