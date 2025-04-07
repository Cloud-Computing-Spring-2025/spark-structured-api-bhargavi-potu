import pandas as pd
import random
from datetime import datetime, timedelta

users = [f"user_{i}" for i in range(1, 51)]
songs = [f"song_{i}" for i in range(1, 101)]

data = []
start_date = datetime(2025, 3, 20)

for _ in range(1000):
    user = random.choice(users)
    song = random.choice(songs)
    timestamp = start_date + timedelta(minutes=random.randint(0, 10000))
    duration = random.randint(30, 300)
    data.append([user, song, timestamp.strftime('%Y-%m-%d %H:%M:%S'), duration])

df = pd.DataFrame(data, columns=["user_id", "song_id", "timestamp", "duration_sec"])
df.to_csv("listening_logs.csv", index=False)