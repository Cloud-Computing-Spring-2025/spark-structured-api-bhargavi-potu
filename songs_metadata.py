import pandas as pd
import random

songs = [f"song_{i}" for i in range(1, 101)]
genres = ["Pop", "Rock", "Jazz", "Hip-Hop", "Classical"]
moods = ["Happy", "Sad", "Energetic", "Chill"]

data = []
for song in songs:
    title = f"Title_{song}"
    artist = f"Artist_{random.randint(1, 20)}"
    genre = random.choice(genres)
    mood = random.choice(moods)
    data.append([song, title, artist, genre, mood])

df = pd.DataFrame(data, columns=["song_id", "title", "artist", "genre", "mood"])
df.to_csv("songs_metadata.csv", index=False)