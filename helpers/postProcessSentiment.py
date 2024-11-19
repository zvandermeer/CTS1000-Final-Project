import os
import sqlite3
import sys

import torch
from transformers import pipeline

mps_device = torch.device("mps")
sentiment_pipeline = pipeline("text-classification", model="cardiffnlp/twitter-xlm-roberta-base-sentiment", device=mps_device)

try:
    if sys.argv[1] == "":
        print("Please enter a filename")
        sys.exit()
except IndexError:
    print("Please enter a filename")
    sys.exit()

filename = sys.argv[1]

if not os.path.isfile(filename):
    print(f"Could not find database \"{filename}\"")
    sys.exit()

connection = sqlite3.connect(filename)
cursor = connection.cursor()

rows = cursor.execute("SELECT id, content FROM TWEETS WHERE sentiment IS NULL").fetchall()

tweetContent = [i[1] for i in rows]

sentiment_data = sentiment_pipeline(tweetContent)

query = "UPDATE TWEETS SET sentiment = ?, confidence = ? WHERE id = ?"

for i in range (len(sentiment_data)):
    cursor.execute(query, (sentiment_data[i]["label"], sentiment_data[i]["score"], rows[i][0]))

connection.commit()

connection.close()