import os
import sqlite3
import sys


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

rows = cursor.execute("SELECT tweetId, COUNT(*) c FROM TWEETS GROUP BY tweetId HAVING c > 1;").fetchall()

for item in rows:
    duplicates = cursor.execute("SELECT id FROM TWEETS WHERE tweetId = ?", (item[0],)).fetchall()

    for i in range(len(duplicates)):
        if(i != 0):
            cursor.execute("DELETE FROM TWEETS WHERE id = ?", (duplicates[i][0],))

connection.commit()
connection.close()

print("Duplicates removed")