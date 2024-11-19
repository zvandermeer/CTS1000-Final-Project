from datetime import datetime
import os
import sqlite3
import sys
import time

from geopy.geocoders import Nominatim

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

rows = cursor.execute("SELECT id, originalLocation FROM TWEETS WHERE locationLatitude IS NULL AND originalLocation IS NOT \"\"").fetchall()

geolocator = Nominatim(user_agent="CTS1000-Final-Project")

for item in rows:
    print(f'{datetime.now()} - Processing location for \"{item[1]}\"')

    location = geolocator.geocode(item[1])

    if(location != None):
        cursor.execute("UPDATE TWEETS SET locationLatitude = ?, locationLongitude = ? WHERE id = ?", (location.latitude, location.longitude, item[0]))

        connection.commit()

    time.sleep(1.1)