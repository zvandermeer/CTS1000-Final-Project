from datetime import datetime
import os
import sqlite3
import sys
import time
import requests
import json
import urllib.parse

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

rows = cursor.execute("SELECT id, originalLocation FROM TWEETS WHERE locationLatitude IS NULL AND originalLocation IS NOT \"\" AND validLocation IS NULL").fetchall()

for item in rows:
    print(f'{datetime.now()} - Processing location for \"{item[1]}\"')

    baseUrl = "https://nominatim.openstreetmap.org/search?q="

    parameters = urllib.parse.quote_plus(item[1], safe='')

    locationData = json.loads(requests.get(baseUrl+parameters+"&format=json&accept-language=en").content)

    if(locationData != []):
        cursor.execute("UPDATE TWEETS SET locationLatitude = ?, locationLongitude = ?, validLocation = 1 WHERE id = ?", (locationData[0]["lat"], locationData[0]["lon"], item[0]))

        connection.commit()
    else:
        cursor.execute("UPDATE TWEETS SET validLocation = 0 WHERE id = ?", (item[0],))

        connection.commit()

    time.sleep(1.1)

connection.close()