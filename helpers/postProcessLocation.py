# Post-process the location for tweets, if not processed during initial scraping

from datetime import datetime
import os
import sqlite3
import sys
import time
import requests
import json
import urllib.parse

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

baseUrl = "https://nominatim.openstreetmap.org/search?format=json&accept-language=en&addressdetails=1&limit=1&q="

rows = cursor.execute("SELECT id, originalLocation FROM TWEETS WHERE (locationLatitude IS NULL OR countryCode IS NULL OR stateCode IS NULL) AND originalLocation IS NOT \"\" AND validLocation IS NULL").fetchall()

for item in rows:
    print(f'{datetime.now()} - Processing location for \"{item[1]}\"')

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}

    parsedLocation = urllib.parse.quote_plus(item[1], safe='')

    locationData = json.loads(requests.get(baseUrl+parsedLocation, headers=headers).content)

    if(locationData != []):
        locationData = locationData[0]

        if not 'ISO3166-2-lvl4' in locationData['address'].keys():
            locationData['address']['ISO3166-2-lvl4'] = ""

        cursor.execute(
            "UPDATE TWEETS SET locationLatitude = ?, locationLongitude = ?, countryCode = ?, stateCode = ?, validLocation = 1 WHERE id = ?", 
            (locationData["lat"], locationData["lon"], locationData["address"]["country_code"], locationData["address"]["ISO3166-2-lvl4"], item[0]))

        connection.commit()
    else:
        cursor.execute("UPDATE TWEETS SET validLocation = 0 WHERE id = ?", (item[0],))

        connection.commit()

    time.sleep(1.1)

connection.close()