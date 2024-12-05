import sqlite3


db = sqlite3.connect('tweetDbTest.sqlite3')
db.row_factory = sqlite3.Row

dbCursor = db.cursor()

rows = dbCursor.execute('SELECT * FROM DATEDLOCATION').fetchall()

for item in rows:
    year = dbCursor.execute('SELECT year FROM YEARS WHERE id = ?', (item["associatedYear"],)).fetchone()["year"]
    locationCode = dbCursor.execute('SELECT locationCode FROM LOCATIONS WHERE id = ?', (item["associatedLocation"],)).fetchone()["locationCode"]
    tweets = dbCursor.execute('SELECT * FROM TWEETS WHERE creationDate LIKE ? AND (countryCode = ? OR stateCode = ?) AND sentiment = ?', (str(year) + '%', locationCode, locationCode, "negative")).fetchall()

    tweetCount = len(tweets)

    dbCursor.execute("UPDATE DATEDLOCATION SET negativeCount = ? WHERE id = ?", (tweetCount, item["id"]))

    tweets = dbCursor.execute('SELECT * FROM TWEETS WHERE creationDate LIKE ? AND (countryCode = ? OR stateCode = ?) AND sentiment = ?', (str(year) + '%', locationCode, locationCode, "neutral")).fetchall()

    tweetCount = len(tweets)

    dbCursor.execute("UPDATE DATEDLOCATION SET neutralCount = ? WHERE id = ?", (tweetCount, item["id"]))

    tweets = dbCursor.execute('SELECT * FROM TWEETS WHERE creationDate LIKE ? AND (countryCode = ? OR stateCode = ?) AND sentiment = ?', (str(year) + '%', locationCode, locationCode, "positive")).fetchall()

    tweetCount = len(tweets)

    dbCursor.execute("UPDATE DATEDLOCATION SET positiveCount = ? WHERE id = ?", (tweetCount, item["id"]))

db.commit()

db.close()