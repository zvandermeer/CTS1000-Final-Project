import sqlite3


db = sqlite3.connect('tweetDbTest.sqlite3')
db.row_factory = sqlite3.Row

dbCursor = db.cursor()

rows = dbCursor.execute("SELECT * FROM TWEETS").fetchall()

counter = 0

for item in rows:
    # if item["countryCode"] != None and item["countryCode"] != "":
    #     print(item["countryCode"])
    #     counter += 1

    #     newRows = dbCursor.execute("SELECT id FROM LOCATIONS WHERE locationCode = ?", (item["countryCode"],)).fetchone()

    #     if newRows == None:
    #         dbCursor.execute("INSERT INTO LOCATIONS (locationCode, locationType, positiveCount, neutralCount, negativeCount) VALUES (?,?,?,?,?)", (item["countryCode"], "country", 0,0,0))

    #         newRows = {}

    #         newRows["id"] = dbCursor.lastrowid

    #     if item["sentiment"] == "negative":
    #         dbCursor.execute("UPDATE LOCATIONS SET negativeCount = negativeCount + 1 WHERE id = ?", (newRows["id"],))
    #     elif item["sentiment"] == "neutral":
    #         dbCursor.execute("UPDATE LOCATIONS SET neutralCount = neutralCount + 1 WHERE id = ?", (newRows["id"],))
    #     else:
    #         dbCursor.execute("UPDATE LOCATIONS SET positiveCount = positiveCount + 1 WHERE id = ?", (newRows["id"],))

    #     db.commit()


    # year = int(item["creationDate"][:4])

    # print(year)
    # counter += 1

    # newRows = dbCursor.execute("SELECT id FROM YEARS WHERE year = ?", (year,)).fetchone()

    # if newRows == None:
    #     dbCursor.execute("INSERT INTO YEARS (year, positiveCount, neutralCount, negativeCount) VALUES (?,?,?,?)", (year, 0,0,0))

    #     newRows = {}

    #     newRows["id"] = dbCursor.lastrowid

    # if item["sentiment"] == "negative":
    #     dbCursor.execute("UPDATE YEARS SET negativeCount = negativeCount + 1 WHERE id = ?", (newRows["id"],))
    # elif item["sentiment"] == "neutral":
    #     dbCursor.execute("UPDATE YEARS SET neutralCount = neutralCount + 1 WHERE id = ?", (newRows["id"],))
    # else:
    #     dbCursor.execute("UPDATE YEARS SET positiveCount = positiveCount + 1 WHERE id = ?", (newRows["id"],))




db.close()

print(counter)