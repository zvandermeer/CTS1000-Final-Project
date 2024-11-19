import sqlite3
import sys

try:
    if sys.argv[1] == "":
        print("Please enter a database filename")
        sys.exit()
except IndexError:
    print("Please enter a database filename")
    sys.exit()

filename = sys.argv[1]

if(input(f"This will fully reset the database \"{filename}\". Are you sure you want to continue? (y/n): ").lower() == "y"):
    connection = sqlite3.connect(filename)
    cursor = connection.cursor()

    cursor.execute("DROP TABLE IF EXISTS SEARCHES")

    searchesTable = """ CREATE TABLE SEARCHES (
                        id INTEGER PRIMARY KEY,
                        anyTerms CHAR,
                        exactTerms CHAR,
                        exactPhrase CHAR,
                        notTerms CHAR,
                        dateStart DATE,
                        dateEnd DATE,
                        fullQuery CHAR);
    """

    cursor.execute(searchesTable)

    cursor.execute("DROP TABLE IF EXISTS TWEETS")

    tweetsTable = """   CREATE TABLE TWEETS (
                        id INTEGER PRIMARY KEY,
                        queryId INTEGER,
                        tweetId INTEGER,
                        language CHAR,
                        username CHAR,
                        originalLocation CHAR,
                        locationLatitude DOUBLE,
                        locationLongitude DOUBLE,
                        content CHAR,
                        creationDate TIMESTAMP,
                        retweets INTEGER,
                        likes INTEGER,
                        sentiment CHAR,
                        confidence DOUBLE,
                        FOREIGN KEY(queryId) REFERENCES SEARCHES(id));
    """

    cursor.execute(tweetsTable)

    connection.commit()

    connection.close()

    print(f"Database \"{filename}\" has been reset")