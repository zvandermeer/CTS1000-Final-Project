import asyncio
import json
import os
import sqlite3
import sys
import threading
import time
from configparser import RawConfigParser
from datetime import datetime
from random import randint

import requests
import torch
from transformers import pipeline
from twikit import Client, TooManyRequests
import urllib

dbFilename = ""

# load sentiment analysis AI
mps_device = torch.device("mps")
sentiment_pipeline = pipeline("text-classification", model="cardiffnlp/twitter-xlm-roberta-base-sentiment", device=mps_device)

location_queue = []
finished_obtaining_tweets = False

# build a query from given searchData
def buildQuery(searchData):
    query = ""

    for term in searchData["exactTerms"]:
        query += f"{term} "
    
    if searchData["exactPhrase"] != "":
        query += f"\"{searchData['exactPhrase']}\" "

    if len(searchData["anyTerms"]) > 0:
        query += "("

        for i in range(len(searchData["anyTerms"])):
            query += searchData["anyTerms"][i]

            if i != len(searchData["anyTerms"]) - 1:
                query += " OR "

        query += ") "

    for term in searchData["notTerms"]:
        query += f"-{term} "

    query += "lang:en "

    return query


# Create new database
def createDatabase():
    connection = sqlite3.connect(dbFilename)
    cursor = connection.cursor()

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

    print(f"Database \"{dbFilename}\" has been created")


# If this is a new query, create a new entry in database, otherwise return ID of previous query
def getQueryId(query, searchData):
    connection = sqlite3.connect(dbFilename)
    cursor = connection.cursor()

    queryId = cursor.execute("SELECT id FROM SEARCHES WHERE fullQuery = ?", (query,)).fetchone()

    if(queryId == None):
        cursor.execute("INSERT INTO SEARCHES (anyTerms, exactTerms, exactPhrase, notTerms, fullQuery) VALUES (?,?,?,?,?)", 
                       (str(searchData["anyTerms"]), str(searchData["exactTerms"]), searchData["exactPhrase"], str(searchData["notTerms"]), query))
        
        id = cursor.lastrowid

        connection.commit()

    else:
        id = queryId[0]

    connection.close()

    return id

        
# Scrape data from twitter
async def scrape(query: str, minTweets: int, queryId: int):
    client = Client(language='en-US')

    # If cookies.json exists, login using cookies, otherwise login using credentials and save cookies
    if os.path.isfile('cookies.json'):
        client.load_cookies('cookies.json')
    else:
        if not os.path.isfile("auth.ini"):
            print("auth.ini not found and cookies.json not found")
            sys.exit()

        config = RawConfigParser()
        config.read('auth.ini')
        username = config['Login']['username']
        email = config['Login']['email']
        password = config['Login']['password']

        await client.login(auth_info_1=username, auth_info_2=email, password=password)
        client.save_cookies('cookies.json')
    
    for i in range(3):
        tweet_count = 0
        tweets = None

        if(i == 0):
            print(f'{datetime.now()} - Processing 2010-2014')
            newQuery = query + "until:2015-01-01 since:2010-01-01"
        elif(i == 1):
            print(f'{datetime.now()} - Processing 2015-2019')
            newQuery = query + "until:2020-01-01 since:2015-01-01"
        else:
            print(f'{datetime.now()} - Processing 2020-2024')
            newQuery = query + "until:2025-01-01 since:2020-01-01"

        # Loop until the desired number of tweets has been reached
        while tweet_count < minTweets:

            # Searches tweets from twitter
            try:
                if tweets is None:
                    print(f'{datetime.now()} - Getting tweets...')
                    tweets = await client.search_tweet(newQuery, product='Top')
                else:
                    wait_time = randint(5, 10)
                    print(f'{datetime.now()} - Getting next tweets after {wait_time} seconds...')
                    time.sleep(wait_time)
                    tweets = await tweets.next()

            # If hit rate limit, wait until rate limit expires
            except TooManyRequests as e:
                rate_limit_reset = datetime.fromtimestamp(e.rate_limit_reset)
                print(f'{datetime.now()} - Rate limit reached. Waiting until {rate_limit_reset}')
                wait_time = rate_limit_reset - datetime.now()
                time.sleep(wait_time.total_seconds())

                wait_time = randint(5, 10)
                print(f'{datetime.now()} - Getting next tweets after {wait_time} seconds ...')
                time.sleep(wait_time)
                continue

            if not tweets:
                print(f'{datetime.now()} - No more tweets found')
                break

            tweet_count += len(tweets)

            # Start thread to process received batch of tweets
            processBatchThread = threading.Thread(target=processBatch, args=(tweets, queryId))
            processBatchThread.start()

            print(f'{datetime.now()} - Got {len(tweets)} new tweets')

        print(f'{datetime.now()} - Done! Got {tweet_count} tweets total')

    # Indicate all tweets have been fetched
    global finished_obtaining_tweets
    finished_obtaining_tweets = True


# Process a batch of tweets
def processBatch(tweets, queryId):
    # Create new connection to the database
    connection = sqlite3.connect(dbFilename)
    cursor = connection.cursor()

    fullTweetDataList = []

    for tweet in tweets:
        # Ignore any accounts explicitly labelled "parody"
        if "parody" in tweet.user.name.lower():
            continue

        # Ignore if tweet is not in supported sentiment analysis languages
        if tweet.lang.lower() not in ["ar", "en", "fr", "de", "hi", "it", "sp", "pt"]:
            continue

        date_format_str = "%a %b %d %H:%M:%S %z %Y"

        # Configure dictionary of tweet data
        myTweetDict = {
            "queryId": queryId,
            "tweetId": tweet.id,
            "lang": tweet.lang,
            "username": tweet.user.name,
            "location": tweet.user.location,
            "content": tweet.text,
            "datetime": datetime.strptime(tweet.created_at, date_format_str),
            "retweets": tweet.retweet_count,
            "likes": tweet.favorite_count
        }

        # Insert tweet into database
        cursor.execute("INSERT INTO TWEETS (queryId, tweetId, language, username, originalLocation, content, creationDate, retweets, likes) VALUES (?,?,?,?,?,?,?,?,?)", tuple(myTweetDict.values()))

        myTweetDict["id"] = cursor.lastrowid

        # Add tweet to list for sentiment analysis
        fullTweetDataList.append(myTweetDict)

        # Add tweet to location queue
        location_queue.append({
            "id": cursor.lastrowid,
            "location": tweet.user.location
        })

    # Commit changes to the database
    connection.commit()

    # Get content of all tweets in batch
    tweetContent = [d['content'] for d in fullTweetDataList]

    # Process sentiment for all tweets in batch
    sentiment_data = sentiment_pipeline(tweetContent)

    # Update values in each tweet dictionary
    for i in range (len(tweetContent)):
        fullTweetDataList[i]["sentiment"] = sentiment_data[i]["label"]
        fullTweetDataList[i]["confidence"] = sentiment_data[i]["score"]

    # Update data in database
    for tweet in fullTweetDataList:
        cursor.execute("UPDATE TWEETS SET sentiment = ?, confidence = ? WHERE id = ?", (tweet["sentiment"], tweet["confidence"], tweet["id"]))

    # Commit changes to database and close the connection
    connection.commit()
    connection.close()


# Process location for each tweet
def processLocation():
    # Connect to the database
    connection = sqlite3.connect(dbFilename)
    cursor = connection.cursor()

    # Base url for the request
    baseUrl = "https://nominatim.openstreetmap.org/search?format=json&accept-language=en&addressdetails=1&limit=1&q="

    # Loop until all tweets have been processed
    while(not finished_obtaining_tweets or len(location_queue) > 0):
        # If there is an item on the queue, remove it and start processing
        if(len(location_queue) > 0):
            myItem = location_queue.pop(0)

            # Process if the location is not empty
            if(myItem['location'] != ""):
                print(f'{datetime.now()} - Processing location for \"{myItem["location"]}\"')

                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}

                parsedLocation = urllib.parse.quote_plus(myItem['location'], safe='')

                locationData = json.loads(requests.get(baseUrl+parsedLocation, headers=headers).content)

                if(locationData != []):
                    locationData = locationData[0]

                    if not 'ISO3166-2-lvl4' in locationData['address'].keys():
                        locationData['address']['ISO3166-2-lvl4'] = ""

                    cursor.execute(
                        "UPDATE TWEETS SET locationLatitude = ?, locationLongitude = ?, countryCode = ?, stateCode = ?, validLocation = 1 WHERE id = ?", 
                        (locationData["lat"], locationData["lon"], locationData["address"]["country_code"], locationData["address"]["ISO3166-2-lvl4"], myItem['id']))

                    connection.commit()
                else:
                    cursor.execute("UPDATE TWEETS SET validLocation = 0 WHERE id = ?", (myItem['id'],))

                    connection.commit()

            # Wait 1 second to ensure API is not overused
            time.sleep(1.1)

    connection.close()
    

if __name__ == "__main__":
    # make sure database filename has been passed as CLI argument
    try:
        if sys.argv[1] == "":
            print("Please enter a filename for the database")
            sys.exit()
    except IndexError:
        print("Please enter a filename for the database")
        sys.exit()

    dbFilename = sys.argv[1]

    # start thread to process location
    try:
        if sys.argv[2].lower() == "true":
            processLocationThread = threading.Thread(target=processLocation, args=())
            processLocationThread.start()
    except IndexError:
        pass

    # check for required files
    if not os.path.isfile("search.json"):
        print("search.json not found")
        sys.exit()

    with open('search.json') as f:
        searchData = json.load(f)

    if not os.path.isfile(dbFilename):
        createDatabase()

    # build query from search.json
    query = buildQuery(searchData)
    
    # check if query has already been made
    queryId = getQueryId(query, searchData)

    # start scraping data
    asyncio.run(scrape(query, searchData["minTweets"], queryId))