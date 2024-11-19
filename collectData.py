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

import torch
from geopy.geocoders import Nominatim
from transformers import pipeline
from twikit import Client, TooManyRequests

filename = ""

mps_device = torch.device("mps")
sentiment_pipeline = pipeline("text-classification", model="cardiffnlp/twitter-xlm-roberta-base-sentiment", device=mps_device)

location_queue = []
finished_obtaining_tweets = False

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

    query += f"until:{searchData['dateEnd']} "

    query += f"since:{searchData['dateStart']}"

    return query


def createDatabase():
    connection = sqlite3.connect(filename)
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

    print(f"Database \"{filename}\" has been created")


def getQueryId(query, searchData):
    connection = sqlite3.connect(filename)
    cursor = connection.cursor()

    queryId = cursor.execute("SELECT id FROM SEARCHES WHERE fullQuery = ?", (query,)).fetchone()

    if(queryId == None):
        cursor.execute("INSERT INTO SEARCHES (anyTerms, exactTerms, exactPhrase, notTerms, dateStart, dateEnd, fullQuery) VALUES (?,?,?,?,?,?,?)", 
                       (str(searchData["anyTerms"]), str(searchData["exactTerms"]), searchData["exactPhrase"], str(searchData["notTerms"]), searchData["dateStart"], searchData["dateEnd"], query))
        
        id = cursor.lastrowid

        connection.commit()

    else:
        id = queryId[0]

    connection.close()

    return id
        

async def scrape(query: str, minTweets: int, queryId: int):
    client = Client(language='en-US')

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

    tweet_count = 0
    tweets = None

    while tweet_count < minTweets:
        try:
            if tweets is None:
                print(f'{datetime.now()} - Getting tweets...')
                tweets = await client.search_tweet(query, product='Top')
            else:
                wait_time = randint(5, 10)
                print(f'{datetime.now()} - Getting next tweets after {wait_time} seconds...')
                time.sleep(wait_time)
                tweets = await tweets.next()

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

        processBatchThread = threading.Thread(target=processBatch, args=(tweets, queryId))
        processBatchThread.start()

        print(f'{datetime.now()} - Got {len(tweets)} new tweets')

    print(f'{datetime.now()} - Done! Got {tweet_count} tweets total')

    global finished_obtaining_tweets
    finished_obtaining_tweets = True

def processBatch(tweets, queryId):
    connection = sqlite3.connect(filename)
    cursor = connection.cursor()

    fullTweetDataList = []

    for tweet in tweets:
        if "parody" in tweet.user.name.lower():
            continue

        if tweet.lang.lower() not in ["ar", "en", "fr", "de", "hi", "it", "sp", "pt"]:
            continue

        date_format_str = "%a %b %d %H:%M:%S %z %Y"

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

        cursor.execute("INSERT INTO TWEETS (queryId, tweetId, language, username, originalLocation, content, creationDate, retweets, likes) VALUES (?,?,?,?,?,?,?,?,?)", tuple(myTweetDict.values()))

        myTweetDict["id"] = cursor.lastrowid

        fullTweetDataList.append(myTweetDict)

        location_queue.append({
            "id": cursor.lastrowid,
            "location": tweet.user.location
        })

    connection.commit()

    tweetContent = [d['content'] for d in fullTweetDataList]

    sentiment_data = sentiment_pipeline(tweetContent)

    for i in range (len(tweetContent)):
        fullTweetDataList[i]["sentiment"] = sentiment_data[i]["label"]
        fullTweetDataList[i]["confidence"] = sentiment_data[i]["score"]

    for tweet in fullTweetDataList:
        cursor.execute("UPDATE TWEETS SET sentiment = ?, confidence = ? WHERE id = ?", (tweet["sentiment"], tweet["confidence"], tweet["id"]))

    connection.commit()

    connection.close()

def processLocation():
    connection = sqlite3.connect(filename)
    cursor = connection.cursor()

    geolocator = Nominatim(user_agent="CTS1000-Final-Project")

    while(not finished_obtaining_tweets or len(location_queue) > 0):
        if(len(location_queue) > 0):
            myItem = location_queue.pop(0)

            print(f'{datetime.now()} - Processing location for \"{myItem["location"]}\"')

            location = geolocator.geocode(myItem["location"])

            if(location != None):
                cursor.execute("UPDATE TWEETS SET locationLatitude = ?, locationLongitude = ? WHERE id = ?", (location.latitude, location.longitude, myItem["id"]))

                connection.commit()

            time.sleep(1.1)

    connection.close()
    
if __name__ == "__main__":
    try:
        if sys.argv[1] == "":
            print("Please enter a filename")
            sys.exit()
    except IndexError:
        print("Please enter a filename")
        sys.exit()

    filename = sys.argv[1]

    if not os.path.isfile("search.json"):
        print("search.json not found")
        sys.exit()

    with open('search.json') as f:
        searchData = json.load(f)

    if not os.path.isfile(filename):
        createDatabase()

    query = buildQuery(searchData)
    
    queryId = getQueryId(query, searchData)

    processLocationThread = threading.Thread(target=processLocation, args=())
    processLocationThread.start()

    asyncio.run(scrape(query, searchData["minTweets"], queryId))