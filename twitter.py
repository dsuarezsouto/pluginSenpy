import tweepy
import json
import os
import argparse
import time

import pandas as pd
from pandas import DataFrame
from datetime import date,timedelta
def retrieve_tweets(query, filePath, count=3000):

    consumer_key = 'n9j2KmkV3FFWkzgGeq4XdPWCp'
    consumer_secret = 'TszufpKDVtR9rkEkfBS8SdljhqMuxO26ohnUqqDjfTNk2xvJrx'
    access_token = '367437965-UHlGKsxK2WfYNptu2tOPBgT7jxgFvSwJ5fuS59C3'
    access_token_secret = 'qYJQdSzBfdNMPllmA5jU5xJAuyTGxzEs6G7uKRoENRsSu'

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)

    max_tweets = count
    max_tweets = int(max_tweets)
    print(max_tweets)
    searched_tweets = []
    #Read the tweets of yesterday
    local_time_obj=date.today()
    local_time_obj=local_time_obj-timedelta(1)
    datetime=local_time_obj.strftime("%Y_%m_%d_%H_%M_%S")
    if os.path.exists('tweets/{}.json'.format(datetime)):
        file=open('tweets/{}.json'.format(datetime),"r")
        lines=file.readlines()
        file.close()

        since_id = json.loads(lines[-1])["id"]
    else:
        since_id=-1

    last_id=-1
    print("SINCE_ID:{} ".format(since_id))
    #last_id=-1
    #Number connections
    count_connections=0
    MAX_CONNECTIONS=15
    while len(searched_tweets) < max_tweets:
        count = max_tweets - len(searched_tweets)
        try:
            new_tweets = api.search(q=query, count=count,since_id=since_id, max_id=str(last_id - 1),geocode="40.416775,-3.703790,820km",include_rts=False)
            #wq = bitter.crawlers.TwitterQueue.from_credentials('credentials.json')
            #new_tweets = wq.statuses.search_tweet(q=query, count=count, include_rts=False)
            #wq = bitter.crawlers.TwitterQueue.from_credentials()
            #new_tweets = wq.statuses.user_timeline(screen_name=entry['nif:isString'], count=count, include_rts=False)
            if not new_tweets:
                break
            # Filter Rts
            for tweet in new_tweets:
                tweetString = json.dumps(tweet._json)
                tweetJson = json.loads(tweetString)
                print(tweetJson['text'])
                if not "retweeted_status" in tweetJson:
                    searched_tweets.append(tweet)
            #searched_tweets.extend(new_tweets)
            last_id = new_tweets[-1].id
            count_connections+=1
            if count_connections>=MAX_CONNECTIONS:
                count_connections=0
                time.sleep(60)
                print("We have to wait 1 minute because of the limit connection")
        except tweepy.TweepError as e:

            # depending on TweepError.code, one may want to retry or wait
            # to keep things simple, we will give up on an error

            break
    with open(filePath, 'a') as output:        
        for item in searched_tweets:
            jsontweet = json.dumps(item._json)
            tweet = json.loads(jsontweet)
            #print(tweet)
            json.dump(tweet, output)
            output.write('\n')

def retrieve_timeline(query, filePath, count=200):

   
    consumer_key = 'n9j2KmkV3FFWkzgGeq4XdPWCp'
    consumer_secret = 'TszufpKDVtR9rkEkfBS8SdljhqMuxO26ohnUqqDjfTNk2xvJrx'
    access_token = '367437965-UHlGKsxK2WfYNptu2tOPBgT7jxgFvSwJ5fuS59C3'
    access_token_secret = 'qYJQdSzBfdNMPllmA5jU5xJAuyTGxzEs6G7uKRoENRsSu'

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)

    #searched_tweets = api.user_timeline(user_id=query, count=count)
    searched_tweets = api.user_timeline(user_id=query,exclude_replies=False,include_rts=True,count=200)
   
    with open(filePath, 'a') as output:        
        for item in searched_tweets:
            jsontweet = json.dumps(item._json)
            tweet = json.loads(jsontweet)
            #print(tweet)
            json.dump(tweet, output)
            output.write('\n')

    # tweets_insomnio.to_json(filePath,orient='records')

