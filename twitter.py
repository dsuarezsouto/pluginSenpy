import tweepy
import json
import os
import argparse
import time

import pandas as pd
from pandas import DataFrame
def retrieve_tweets(query, filePath, count=200):

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
    last_id = -1
    while len(searched_tweets) < max_tweets:
        count = max_tweets - len(searched_tweets)
        try:
            new_tweets = api.search(q=query, count=count, max_id=str(last_id - 1))
            #wq = bitter.crawlers.TwitterQueue.from_credentials()
            #new_tweets = wq.statuses.user_timeline(screen_name=entry['nif:isString'], count=count, include_rts=False)
            if not new_tweets:
                break
            searched_tweets.extend(new_tweets)
            last_id = new_tweets[-1].id
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

