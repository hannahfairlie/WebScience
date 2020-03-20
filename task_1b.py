import pymongo
from pymongo import MongoClient
import twitter
import json
import tweepy
from threading import Thread
from tweepy import StreamListener
from datetime import datetime
from datetime import timedelta
import time

consumer_key = 'lZdLxJnFdXCMik1U3zRZoPVWm'
consumer_secret_key = 'evPiFdmVtQtdRsF4CBdOzwPQw39mpQDIvyCSThaFJjfw9YwFll'
token = '1225421260441767939-zC8AudpMxCZi4BxYILP1tm8vFX4OQk'
secret_token = 'vCa4N2pcuIBKsnEMWCsNSRBHjVpyJwM3o1I7TZ23UqyJm'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret_key)
auth.set_access_token(token, secret_token)

c = MongoClient('localhost', 27017)
db = c.tweets

api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)


RUN_TIME = 60
uk_woeid = 23424975
times = False
collection = "Crawler Task1b Test"


#Converts time so that mongodb can deal with it more easily
def store(status):
    t = status._json
    dt = t["created_at"]
    t["created_at"] = datetime.strptime(dt, '%a %b %d %H:%M:%S +0000 %Y')
    return t

#Setting up the Stream API to add tweets to the database
class Listener(tweepy.StreamListener):
    def on_status(self, status):
        
        #Uses predefined error to check if there are duplicates if not it is added to the database
        
        duplicates = 0

        try:
            db[collection].insert_one(store(status))
        except pymongo.errors.DuplicateKeyError: 
            duplicates += 1
        return True

    def on_error(self, status_code):
        if status_code == 420:
            return False
        print(status_code)
     

#Sorts the trends and removes None   
def sort(no_of_trends):

    if no_of_trends is None:
        return 0
    return no_of_trends


#sorts the trends by only finding the ones in the uk with the largest number of tweets
def uk_trends():

    trend = api.trends_place(uk_woeid)
    d = trend[0]
    trends = d['trends']
    trend_volume = sorted(trends, key=lambda listT: sort(listT['tweet_volume']))

    for each_trend in trend_volume:
        name = each_trend['name']
        for entry in tweepy.Cursor(api.search, q= name, count=150, lang='en').items():
            db[collection].insert(store(entry))
            if time:
                break
        if time:
            break


#Calls the stream API
listener = Listener(api = api)
t_stream = tweepy.Stream(auth = auth, listener = listener)

#Defines a time for the crawler to run for
start = datetime.now()
end = start + timedelta(minutes=RUN_TIME)

#Creates a thread to run the REST API
r_thread = Thread(target=uk_trends)
r_thread.start()

t_stream.sample(languages=['en'], is_async = True)


#Disconnects crawler after time ends
while start < end:
    time.sleep(30)

times = True
r_thread.join()
t_stream.disconnect()


