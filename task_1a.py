import pymongo
from pymongo import MongoClient
import twitter
import json
import tweepy
from tweepy import StreamListener
from datetime import datetime
from datetime import timedelta
import time

consumer_key = 'lZdLxJnFdXCMik1U3zRZoPVWm'
consumer_secret_key = 'evPiFdmVtQtdRsF4CBdOzwPQw39mpQDIvyCSThaFJjfw9YwFll'
token = '1225421260441767939-zC8AudpMxCZi4BxYILP1tm8vFX4OQk'
secret_token = 'vCa4N2pcuIBKsnEMWCsNSRBHjVpyJwM3o1I7TZ23UqyJm'

RUN_TIME = 60


auth = tweepy.OAuthHandler(consumer_key, consumer_secret_key)
auth.set_access_token(token, secret_token)

c = MongoClient('localhost', 27017)
db = c.tweets


#Converts time so that mongodb can deal with it more easily
def store(status):
    t = status._json
    dt = t["created_at"]
    t["created_at"] = datetime.strptime(dt, '%a %b %d %H:%M:%S +0000 %Y')
    return t


#Setting up the Stream API to add tweets to the database
class Listener(tweepy.StreamListener):
    def on_status(self, status):

        duplicates = 0
 #Uses predefined error to check if there are duplicates if not it is added to the database
        try:
            db['Crawler Task1a'].insert_one(store(status))
        except pymongo.errors.DuplicateKeyError: 
            duplicates += 1
        return True

    def on_error(self, status_code):
        if status_code == 420:
            return False
        print(status_code)

#Allows to run for an hour

start = datetime.now()
end = start + timedelta(minutes=RUN_TIME)

#Calls stream APIto collect 1% data where the language is english

t_stream = tweepy.Stream(auth, Listener())

t_stream.sample(languages=["en"], is_async=True)

while start < end:
    time.sleep(30)

t_stream.disconnect()
