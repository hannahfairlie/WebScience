import pymongo
from pymongo import MongoClient
import twitter
import json
import tweepy
from tweepy import StreamListener
from datetime import datetime
from datetime import timedelta
import time
import numpy
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics import adjusted_rand_score
import pandas as pd
import numpy as np
from scipy import stats
from operator import itemgetter
import re

c = MongoClient('localhost', 27017)
db = c.tweets


df = pd.DataFrame(list(db['Crawler Task1b Test'].find()))


#Makes sure the tweets can be processed and that there are no special characters left in. Also removes the intial RT that is put if any tweet has been retweeted as it makes the clustering be off.
def strip(tweet): 
        tweet.replace('RT', '')
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split()) 


#Gets all the hashtags for one user and puts them in a list and removes the indices that comes along with them.
all_hash = []
    
for entity in df['entities']:
    for key, val in entity.items():
        if (key == "hashtags"):
            hash_text = []
            for hasht in val:
                content = hasht.get('text')
                hash_text.append(content)
            all_hash.append(hash_text)
                
        else:
            continue

#Assigns the hashtags that have been collected to the hashtag column which has been added to the dataframe.
df['hashtags'] = all_hash

#Retrieves the name of the people that have been mentioned and adds them to a specific column in the dataframe.
df["user_mentions_screen_name"] = df["entities"].apply(lambda x: x["user_mentions"][0]["screen_name"] if x["user_mentions"] else np.nan)

 

#Assigns each column to a variable so they can be used more easily in iterations.
tweet_text = df['text']
hashtags = df['hashtags']
names = df['user_mentions_screen_name']

#Removes all the unwanted characters from the text
for text in tweet_text:
    strip(text)


#Uses predefined packages in order to cluster the terms 
frequency = TfidfVectorizer(stop_words='english')
train_model = frequency.fit_transform(tweet_text)
tf_idf_array = train_model.toarray()

#Determines the number of clusters required for the model
#Based on trying different values and seeing which one was most appropriate
k_model = KMeans(n_clusters=5, init='k-means++', max_iter=100, n_init=1)
k_model.fit(train_model)

#Returns a prediction for each term and the cluster they belong in.
predict = k_model.predict(train_model)

#Counts the number of terms in each cluster
cluster0 = list(predict.flatten()).count(0)
cluster1 =  list(predict.flatten()).count(1)
cluster2 =  list(predict.flatten()).count(2)
cluster3 =  list(predict.flatten()).count(3)
cluster4 =  list(predict.flatten()).count(4)


clusters = k_model.labels_.tolist()

df['cluster'] = clusters


labels = np.unique(clusters)



#Gets the top values for all clusters for hashtags and user mentions by counting them

i = 0

while i < 5:
    check_clusters = df[df.cluster.isin([i])]
    print(check_clusters['hashtags'].value_counts())
    print(check_clusters['user_mentions_screen_name'].value_counts())
    i += 1

#Gets the most central entry as a cluster center
central = k_model.cluster_centers_.argsort()[:, ::-1]



#prints the cluster and the terms that are included

features = frequency.get_feature_names()
for i in range(5): 
    print("Cluster %d:" % i),
    for ind in central[i, :5]:
        print(' %s' % features[ind]),
       

#Returns the top scoring features within the cluster based on their means

def top_clusters(tf_idf_array, predict, n_feats):
    labels = np.unique(predict)
    data_frame = []
    for label in labels:
        ind_cluster = np.where(predict==label) 
        x_means = np.mean(tf_idf_array[ind_cluster], axis = 0) 
        sorted_means = np.argsort(x_means)[::-1][:n_feats]
        features = frequency.get_feature_names()
        best_features = [(features[i], x_means[i]) for i in sorted_means]
        new_df = pd.DataFrame(best_features, columns = ['features', 'score'])
        data_frame.append(new_df)
    return data_frame

data_frame = top_clusters(tf_idf_array, predict, 20)
