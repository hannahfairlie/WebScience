import pymongo
from pymongo import MongoClient
import json
import re
import twitter
import tweepy
import numpy
from threading import Thread
from pprint import pprint
from datetime import datetime
from datetime import timedelta
import time
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics import adjusted_rand_score
import pandas as pd
import numpy as np
from scipy import stats
from operator import itemgetter
from itertools import combinations
import jgraph as ig
import json
import matplotlib.pyplot as plt
import seaborn as sns
import networkx as nx


c = MongoClient('localhost', 27017)
db = c.tweets


df = pd.DataFrame(list(db['Crawler Task1b Test'].find()))

#Creates a new dataframe with specific columns required for this specific task

interaction_df = pd.DataFrame(columns = ["created_at", "id", "reply_name", "reply_id", "reply_user_id",
                                      "retweeted_id", "retweeted_name", "user_mentions_name", "user_mentions_id", "hashtags"
                                       "text", "user_id", "name"])


#Gets the user id and name of the person who wrote the tweet
def get_user_info(dataframe):
    dataframe["name"] = df["user"].apply(lambda x: x["screen_name"])
    dataframe["user_id"] = df["user"].apply(lambda x: x["id"])
    return dataframe

#Where the name and id of the user that has been mentioned by a specific user is found in order to find interactions later.
def get_mentions(dataframe):
    dataframe["user_mentions_name"] = df["entities"].apply(lambda x: x["user_mentions"][0]["screen_name"] if x["user_mentions"] else np.nan)
    dataframe["user_mentions_id"] = df["entities"].apply(lambda x: x["user_mentions"][0]["id_str"] if x["user_mentions"] else np.nan)
    return dataframe

#Gets the hashtag entries that are in each user entries column and puts only the text in a list (removes the indices)
def get_hashtags(dataframe):
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
    dataframe['hashtags'] = all_hash




#Where the name and id of the user that has been retweeted by a specific user is found in order to find interactions later.
def get_retweets(dataframe):
    dataframe["retweeted_name"] = df["retweeted_status"].apply(lambda x: x["user"]["screen_name"] if x is not np.nan else np.nan)
    dataframe["retweeted_id"] = df["retweeted_status"].apply(lambda x: x["user"]["id_str"] if x is not np.nan else np.nan)
    return dataframe


#Gathers all the information required to fill the dataframe
def populate(dataframe):
    dataframe["created_at"] = df["created_at"]
    dataframe["id"] = df["id"]
    dataframe["text"] = df["text"]
    get_user_info(dataframe)
    get_mentions(dataframe)
    get_retweets(dataframe)
    get_hashtags(dataframe)
    dataframe['reply_name'] = df['in_reply_to_user_id']
    dataframe['reply_id'] = df['in_reply_to_status_id']
    dataframe['reply_user_id'] = df['in_reply_to_user_id']
    return dataframe


#Function created in order to determine any interactions made based of the user mentions, retweets and replies.

def get_interactions(row):
   
   #Gets a new row which includes a username and id. If it is none it returns none.
    user = row["user_id"], row["name"]
    if user[0] is None:
        return (None, None), []
    
    #Creates an empty set of tuples which will be updated with ids and usernames of any interactions.
    interactions = set()
    
    #Adds the interactions to the set for each different type of interaction.
    interactions.add((row["reply_user_id"], row["reply_name"]))
    interactions.add((row["retweeted_id"], row["retweeted_name"]))
    interactions.add((row["user_mentions_id"], row["user_mentions_name"]))
    
    #Removes any duplicate user id and usernames and removes any empty tuples.
    interactions.discard((row["user_id"], row["name"]))
    interactions.discard((None, None))
  
    #This function returns the username and id as a tuple and the interactions as a set of tuples.
    return user, interactions

#Adds all the information that has been identified into the dataframe
interaction_df = populate(interaction_df)
interaction_df = interaction_df.where((pd.notnull(interaction_df)), None)


#Initiates the graph
u_i_graph = nx.Graph()


#For each line of the dataframe it finds the userid and username and the tweet id for each ineraction entry.
#For each interaction in the dataframe interactions entry there is a new node added and connected to the original user.
#i.e an edge is created between the user and their interaction and then a node is created for each.
for i, t in interaction_df.iterrows():
    user, interact = get_interactions(t)
    u_id, u_name = user
    t_id = t["id"]
    for inter in interact:
        other_id, other_name = inter
        u_i_graph.add_edge(u_id, other_id, tweet_id=t_id)
        u_i_graph.node[u_id]["name"] = u_name
        u_i_graph.node[other_id]["name"] = other_name



#degrees = [val for (node, val) in graph.degree()]

#Works out important information about the users in the graph.
#First uses pre-defined functions to find the degree centrality, the closeness and betweenness.
#Find the max value for each - the one that appears the most.

biggest_sub = max(nx.connected_component_subgraphs(u_i_graph), key=len)

graph_degree_central = nx.degree_centrality(biggest_sub)

max_degree = max(graph_degree_central.items(), key=itemgetter(1))

graph_closeness = nx.closeness_centrality(biggest_sub)

max_closeness = max(graph_closeness.items(), key=itemgetter(1))

graph_betweenness = nx.betweenness_centrality(biggest_sub, normalized=True, endpoints=False)

max_betweenness = max(graph_betweenness.items(), key=itemgetter(1))


#Plots and draws the graph. Intially draws all the nodes and edges thnen add the nodes of importance in a different colour. Here it plots the degree centrality node in orange which is essentilly the mos important user in the graph.
#Saves and displays the graph so that it can be used in the report.
plt.figure(figsize = (20,20))
nx.draw(biggest_sub, pos=nx.spring_layout(biggest_sub, k=0.05), edge_color="black", linewidths=0.3, node_size=60, alpha=0.6, with_labels=False)
nx.draw_networkx_nodes(biggest_sub, pos=nx.spring_layout(biggest_sub, k=0.05), nodelist=[max_degree[0]], node_size=300, node_color='orange')
plt.savefig('graph.png')
plt.show()

#Question 4 - uses the graph to find out the connections that have been made

#Uses triad census which returns a dictionary where the keys are the types of triad and the value is the number of occurences.
triads = nx.triadic_census(u_i_graph.to_directed())
print(triads)

#Number of edges associates with how many times two nodes are linked and therefore can determine how many links are in the graph.
links = u_i_graph.number_of_edges()
print(links)
