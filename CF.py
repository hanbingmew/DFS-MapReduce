#!/usr/bin/env python
from pandas import DataFrame, read_csv
import pandas as pd 
import math
import sys


def getStructure(df):
    userDict={}
    movieDict={}
    for i in range(len(df)):
        if df.loc[i,'userId'] in userDict:
            userDict[df.loc[i,'userId']].append([df.loc[i,'movieId'],df.loc[i,'rating']])
        else:
            userDict[df.loc[i,'userId']]=[[df.loc[i,'movieId'],df.loc[i,'rating']]]
        if df.loc[i,'movieId'] in movieDict:
            movieDict[df.loc[i,'movieId']].append([df.loc[i,'userId'],df.loc[i,'rating']])
        else:
            movieDict[df.loc[i,'movieId']]=[[df.loc[i,'userId'],df.loc[i,'rating']]]
    return userDict,movieDict

def removeMean(userDict,movieDict):
    rate_avg={}
    for i in movieDict:
        rate_avg[i]=sum([j[1] for j in movieDict[i]])/len(movieDict[i])
    for i in userDict:
        for j in userDict[i]:
            j[1]=j[1]-rate_avg[j[0]]
    return userDict

def getDistance(remove_mean,userId1,userId2):     
    inner_xy=0
    len_x=0
    len_y=0
    for i in remove_mean[userId1]:
        for j in remove_mean[userId2]:
            if i[0]==j[0]:
                inner_xy+=i[1]*j[1]
                len_x+=i[1]**2
                len_y+=j[1]**2
    dist=inner_xy/(math.sqrt(len_x)*math.sqrt(len_y))
    return dist

def getNeighbors(movieDict,userId,remove_mean):
    pre_neighbor={userId:0}
    movie_list=[i[0] for i in remove_mean[userId]]
    for i in movie_list:
        for j in movieDict[i]:
            if j[0] not in pre_neighbor:
                dist=getDistance(remove_mean,userId,j[0])
                pre_neighbor[j[0]]=dist
    del pre_neighbor[userId]
    return sorted(pre_neighbor.items(),key = lambda x:x[1],reverse = True)[:20]

def getRecommand(userId,neighbors,userDict,movieDict):
    rec_dict={}
    movie_seen=[i[0] for i in userDict[userId]]
    for i in neighbors:
        for j in userDict[i[0]]:
            if j[0] not in rec_dict :
                rec_dict[j[0]]=j[1]*i[1]
            else:
                rec_dict[j[0]]=rec_dict[j[0]]+j[1]*i[1]
    recommend_list=sorted(rec_dict.items(),key = lambda x:x[1],reverse = True)
    for i in recommend_list:
        if i[0] in movie_seen:
            recommend_list.remove(i)
    if len(recommend_list) >10:
        return recommend_list[:10]
    else:
        return recommend_list

def getRecMovies(recommend_list):
    movie_list=[]
    df_movies=read_csv('small_movies.csv')
    for i in recommend_list:
        print(df_movies[df_movies['movieId']==i[0]]['title'].values)

if __name__=='__main__':
    userId=int(sys.argv[1])
    df = pd.read_csv('small_ratings.csv')
    userDict,movieDict=getStructure(df);
    remove_mean=removeMean(userDict,movieDict)
    neighbors=getNeighbors(movieDict,userId,remove_mean)
    rec_list=getRecommand(userId,neighbors,userDict,movieDict)
    getRecMovies(rec_list)
