import json
import os
import pymongo
import pandas as pd
import numpy as np
from bson.json_util import dumps
from call_database import match_keyword, levenshtein
from config.config_loader import ConfigConnection
from business_logic import input_logic, validate_logic
import re
from pytictoc import TicToc
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

ROOT_USERNAME = ConfigConnection.MONGODB["ROOT_USERNAME"]
ROOT_PASSWORD = ConfigConnection.MONGODB['ROOT_PASSWORD']
DB_IP = ConfigConnection.MONGODB['DB_IP']
DB_PORT = ConfigConnection.MONGODB['DB_PORT']

mongo_connect = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' %
                                 (ROOT_USERNAME, ROOT_PASSWORD, DB_IP, DB_PORT))
mongodb = mongo_connect[ConfigConnection.MONGODB['DB_NAME']]

def check_project_code(project_code):
    qc_config = json.load(open(os.path.join("config","qc_config.json")))
    for project in qc_config.keys():
        if project_code == project:
            return True
    return False

def ngrams(string, n=3):
    """Takes an input string, cleans it and converts to ngrams. 
    This script is focussed on cleaning UK company names but can be made generic by removing lines below"""
    string = str(string)
    string = string.lower() # lower case
    string = ' '+ string +' ' # pad names for ngrams...
    ngrams = zip(*[string[i:] for i in range(n)])
    return [''.join(ngram) for ngram in ngrams]

def get_tf_idf_query_similarity(vectorizer, docs_tfidf, query):
    """
    vectorizer: TfIdfVectorizer model
    docs_tfidf: tfidf vectors for all docs
    query: query doc

    return: cosine similarity between query and all docs
    """
    query_tfidf = vectorizer.transform([query])
    cosineSimilarities = cosine_similarity(query_tfidf, docs_tfidf).flatten()
    return cosineSimilarities

def qc_validate(json_input, project_code):
    qc_config = json.load(open(os.path.join("config","qc_config.json")))
    tt = TicToc()
    tt.tic()
    if type(json_input) == list:
        merge = {'json_output':[],'suggestion':[],'result':[],'confidence':[]}
        for i in json_input:
            temp_data = qc_validate(i,project_code)
            merge['json_output'].append(temp_data['json_output'])
            merge['suggestion'].append(temp_data['suggestion'])
            merge['result'].append(temp_data['result'])
            merge['confidence'].append(temp_data['confidence'])
        return merge

    col = mongodb[qc_config[project_code]['db_knowledge']]
    text_dict = json_input
    text_dict = input_logic(text_dict, project_code)

    distance_dict = {'_id': 1}
    sow = 0 #sum of weights
    project = qc_config[project_code]
    drop_list = []
    for field in project['field_qc']:
        distance_dict[field] = 1
        if text_dict[field] != '':
            sow += project['field_qc'][field]['weight']
        else:
            drop_list.append(field)
    cursor = col.find({}, distance_dict)
    df = pd.DataFrame(list(cursor)).set_index('_id')
    df = df.fillna(0)
    df_check = df.copy()
    df = df.drop(columns=drop_list)
    if project['mode'] == 0:
        for key in project['field_qc']:
            if text_dict[key] != '':
                try:
                    df[key] = df[key].apply(lambda x: 1  if str(float(re.sub(',','',text_dict[key]))) == str(float(re.sub(',','',x))) else 0) * project['field_qc'][key]['weight']
                except:
                    df[key] = df[key].apply(lambda x: 1  if str(text_dict[key]) == str(x) else 0) * project['field_qc'][key]['weight']
    elif project['mode'] == 1:
        for key in project['field_qc']:
            if text_dict[key] != '':
                try:
                    #df[key] = df[key].apply(lambda x: 1 - match_keyword(str(float(re.sub(',','',text_dict[key]))),str(float(re.sub(',','',x))),1)['score'] if match_keyword(str(float(re.sub(',','',text_dict[key]))),str(float(re.sub(',','',x))),1)['pos'] != -1 else 0) * project['field_qc'][key]['weight']
                    df[key] = df[key].apply(lambda x: 1 - levenshtein(str(float(re.sub(',','',text_dict[key]))),str(float(re.sub(',','',x))))/max(len(str(float(re.sub(',','',text_dict[key])))),len(str(float(re.sub(',','',x)))))) * project['field_qc'][key]['weight']
                except:
                    #df[key] = df[key].apply(lambda x: 1 - match_keyword(str(text_dict[key]),str(x),1)['score'] if match_keyword(str(text_dict[key]),str(x),1)['pos'] != -1 else 0) * project['field_qc'][key]['weight']
                    df[key] = df[key].apply(lambda x: 1 - levenshtein(str(text_dict[key]),validate_logic(str(x), project_code))/max(len(str(text_dict[key])),len(validate_logic(str(x), project_code)))) * project['field_qc'][key]['weight']
    else:
        for key in project['field_qc']:
            if text_dict[key] != '':
                item_list = list(df[key]) #unique org names from company watch file
                #Building the TFIDF off the clean dataset - takes about 5 min
                vectorizer = TfidfVectorizer(min_df=1, analyzer=ngrams)
                vecfit = vectorizer.fit_transform(item_list)
                df[key] = get_tf_idf_query_similarity(vectorizer, vecfit, text_dict[key])


    df = df.fillna(0)
    sorted_df = df.T.sum().sort_values(ascending=False)
    json_output = dict()
    suggestion = dict()
    if sorted_df[0] / sow >= project['threshold_ratio']:
        result = True
    else:
        result = False
    # find most score index
    data = dict()
    ### add condition > 0 
    for field in dict(df_check.loc[sorted_df.index[0],:]):
        try:
            if str(float(re.sub(',','',dict(df_check.loc[sorted_df.index[0],:])[field]))) == str(float(re.sub(',','',text_dict[field]))):
                json_output[field] = True
                suggestion[field] = [df_check.loc[sorted_df.index[0],field],df_check.loc[sorted_df.index[1],field],df_check.loc[sorted_df.index[2],field]]
            else:
                json_output[field] = False
                #suggestion[field] = df_check.loc[sorted_df.index[0],field] if sorted_df[0] / sow != sorted_df[1] / sow else None
                suggestion[field] = [df_check.loc[sorted_df.index[0],field],df_check.loc[sorted_df.index[1],field],df_check.loc[sorted_df.index[2],field]]
        except:
            if str(dict(df_check.loc[sorted_df.index[0],:])[field]) == str(text_dict[field]):
                json_output[field] = True
                suggestion[field] = [df_check.loc[sorted_df.index[0],field],df_check.loc[sorted_df.index[1],field],df_check.loc[sorted_df.index[2],field]]
            else:
                json_output[field] = False
                #suggestion[field] = df_check.loc[sorted_df.index[0],field] if sorted_df[0] / sow != sorted_df[1] / sow else None
                suggestion[field] = [df_check.loc[sorted_df.index[0],field],df_check.loc[sorted_df.index[1],field],df_check.loc[sorted_df.index[2],field]]
    data['json_output'] = json_output
    data['suggestion'] = suggestion
    data['result'] = result
    data['confidence'] = [sorted_df[0] / sow,sorted_df[1] / sow,sorted_df[2] / sow]
    print(df.T.sum().sort_values(ascending=False)/sow)
    tt.toc()
    return data