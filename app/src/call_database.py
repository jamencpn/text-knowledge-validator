import json
import os
import pymongo
import pandas as pd
import numpy as np
from bson.json_util import dumps
# from hippo_tools.text_processing.match import match_keyword
from config.config_loader import ConfigConnection

ROOT_USERNAME = ConfigConnection.MONGODB["ROOT_USERNAME"]
ROOT_PASSWORD = ConfigConnection.MONGODB['ROOT_PASSWORD']
DB_IP = ConfigConnection.MONGODB['DB_IP']
DB_PORT = ConfigConnection.MONGODB['DB_PORT']

class DB:
    def __init__(self, database):
        self.ROOT_USERNAME = ROOT_USERNAME
        self.ROOT_PASSWORD = ROOT_PASSWORD
        self.DB_IP = DB_IP
        self.DB_PORT = DB_PORT
        self.mongo_client = self.client()
        self.database = database
        #self.DISTANCE_FIELD = DISTANCE_FIELD
        #self.TEXTSCORE_FIELD = TEXTSCORE_FIELD
        #self.THRESHOLD_RATIO = THRESHOLD_RATIO

    def find_all(self, collection):
        col = self.mongo_client[self.database][collection]
        cursor = col.find()
        #df = pd.DataFrame(list(cursor))
        result = dumps(list(cursor), ensure_ascii=False)
        return result

    def query(self, collection, cond):
        col = self.mongo_client[self.database][collection]
        cursor = col.find(cond)  # find by condition (dictionary type)
        #df = pd.DataFrame(list(cursor))
        result = dumps(list(cursor), ensure_ascii=False)
        return result

    def text_score(self, collection, field, text):
        col = self.mongo_client[self.database][collection]
        col.create_index([(field, pymongo.TEXT)])
        cursor = col.find({'$text': {'$search': text}}, {
                          'score': {'$meta': 'textScore'}})
        #df = pd.DataFrame(list(cursor))
        result = dumps(list(cursor), ensure_ascii=False)
        return result

    def query_where(self, collection, cond):
        col = self.mongo_client[self.database][collection]
        # find by where condition which cond is javascript functio
        cursor = col.find().where(cond)
        #df = pd.DataFrame(list(cursor))
        result = dumps(list(cursor), ensure_ascii=False)
        return result

    def insert(self, collection, list_dict):
        col = self.mongo_client[self.database][collection]
        col.insert_many(list_dict)  # insert list of dictionary per document

    def insert_one(self, collection, list_dict):
        col = self.mongo_client[self.database][collection]
        # insert list of dictionary per document
        df = col.insert_one(list_dict)
        dbid = (df.inserted_id)
        return dbid

    def delete(self, collection, cond):
        col = self.mongo_client[self.database][collection]
        col.delete_many(cond)

    def update(self, collection, cond, newvalue):
        col = self.mongo_client[self.database][collection]
        # Ex. {"name" : {"$regex":"^J"}},{"signature":"j","_id":3}
        col.update_many(cond, {'$set': newvalue})

    def client(self):
        """
        create mongoDB client object
        """
        mc = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' %
                                 (self.ROOT_USERNAME, self.ROOT_PASSWORD, self.DB_IP, self.DB_PORT))

        return mc

    def validation(self, collection, text_dict):
        """
            instructions
        collection -> database collection name Ex. 'collection'
        text_dict -> dictionary of OCR text Ex. {'name':'Jame', 'tel.':'xxxxxxxxxx'}
        DISTANCE_FIELD -> dictionary of document field and weight searching with levenshtein distance Ex. {'name': 5, 'tel.': 2} -> no upper bound
        TEXTSCORE_FIELD -> dictionary of document field and weight searching with mongodb textscore Ex. {'address': 3}
        return document matched with maximum score which more than self.THRESHOLD_RATIO -> range 0 - 1
        """
        col = self.mongo_client[self.database][collection]
        distance_dict = {'_id': 1}
        sow = 0 #sum of weights
        if self.DISTANCE_FIELD != None:
            for field in self.DISTANCE_FIELD:
                sow += self.DISTANCE_FIELD[field]
                distance_dict[field] = 1
            cursor = col.find({}, distance_dict)
            df = pd.DataFrame(list(cursor)).set_index('_id')
            df = df.fillna(0)

            for key in self.DISTANCE_FIELD:
                df[key] = df[key].apply(lambda x: 1 - match_keyword(str(text_dict[key]),str(x),1)['score'] if match_keyword(str(text_dict[key]),str(x),1)['pos'] != -1 else 0) * self.DISTANCE_FIELD[key]
        else:
            cursor = col.find({}, distance_dict)
            df = pd.DataFrame(list(cursor)).set_index('_id')

        if self.TEXTSCORE_FIELD != None:

            # col.create_index([(field, pymongo.TEXT)
            #                  for field in self.TEXTSCORE_FIELD])
            # text_dict_value = list()
            # for key in self.TEXTSCORE_FIELD:
            #     text_dict_value.append(text_dict[key])
            # textscore_text = ' '.join(text_dict_value)

            for field in self.TEXTSCORE_FIELD:
                sow += self.TEXTSCORE_FIELD[field]
                col.drop_indexes()
                col.create_index([(field, pymongo.TEXT)])
                textscore_text = text_dict[field]

                cursor = col.find(
                    {'$text': {'$search': textscore_text
                               }},
                    {'score': {'$meta': 'textScore'}})

                df_score = pd.DataFrame(list(cursor))
                if not df_score.empty:
                    df_score = df_score.set_index('_id')
                    df[field] = df_score['score']/df_score['score'].max() * self.TEXTSCORE_FIELD[field]
                else:
                    df[field] = 0

        df = df.fillna(0)
        if df.T.sum().sort_values(ascending=False)[0] / sow > self.THRESHOLD_RATIO:
            cursor = col.find(
                {'_id': df.T.sum().sort_values(ascending=False).index[0]})
            result = dumps(list(cursor), ensure_ascii=False)
        else:
            result = text_dict
        print(df.T.sum().sort_values(ascending=False)/sow)
        return result


def levenshtein(source, target):
    if len(source) < len(target):
        return levenshtein(target, source)

    # So now we have len(source) >= len(target).
    if len(target) == 0:
        return len(source)

    # We call tuple() to force strings to be used as sequences
    # ('c', 'a', 't', 's') - numpy uses them as values by default.
    source = np.array(tuple(source))
    target = np.array(tuple(target))

    # We use a dynamic programming algorithm, but with the
    # added optimization that we only need the last two rows
    # of the matrix.
    previous_row = np.arange(target.size + 1)
    for s in source:
        # Insertion (target grows longer than source):
        current_row = previous_row + 1

        # Substitution or matching:
        # Target and source items are aligned, and either
        # are different (cost of 1), or are the same (cost of 0).
        current_row[1:] = np.minimum(
            current_row[1:],
            np.add(previous_row[:-1], target != s))

        # Deletion (target grows shorter than source):
        current_row[1:] = np.minimum(
            current_row[1:],
            current_row[0:-1] + 1)

        previous_row = current_row

    return previous_row[-1]


def match_keyword(input_word, keyword, threshold):
    """
    compute edit distance between input_word and keyword.
    Parameters:
        input_word (str):
        keyword (str):
        threshold (float): range(0.0, 1.0)
    Returns:
        match (dict): {'word': (str), 'pos': index(int) , 'score': (float)}
    """

    # edit distance < threshold(%) of shortest word
    len_keyword = len(keyword)
    input_word_up = input_word.upper()
    keyword_up = keyword.upper()
    pos = input_word_up.find(keyword_up)  # pos
    match_output = {
        "word": "",
        "pos": -1,
        "score": 0
    }

    if pos >= 0:
        match_output["word"] = keyword
        match_output["pos"] = pos
        return match_output

    min_distance_score = [10000, "", 0.0]

    input_word_up_ex = input_word_up.split()
    input_word_ex = input_word.split()

    # print("in match ", key_split, len(key_split), input_word_up_ex, len(input_word_up_ex))
    for input_word_index in range(len(input_word_up_ex)):
        for key_merge_chunk in range(0, len(input_word_up_ex), 1):
            # print("index", input_word_index, key_merge_chunk)
            input_word_cut = input_word_up_ex[input_word_index:
                                              input_word_index + key_merge_chunk + 1]
            input_word_up_n = " ".join(input_word_cut)
            len_word = len(input_word_up_n)
            # print("word run ", input_word_cut, input_word_up_n, len_word)
            if abs(len_word - len_keyword) < 5:
                distance = levenshtein(input_word_up_n, keyword_up)
                # print("dist ", distance, "++key++", keyword_up, "..in word..", input_word_up_n)
                if len_word > len_keyword:
                    distance_score = distance / len_keyword
                else:
                    distance_score = distance / len_word
                if distance_score < min_distance_score[0]:
                    min_distance_score[0] = distance_score
                    input_word_cut = input_word_ex[input_word_index:
                                                   input_word_index + key_merge_chunk + 1]
                    input_word_n = " ".join(input_word_cut)
                    min_distance_score[1] = input_word_n

            if min_distance_score[0] < threshold:
                match_output["word"] = min_distance_score[1]
                match_output["pos"] = input_word_up.find(
                    min_distance_score[1].upper())
                match_output["score"] = min_distance_score[0]

    # print("outmatch ", match_output)
    return match_output