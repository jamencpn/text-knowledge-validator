import pandas as pd
from config.config_loader import ConfigConnection
from config.config_loader.qc_load import check_db_config
from call_database import DB
import json
import pymongo
import os

### input = {db_knowledge, dunpped_json}

ROOT_USERNAME = ConfigConnection.MONGODB["ROOT_USERNAME"]
ROOT_PASSWORD = ConfigConnection.MONGODB['ROOT_PASSWORD']
DB_IP = ConfigConnection.MONGODB['DB_IP']
DB_PORT = ConfigConnection.MONGODB['DB_PORT']

mongo_connect = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' %
                                 (ROOT_USERNAME, ROOT_PASSWORD, DB_IP, DB_PORT))
mongodb = mongo_connect[ConfigConnection.MONGODB['DB_NAME']]

async def upload_to_db(db_knowledge, data):
    qc_config = json.load(open(os.path.join("config","qc_config.json")))
    collection = mongodb[db_knowledge]
    if collection.count() == 0:
        print('not exist')
        df = data 
        if check_db_config(db_knowledge) != False:
            key = check_db_config(db_knowledge)
            dup_list = list()
            rename_dict = dict()
            for field in qc_config[key]['field_qc']:
                if qc_config[key]['field_qc'][field]['update'] == 0:
                    dup_list.append(field)
                rename_dict[qc_config[key]['field_qc'][field]['name']] = field

            ### change name function
            df = df.rename(columns=rename_dict)
            ### subset update
            df = df.drop_duplicates(subset=dup_list,keep='last')
        else:
            df = df.drop_duplicates(keep='last')
    else:
        cursor = collection.find()
        df_old = pd.DataFrame(list(cursor))
        df_old = df_old.drop(columns=['_id'])
        ### merge old with new
        df_new = data
        if check_db_config(db_knowledge) != False:
            key = check_db_config(db_knowledge)
            dup_list = list()
            rename_dict = dict()
            for field in qc_config[key]['field_qc']:
                if qc_config[key]['field_qc'][field]['update'] == 0:
                    dup_list.append(field)
                rename_dict[qc_config[key]['field_qc'][field]['name']] = field

            ### change name function
            df_new = df_new.rename(columns=rename_dict)
            ### subset update
            ### need code merge field == field or field != field (column pos)
            if sorted(list(df_old.columns)) != sorted(list(df_new.columns)):
                df_new.columns = [df_old.columns[i] for i in range(len(df_new.columns))]
            df = pd.concat([df_old, df_new]).reset_index(drop=True)
            ### drop dup with old and new
            df = df.drop_duplicates(subset=dup_list,keep='last')
        else:
            df = df_new.drop_duplicates(keep='last')
    #### insert function ####
    collection.drop()
    result = df.to_json(orient='records')
    parsed = json.loads(result)
    collection.insert_many(parsed)
