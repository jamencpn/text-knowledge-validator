import json
import os

qc_config = json.load(open(os.path.join("config","qc_config.json")))
#db_knowledge = "GP"
test2 = qc_config.keys()

def check_db_config(db_knowledge):
    for project in qc_config.keys():
        if qc_config[project]['db_knowledge'] == db_knowledge:
            return project
    return False

#print(check_db_config("GP"))

#json_dump = json.dumps(test2, ensure_ascii=False)
#print(json_dump)
