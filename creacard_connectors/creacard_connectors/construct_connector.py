""" construct a connector """

# Author: Justin Valet
# Date : 31/08/2019
# e-mail: jv.datamail@gmail.com



import json
import os
import sys

def construct_connection_to_database(database_type):

    if sys.platform == "win32":
        folder_json = os.path.expanduser('~') + "\\conf_python\\sqlalchemy_construction.json"
    else:
        folder_json = os.environ['HOME'] + "/conf_python/sqlalchemy_construction.json"
    with open(folder_json, 'r') as JSON:
        con = json.load(JSON)

    return con[database_type]["alchemy_constructor"]
