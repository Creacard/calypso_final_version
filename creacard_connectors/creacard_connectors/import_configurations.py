import json
import os
import sys

def database_connection(database_type, database_name):

    if sys.platform == "win32":
        folder_json = os.path.expanduser('~') + "\\conf_python\\database_connection.json"
    else:
        folder_json = os.environ['HOME'] + "/conf_python/database_connection.json"
    with open(folder_json, 'r') as JSON:
        con = json.load(JSON)

    return con[database_type][database_name]


def FTP_connection(protocole_type, protocole_name):

    if sys.platform == "win32":
        folder_json = os.path.expanduser('~') + "\\conf_python\\FTP_connection.json"
    else:
        folder_json = os.environ['HOME'] + "/conf_python/FTP_connection.json"
    with open(folder_json, 'r') as JSON:
        con = json.load(JSON)

    return con[protocole_type][protocole_name]
