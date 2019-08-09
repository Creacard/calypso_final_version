import sys
import os
import json
import glob

class credentials_extractor(object):


    def __init__(self):

        """
               Parameters
               ----------
               user : str
                   username of your GitHub account
               pwd : str
                   password of your GitHub account
               """

        self._connection_types = extract_connection_type()
        self._db_type = show_available_database_type()
        self._API_type = None
        self._FTP_Type = show_available_FTP_type()


    def get_database_credentials(self, database_type, database_name):

        if sys.platform == "win32":
            folder_json = os.path.expanduser('~') + "\\conf_python\\credentials_database.json"
        else:
            folder_json = os.environ['HOME'] + "/conf_python/credentials_database.json"
        with open(folder_json, 'r') as JSON:
            con = json.load(JSON)

        return con[database_type][database_name]

    def get_ftp_credentials(self, protocole_type, connection_name):

        if sys.platform == "win32":
            folder_json = os.path.expanduser('~') + "\\conf_python\\credentials_FTP.json"
        else:
            folder_json = os.environ['HOME'] + "/conf_python/credentials_FTP.json"

        with open(folder_json, 'r') as JSON:
            con = json.load(JSON)

        return con[protocole_type][connection_name]

    def get_sftp_credentials(self, protocole_type, connection_name):

        if sys.platform == "win32":
            folder_json = os.path.expanduser('~') + "\\conf_python\\credentials_SFTP.json"
        else:
            folder_json = os.environ['HOME'] + "/conf_python/credentials_SFTP.json"

        with open(folder_json, 'r') as JSON:
            con = json.load(JSON)

        return con[protocole_type][connection_name]


def extract_connection_type():
    if sys.platform == "win32":
        Folder = os.path.expanduser('~') + "\\conf_python\\"
    else:
        Folder = os.environ['HOME'] + "/conf_python/"
    os.chdir(Folder)
    ListOfFile = glob.glob('credentials_**.json')

    connection_type = list()
    for json_file in ListOfFile:
        json_file = json_file.split("_")[1].replace(".json", "")
        connection_type.append(json_file)

    return connection_type


def show_available_database_type():

    if sys.platform == "win32":
        folder_json = os.path.expanduser('~') + "\\conf_python\\credentials_database.json"
    else:
        folder_json = os.environ['HOME'] + "/conf_python/credentials_database.json"
    with open(folder_json, 'r') as JSON:
        con = json.load(JSON)

    db_type = dict()
    for _database_type in con.keys():
        _tmp_list = list()
        for _database_name in con[str(_database_type)]:
            _tmp_list.append(str(_database_name))

        db_type[str(_database_type)] = _tmp_list

    return db_type


def show_available_FTP_type():

    if sys.platform == "win32":
        folder_json = os.path.expanduser('~') + "\\conf_python\\credentials_FTP.json"
    else:
        folder_json = os.environ['HOME'] + "/conf_python/credentials_FTP.json"
    with open(folder_json, 'r') as JSON:
        con = json.load(JSON)

        FTP_type = dict()
    for _protocole_type in con.keys():
        _tmp_list = list()
        for _protocole_name in con[str(_protocole_type)]:
            _tmp_list.append(str(_protocole_name))

        FTP_type[str(_protocole_type)] = _tmp_list

    return FTP_type


def show_available_API_type():

    if sys.platform == "win32":
        folder_json = os.path.expanduser('~') + "\\conf_python\\credentials_API.json"
    else:
        folder_json = os.environ['HOME'] + "/conf_python/credentials_API.json"
    with open(folder_json, 'r') as JSON:
        con = json.load(JSON)

        API_type = dict()
    for _protocole_type in con.keys():
        _tmp_list = list()
        for _protocole_name in con[str(_protocole_type)]:
            _tmp_list.append(str(_protocole_name))

        API_type[str(_protocole_type)] = _tmp_list

    return API_type
