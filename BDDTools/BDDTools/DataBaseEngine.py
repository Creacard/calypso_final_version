""" Connection to database """

# Author: Justin Valet <jv.datamail@gmail.com>
# Date: 15/1/2019



from sqlalchemy import create_engine
import json
import os
import sys

def postres_creacard_config():

    if sys.platform == "win32":
        folder_json = os.path.expanduser('~') + "\\conf_python\\database_connection.json"
    else:
        folder_json = os.environ['HOME'] + "/conf_python/database_connection.json"
    with open(folder_json, 'r') as JSON:
        con = json.load(JSON)

    return con


class ConnectToPostgres(object):

    """Create an sqlalchemy connection to the database

        Parameters
        -----------
        credentials : dictionnary
            User credentials for the connection of the database

        Returns
        -----------
        engine : sqlachemy object
            A sqlalchemy object from create_engine (see docs)

    """

    _user           = None
    _pwd            = None
    _hostname       = None
    _database       = None
    _port           = None

    def __init__(self, credentials, **kwargs):
        self._user     = credentials["user"]
        self._pwd      = credentials["pwd"]
        self._use_conf = kwargs.get('use_conf', None)

    def CreateEngine(self, **kwargs):

        self.Echo = kwargs.get('Echo', False)

        try:
            assert isinstance(self.Echo, bool)
        except ValueError:
                print("Echo must be a boolean")

        try:

            if self._use_conf is None:
                database_conn = postres_creacard_config()
                self._hostname = database_conn["hostname"]
                self._port = database_conn["port"]
                self._database = database_conn["database"]
            else:
                self._hostname = self._use_conf["hostname"]
                self._port = self._use_conf["port"]
                self._database = self._use_conf["database"]

            con = "postgresql://" + self._user + ":" + self._pwd + "@" + self._hostname + ":" + self._port + "/" + self._database
            _engine = create_engine(con, echo=self.Echo)

        except Exception as e:
            print("An error occurred : {}".format(e))

        return _engine.connect()