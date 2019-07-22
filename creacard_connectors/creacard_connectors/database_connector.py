""" Connection to database """

# Author: Justin Valet <jv.datamail@gmail.com>
# Date: 15/1/2019


from creacard_connectors.import_configurations import database_connection
from creacard_connectors.construct_connector import construct_connection_to_database
from sqlalchemy import create_engine
from Creacard_Utils.import_credentials import credentials_extractor

class connect_to_database(object):

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
    _database_name  = None
    _database_type = None

    def __init__(self, database_type, database_name, **kwargs):

        self._database_name = database_name
        self._database_type = database_type
        self._use_conf = kwargs.get('use_conf', None)
        self._use_credentials = kwargs.get('use_credentials', None)

    def CreateEngine(self, **kwargs):

        self.Echo = kwargs.get('Echo', False)

        try:
            assert isinstance(self.Echo, bool)
        except ValueError:
                print("Echo must be a boolean")

        try:

            if self._use_conf is None:
                database_conn = database_connection(self._database_type, self._database_name)
                self._hostname = database_conn["hostname"]
                self._port = database_conn["port"]
                self._database = database_conn["database"]
            else:
                self._hostname = self._use_conf["hostname"]
                self._port = self._use_conf["port"]
                self._database = self._use_conf["database"]

            if self._use_credentials is None:
                cred = credentials_extractor().get_database_credentials(self._database_type, self._database_name)
                self._user = cred["user"]
                self._pwd = cred["pwd"]

            else:
                self._user = self._use_credentials["user"]
                self._pwd = self._use_credentials["pwd"]

            _connector = construct_connection_to_database(self._database_type)


            con = str(_connector) + self._user + ":" + self._pwd + "@" + self._hostname + ":" + self._port + "/" + self._database
            _engine = create_engine(con, echo=self.Echo)

        except Exception as e:
            print("An error occurred : {}".format(e))

        return _engine.connect()