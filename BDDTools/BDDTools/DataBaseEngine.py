""" Connection to database """

# Author: Justin Valet <jv.datamail@gmail.com>
# Date: 15/1/2019



from sqlalchemy import create_engine
import json

def postres_creacard_config():

    folder_json = "/Users/justinvalet/CreacardProject/Configurations/database_connection.json"
    with open(folder_json, 'r') as JSON:
        con = json.load(JSON)

    return con


class ConnectToPostgres(object):

    """Create an sqlalchemy connection to the database

        Parameters
        -----------
        user : str
            User name for database credentials
        pwd : str
            Password for database connection
        hostname: str
            IP address of host name of the database
        port : str
            open port to connect to the database
        database : str
            Database name

        Returns
        -----------
        engine : sqlachemy object
            A sqlalchemy object from create_engine (see docs)

    """

    _user           = None
    _pwd            = None
    _hostname       = None
    _database       = None
    _engine         = None
    _openConnection = None

    def __init__(self, user=None, pwd=None, hostname=None, port=None, database=None):
        self._user     = user
        self._pwd      = pwd
        self._hostname = ""
        self._port     = ""
        self._database = ""

    def CreateEngine(self, **kwargs):

        Echo   = kwargs.get('Echo')
        logger = kwargs.get('logger')

        if Echo is None:
            Echo = False
        else:
            if ~isinstance(Echo, bool):
                if logger is not None:
                    logger.error("The optional argmument 'Echo' is not a boolean", exc_info=True)
                    exit()
                else:
                    exit()
            else:
                pass

        database_conn = postres_creacard_config()
        self._hostname = database_conn["hostname"]
        self._port = database_conn["port"]
        self._database = database_conn["database"]

        con = "postgresql://" + self._user + ":" + self._pwd + "@" + self._hostname + ":" + self._port + "/" + self._database
        self._engine = create_engine(con, echo=Echo)

        try:
            self._openConnection = self._engine.connect()
        except Exception as e:
            if logger is not None:
                logger.error(e, exc_info=True)
            else:
                print(e)

        return self

