import pysftp
import pandas as pd
import io
from StringIO import StringIO
from Creacard_Utils.import_credentials import credentials_extractor
from creacard_connectors.import_configurations import SFTP_connection


class connect_to_SFTP(object):

    """Create an ftp connection to the database

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
    _private_key    = None
    _port      = None
    def __init__(self, protocole_type, protocole_name, **kwargs):

        self._protocole_type = protocole_type
        self._protocole_name = protocole_name
        self._use_conf = kwargs.get('use_conf', None)
        self._use_credentials = kwargs.get('use_credentials', None)

        self._user, self._pwd, self._hostname, self._port, self._private_key = extract_SFTP_connection(self._protocole_type,
                                                                       self._protocole_name,
                                                                       self._use_conf,
                                                                       self._use_credentials)

    def create_connection(self):

        try:
            sftp_connection = create_sftp_connection(self._hostname,
                                                    self._port,
                                                    self._private_key,
                                                    self._user,
                                                    self._pwd)

        except Exception as e:
            print("An error occurred : {}".format(e))

        return sftp_connection

    def get_filenames_from_folder(self, folder=None):

        session = create_sftp_connection(self._hostname,
                                                    self._port,
                                                    self._private_key,
                                                    self._user,
                                                    self._pwd)

        if folder is not None:

            session.cwd(folder)

        list_file = session.listdir()

        session.close()

        return list_file


    def extract_csv_from_sftp_to_dataframe(self, filename, **kwargs):

        folder = kwargs.get('folder', None)
        csv_params = kwargs.get('csv_params', None)

        session = create_sftp_connection(self._hostname,
                                                    self._port,
                                                    self._private_key,
                                                    self._user,
                                                    self._pwd)

        if folder is not None:

            folder_path = folder
            filepath = folder_path + filename

        else:

            filepath = filename

        tmp = io.BytesIO()
        session.getfo(filepath, tmp)
        s = str(tmp.getvalue())
        data_tmp = StringIO(s)

        if csv_params is not None:
            file_extracted = pd.read_csv(data_tmp, **csv_params)
        else:
            file_extracted = pd.read_csv(data_tmp)

        session.close()

        return file_extracted



def extract_SFTP_connection(_protocole_type,_protocole_name, _use_conf, _use_credentials):

    if _use_conf is None:
        _SFTP_connection = SFTP_connection(_protocole_type, _protocole_name)
        _hostname = _SFTP_connection["hostname"]
        _port = _SFTP_connection["port"]
        _private_key = parse_private_key(_SFTP_connection["private_key"])
    else:
        _hostname = _use_conf["hostname"]
        _port = _use_conf["port"]
        _private_key = parse_private_key(_use_conf["private_key"])

    if _use_credentials is None:
        cred = credentials_extractor().get_sftp_credentials(_protocole_type, _protocole_name)
        _user = cred["user"]
        _pwd = cred["pwd"]

    else:
        _user = _use_credentials["user"]
        _pwd = _use_credentials["pwd"]

    return _user, _pwd, _hostname, _port, _private_key


def create_sftp_connection(hostname, port, private_key, user, pwd):

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    sftp_con = pysftp.Connection(host=hostname,
                                 username=user,
                                 port=port,
                                 password=pwd,
                                 private_key=private_key, cnopts=cnopts)

    return sftp_con


def parse_private_key(path_key):
    key = pd.read_csv(path_key, header=None, sep=";")
    keyfinal = """"""
    for i in range(0, len(key)):
        keyfinal = keyfinal + str(key.iloc[i, 0])

    return keyfinal