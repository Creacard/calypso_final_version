from ftplib import FTP
import pandas as pd
import io
from ftplib import FTP_TLS
from StringIO import StringIO
from Creacard_Utils.import_credentials import credentials_extractor
from creacard_connectors.import_configurations import FTP_connection


class connect_to_FTP(object):

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
    _port      = None
    def __init__(self, protocole_type, protocole_name, **kwargs):

        self._protocole_type = protocole_type
        self._protocole_name = protocole_name
        self._use_conf = kwargs.get('use_conf', None)
        self._use_credentials = kwargs.get('use_credentials', None)

        self._user, self._pwd, self._hostname, self._port = extract_FTP_connection(self._protocole_type,
                                                                       self._protocole_name,
                                                                       self._use_conf,
                                                                       self._use_credentials)

    def create_connection(self):

        try:
            ftp_connection = create_FTP_connection(self._protocole_type, self._user,
                              self._pwd,
                              self._hostname,
                              self._port)
        except Exception as e:
            print("An error occurred : {}".format(e))

        return ftp_connection

    def get_filenames_from_folder(self, folder=None):

        session = create_FTP_connection(self._protocole_type, self._user, self._pwd, self._hostname, self._port)

        if folder is not None:

            session.cwd(session.pwd() + folder)

        list_file = session.nlst()

        session.close()

        return list_file

    def write_dataframe_into_FTP(self, data, filename, folder=None, **kwargs):

        _csv_params = kwargs.get('csv_params', None)

        session = create_FTP_connection(self._protocole_type, self._user, self._pwd, self._hostname, self._port)

        tmp = StringIO()  # create buffer (or cash memory)
        # store the pandas dataframe into the buffer

        if _csv_params is not None:

            data.to_csv(tmp, **_csv_params)
        else:
            data.to_csv(tmp, index=False)

        bio = io.BytesIO(tmp.getvalue())


        # tmp.getvalue() # print binary values
        # setup the folder where you want to write the file
        if folder is not None:
            session.cwd(session.pwd() + folder)

        session.storbinary('STOR {}.csv'.format(filename), bio)  # save the file in the FTP server

        session.close()


    def extract_csv_from_ftp_to_dataframe(self, filename, **kwargs):

        folder = kwargs.get('folder', None)
        csv_params = kwargs.get('csv_params', None)

        session = create_FTP_connection(self._protocole_type, self._user, self._pwd, self._hostname, self._port)

        if folder is not None:

            session.cwd(session.pwd() + folder)

        # create a virtual file object
        download_file = io.BytesIO()

        # write the file into the virtual file object
        session.retrbinary("RETR {}".format(filename), download_file.write)
        download_file.seek(0)

        if csv_params is not None:
            file_extracted = pd.read_csv(download_file, **csv_params)
        else:
            file_extracted = pd.read_csv(download_file)

        session.close()

        return file_extracted


def extract_FTP_connection(_protocole_type,_protocole_name, _use_conf, _use_credentials):

    if _use_conf is None:
        _FTP_connection = FTP_connection(_protocole_type, _protocole_name)
        _hostname = _FTP_connection["hostname"]
        _port = _FTP_connection["port"]
        if _port == "None":
            _port = None
    else:
        _hostname = _use_conf["hostname"]
        _port = _use_conf["port"]

    if _use_credentials is None:
        cred = credentials_extractor().get_ftp_credentials(_protocole_type, _protocole_name)
        _user = cred["user"]
        _pwd = cred["pwd"]

    else:
        _user = _use_credentials["user"]
        _pwd = _use_credentials["pwd"]


    return _user, _pwd, _hostname, _port

def create_FTP_connection(protocole_type, _user, _pwd, _hostname, _port):

    if protocole_type == "FTP":
        ftp_connection = FTP(_hostname)
        ftp_connection.login(user=_user, passwd=_pwd)
    else:
        ftp_connection = FTP_TLS()
        ftp_connection.connect(_hostname, _port)
        ftp_connection.login(user=_user, passwd=_pwd)

    return ftp_connection






