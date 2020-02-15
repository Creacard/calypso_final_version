from creacard_connectors.database_connector import connect_to_database
import datetime
from creacard_connectors.import_configurations import SFTP_connection
from Creacard_Utils.import_credentials import credentials_extractor
import pandas as pd
from Creacard_Utils import LogErrorFromInsert
import subprocess
import time
import os
import re


class extract_data_from_postgres(object):

    """SFTP extraction of data from postgreSQL query to remote sftp

        Requiered Parameters
        -----------

        local_folder: str
            local existing folder where the .csv from postgres will be temporary copied
        local_filename: str
            local existing filename where the .csv from postgres will be temporary copied
        query: str
            PostgreSQL query to extract targeted data (input by user) Usualy a select query
        remote_folder: str
            path to the sftp folder
        postgres_con: str
            name of the postgres connection (json conf)
        sftp_con : str
            name of the sftp connection (json conf)

        Optional Parameters
        -----------

        use_compression: Boolean (True / False)
            should the file be compressed - False by default
        logs : logger object
            logger object to track errors (in production) - None by default
        rm_csv_file: Boolean (True / False)
            Does the extracted file must be remove (after be sent by sftp) - False by default
        file_delimiter: str
            option that allows the user to choose the output csv file delimiter - ';' by default

        Example
        -----------

        from Creacard_Utils import LogErrorFromInsert
        from creacard_connectors.sftp_by_putty import extract_data_from_postgres

        con_name = "kevin_server_sftp"
        local_folder = "F:/FTP/test/"
        local_filename = "card_status_test"
        query = "select * from "TRANSACTIONS"."POS_TRANSACTIONS" limit 1000"

        sftp_file_folder = "F:/FTP/test/"
        remote_sftp_folder = "Desktop\Creacard"
        logs_folder = "F:/LOGS/"
        connexion_name = "Creacard_Calypso"


        logger = LogErrorFromInsert.CreateLogger("from_postgres_to_sftp", "pos_trasactions_", logs_folder)

        extraction = extract_data_from_postgres(local_folder=local_folder,
                                                local_filename = local_filename,
                                                query = query,
                                                remote_sftp_folder = remote_sftp_folder,
                                                remote_sftp_folder = connexion_name,
                                                sftp_con = con_name,
                                                use_compression = False,
                                                logs = logger,
                                                rm_csv_file = False,
                                                file_delimiter = ";")
    """

    _local_folder = None
    _local_filename = None
    _remote_folder = None
    _query = None
    _postgres_con = None
    _sftp_con = None
    _use_compression = None
    _local_path = None
    _loggin = None
    _rm_csv_file = None
    _file_delimiter = None

    def __init__(self, local_folder, local_filename, query, remote_folder, postgres_con, sftp_con, **kwargs):

        self._local_folder = local_folder
        self._local_filename = local_filename
        self._remote_folder = remote_folder
        self._query = query
        self._postgres_con = postgres_con
        self._sftp_con = sftp_con
        self._use_compression = kwargs.get('use_compression', False)
        self._loggin = kwargs.get('logs', None)
        self._rm_csv_file = kwargs.get('rm_csv_file', False)
        self._file_delimiter = kwargs.get('file_delimiter', ';')

    def extract_csv_from_postgres(self):

        try:

            self._local_path, self._local_filename = copy_to_csv(self._query, self._local_filename, self._local_folder,
                                                                 self._postgres_con, compression=self._use_compression,
                                                                 remove_csv_file=self._rm_csv_file, delimiter=self._file_delimiter)

        except Exception as e:
            print("An error occurred : {}".format(e))

            if self._loggin is not None:
                self._loggin.error(e, exc_info=True)
            else:
                print(e)

        return self._local_path, self._local_filename

    def push_file_sftp(self, **kwargs):

        _rm_sent_file = kwargs.get('rm_sent_file', False)

        try:

            self._local_path, self._local_filename = copy_to_csv(self._query, self._local_filename, self._local_folder,
                                                                 self._postgres_con, compression=self._use_compression,
                                                                 remove_csv_file=self._rm_csv_file, delimiter=self._file_delimiter)

            push_file_putty(self._local_filename, self._local_folder, self._remote_folder,
                            self._sftp_con, self._local_path, remove_file=_rm_sent_file)

        except Exception as e:
            print("An error occurred : {}".format(e))

            if self._loggin is not None:
                self._loggin.error(e, exc_info=True)
            else:
                print(e)


# copy a table to csv - postgres

def copy_to_csv(query, local_filename, local_folder, connexion_name, **kwargs):
    tic = time.time()

    to_compress = kwargs.get('compression', False)
    _remove_csv_file = kwargs.get('remove_csv_file', False)
    _delimiter =  kwargs.get('delimiter', ';')

    local_filename = local_filename + "_" + str(datetime.datetime.now())[0:10].replace("-", "").replace(":",
                                                                                                        "").replace(
        ".", "").replace(" ", "") + ".csv"

    local_path = local_folder + local_filename

    # extract data from query to csv using a built-in postgres function
    # by passing query as argument and destination file

    cmd = """

    COPY ({}) TO '{}' WITH CSV HEADER DELIMITER '{}';

    """.format(query, local_path, _delimiter)

    engine = connect_to_database("Postgres", connexion_name).CreateEngine()

    engine.execute(cmd)

    engine.close()

    if to_compress:
        cmd = "Compress-Archive " + local_path + " " + local_folder + local_filename.replace(".csv", "") + ".zip"
        p = subprocess.Popen(['powershell.exe', cmd])
        # wait until the process finishs
        p.wait()
        print("compression of the file is finished")

        local_path = local_folder + local_filename.replace(".csv", "") + ".zip"

    if _remove_csv_file:
        os.remove(local_folder + local_filename)

    print("file extraction from postgres took {} seconds".format(time.time() - tic))

    return local_path, local_filename


def push_file_putty(local_filename, sftp_file_folder, remote_sftp_folder, con_name, local_path, **kwargs):
    _remove_file = kwargs.get('remove_file', False)

    # create a sftp configuration file to put the file trough putty
    pattern = re.compile("\.(.*)")  # replace all after "." by sftp
    sftp_file_filename = pattern.sub(".sftp", local_filename)

    f = open(sftp_file_folder + sftp_file_filename, "w+")
    f.write("cd " + remote_sftp_folder + "\n" + "put " + local_path)
    f.close()

    username, password, host, port, ii = extract_SFTP_connection("SFTP", con_name, None, None)
    con = "psftp {}@{} -P {} -pw {}".format(username, host, port, password, remote_sftp_folder)
    print(con)

    subprocess.call("{} -b {}".format(con, sftp_file_folder + sftp_file_filename))
    print("{} -b {}".format(con, sftp_file_folder + sftp_file_filename))

    os.remove(sftp_file_folder + sftp_file_filename)

    if _remove_file:
        os.remove(local_path)


def extract_SFTP_connection(_protocole_type, _protocole_name, _use_conf, _use_credentials):
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


def parse_private_key(path_key):
    if path_key == "":
        keyfinal = ""
    else:
        key = pd.read_csv(path_key, header=None, sep=";")
        keyfinal = """"""
        for i in range(0, len(key)):
            keyfinal = keyfinal + str(key.iloc[i, 0])

    return keyfinal


def copy_by_putty(folder, filename, destination_folder, batch_folder, batch_file, args_file):

    """SFTP extraction of a file from sftp using putty (requiered putty install - psftp method)

        Parameters
        -----------
        folder: str
            folder of the sftp
        filename: str
            name of the file targeted on the sftp
        destination_folder: str
            local name of the folder where the file needs to be copied
        batch_folder: str
            path of the folder where the batch file file is stored (needs to be the same for args_file)
        batch_file: str
            name of the file where the batch file for putty is store (needs to include credentials and conenction for sftp)
        args_file: str
            get method for sftp

    """



    if os.path.isfile(batch_folder + args_file):
        os.remove(batch_folder + args_file)

    f = open(batch_folder + args_file.split(".")[0] + ".txt", "w+")
    f.write("get {}{} {}{}".format(folder, filename, destination_folder, filename))
    f.close()

    os.rename(batch_folder + args_file.split(".")[0] + ".txt", batch_folder + args_file.split(".")[0] + ".sftp")

    subprocess.call([batch_folder + batch_file])
