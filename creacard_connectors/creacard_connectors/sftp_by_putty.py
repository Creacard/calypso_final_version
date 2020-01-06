from creacard_connectors.database_connector import connect_to_database
import datetime
from creacard_connectors.import_configurations import SFTP_connection
from Creacard_Utils.import_credentials import credentials_extractor
import pandas as pd
from Creacard_Utils import LogErrorFromInsert
import subprocess
import time
import os


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


def from_postgres_to_sftp_by_putty(con_name, local_folder, local_filename, query, sftp_file_folder,remote_sftp_folder, logs_folder):


    """SFTP extraction of data from postgreSQL query to remote sftp

        Parameters
        -----------
        con_name: str
            name of the sftp connection
        local_folder: str
            local existing folder where the .csv from postgres will be temporary copied
        local_filename: str
            local existing filename where the .csv from postgres will be temporary copied
        query: str
            PostgreSQL query to extract targeted data (input by user) Usualy a select query
        sftp_file_folder: str
            local folder
        remote_sftp_folder: str
            remote folder
        logs_folder: str
            logs to track

    """

    # useful functions

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

    # log generation

    logger = LogErrorFromInsert.CreateLogger("from_postgres_to_sftp", local_filename + "_", logs_folder)

    try:

        tic = time.time()

        local_filename = "export_" + local_filename + "_" + str(datetime.datetime.now()).replace("-", "").replace(":",
                                                                                                                  "").replace(
            ".", "").replace(" ", "") + ".csv"

        local_path = local_folder + local_filename

        # extract data from query to csv using a built-in postgres function
        # by passing query as argument and destination file

        cmd = """

        COPY ({}) TO '{}' WITH CSV HEADER;

        """.format(query, local_path)

        engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()

        engine.execute(cmd)

        engine.close()

        # create a sftp configuration file to put the file trough putty
        sftp_file_filename = local_filename.replace(".csv", "") + ".sftp"

        f = open(sftp_file_folder + sftp_file_filename, "w+")
        f.write("cd " + remote_sftp_folder + "\n" + "put " + local_path)
        f.close()

        username, password, host, port, ii = extract_SFTP_connection("SFTP", con_name, None, None)
        con = "psftp {}@{} -P {} -pw {}".format(username, host, port, password, remote_sftp_folder)
        print(con)

        subprocess.call("{} -b {}".format(con, sftp_file_folder + sftp_file_filename))
        print("{} -b {} -b {}".format(con, sftp_file_folder + sftp_file_filename))

        print("The extraction process from PostgreSQL to sftp took {} seconds".format(time.time() - tic))

        os.remove(local_path)
        os.remove(sftp_file_folder + sftp_file_filename)

    except Exception as e:

        if logger is not None:
            logger.error(e, exc_info=True)
        else:
            print(e)