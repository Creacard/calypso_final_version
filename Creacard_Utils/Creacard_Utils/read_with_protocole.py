from creacard_connectors.ftp_connector import connect_to_FTP
from creacard_connectors.sftp_connector import connect_to_SFTP
import pandas as pd
import os
import glob

def read_csv_protocole(protocole_type, protocole_name, filepath, csv_params, **kwargs):

    _local_filesystem_path = kwargs.get('copy_to_local', None)

    i = 0
    folder_path = ""
    folder_splitted = filepath.split("/")

    while i < len(folder_splitted) - 1:
        folder_path = folder_path + str(folder_splitted[i]) + "/"

        i += 1

    filename = filepath.split("/")[-1]


    if protocole_type == "LOCAL":

        if csv_params is not None:
            data = pd.read_csv(filepath, **csv_params)

        else:
            data = pd.read_csv(filepath)

    elif protocole_type == "FTP":
        data = connect_to_FTP(protocole_type, protocole_name).extract_csv_from_ftp_to_dataframe(filename,
                                                                                folder=folder_path,
                                                                                csv_params=csv_params)
    elif protocole_type == "SFTP":
        data = connect_to_SFTP(protocole_type, protocole_name).extract_csv_from_sftp_to_dataframe(filename,
                                                                                                folder=folder_path,
                                                                                                csv_params=csv_params)

    if _local_filesystem_path is not None:

        folder_destination = _local_filesystem_path["destination_folder"]
        csv_destination_params = _local_filesystem_path["csv_destination_params"]

        if csv_destination_params is not None:

            data.to_csv(folder_destination+filename, **csv_destination_params)

        else:

            data.to_csv(folder_destination + filename, index=False)

    return data


def list_files_protocole(protocole_type, protocole_name, folder):

    if protocole_type == "LOCAL":

        os.chdir(folder)
        _file_list = glob.glob('**.csv')
        _file_list = pd.DataFrame(_file_list, columns=["FileName"])

    elif protocole_type == "FTP":

        _file_list = pd.DataFrame(
            connect_to_FTP(protocole_type, protocole_name).get_filenames_from_folder(folder=folder),
            columns=["FileName"])

        _file_list = _file_list[_file_list["FileName"].astype(str).str.contains('^(.*?)\.csv',
                                                                             regex=True,
                                                                             case=False)]
        _file_list = _file_list.reset_index(drop=True)

    elif protocole_type == "SFTP":

        _file_list = pd.DataFrame(
            connect_to_SFTP(protocole_type, protocole_name).get_filenames_from_folder(folder=folder),
            columns=["FileName"])

        _file_list = _file_list[_file_list["FileName"].astype(str).str.contains('^(.*?)\.csv',
                                                                             regex=True,
                                                                             case=False)]
        _file_list = _file_list.reset_index(drop=True)



    return _file_list

