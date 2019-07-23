from creacard_connectors.ftp_connector import connect_to_FTP
import pandas as pd
import os
import glob

def read_csv_protocole(protocole_type, protocole_name, filepath, csv_params):

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


    return _file_list

