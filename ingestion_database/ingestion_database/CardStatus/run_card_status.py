from Creacard_Utils.read_with_protocole import *
from ingestion_database.CardStatus.add_new_card_status import daily_card_status2
import time
import datetime
from Creacard_Utils import LogErrorFromInsert
from Postgres_Toolsbox.Ingestion import InsertTableIntoDatabase
from creacard_connectors.sftp_by_putty import copy_by_putty



def run():
    ### Inputs
    folder_2 = "F:/HISTORIQUE/CARD_STATUS/CardStatus2/"

    csv_params = {'sep': ",", 'error_bad_lines': True, 'encoding': 'iso-8859-1', 'header': None}
    ingestion_params = dict()
    ingestion_params["protocole_type"] = "SFTP"
    ingestion_params["protocole_name"] = "pfs_sftp"
    ingestion_params["csv_params"] = csv_params
    ingestion_params["copy_to_filesystem"] = None

    Folder = "PCSFrance/CardStatus2/"
    batch_folder = "C:/Users/creacard/Documents/launchers/card_status/"

    database_type = "Postgres"
    database_name = "Creacard_Calypso"

    logger = LogErrorFromInsert.CreateLogger("card_status", "Ingestion_", 'F:/LOGS/')

    tmp_ingestion = dict()

    # params task checkers
    param_task_checker = {}
    param_task_checker["TASK_TYPE"] = "TEXT"
    param_task_checker["TASK_DT"] = "timestamp without time zone"
    param_task_checker["STATUS"] = "TEXT"

    # date_now = datetime.datetime.now()
    date_now = datetime.datetime.now()

    _file_date_condition = dict()
    _file_date_condition["start"] = [date_now.year, date_now.month, date_now.day]
    _file_date_condition["end"] = [date_now.year, date_now.month, date_now.day]

    ###

    ListOfFile = list_files_protocole(ingestion_params["protocole_type"], ingestion_params["protocole_name"], Folder)

    ii = pd.DataFrame(ListOfFile.FileName.str.replace(".csv", ""), columns=["FileName"])
    Month = []
    Year = []
    Day = []
    ii = ii.FileName.str.split('-')
    for i in range(0, len(ii)):
        Year.append(ii[i][1])
        Month.append(ii[i][2])
        Day.append(ii[i][3])

    ListOfFile = pd.concat([ListOfFile, pd.DataFrame(Year, columns=['Year']), pd.DataFrame(Month, columns=['Month']),
                            pd.DataFrame(Day, columns=['Day'])],
                           axis=1)
    ListOfFile["FilePath"] = Folder + ListOfFile["FileName"]
    ListOfFile["FileTime"] = ListOfFile["Year"] + "-" + ListOfFile["Month"] + "-" + ListOfFile["Day"]
    ListOfFile["FileTime"] = pd.to_datetime(ListOfFile.FileTime, errors='coerce')

    if _file_date_condition is not None:
        if len(_file_date_condition) == 2:
            ListOfFile = ListOfFile[
                (ListOfFile.loc[:, "FileTime"] >= datetime.datetime(_file_date_condition["start"][0],
                                                                    _file_date_condition["start"][1],
                                                                    _file_date_condition["start"][2]))
                & (ListOfFile.loc[:, "FileTime"] <= datetime.datetime(_file_date_condition["end"][0],
                                                                      _file_date_condition["end"][1],
                                                                      _file_date_condition["end"][2]))]
        else:
            ListOfFile = ListOfFile[
                (ListOfFile.loc[:, "FileTime"] >= datetime.datetime(_file_date_condition["start"][0],
                                                                    _file_date_condition["start"][1],
                                                                    _file_date_condition["start"][2]))]

    ListOfFile = ListOfFile.reset_index(drop=True)

    try:

        copy_by_putty(Folder, str(ListOfFile.loc[0, "FileName"]), folder_2, batch_folder, "batch_putty_card_status.bat",
                      "args_card_status_batch.sftp")

        print(folder_2 + str(ListOfFile.loc[0, "FileName"]))

        # read the data
        Data = read_csv_protocole("LOCAL", None, folder_2 + str(ListOfFile.loc[0, "FileName"]),
                                  ingestion_params["csv_params"])
        print(Data.head())

        daily_card_status2(Data, folder_2 + str(ListOfFile.loc[0, "FileName"]))

        tmp_ingestion["TASK_TYPE"] = "card_status_dayli"
        tmp_ingestion["TASK_DT"] = str(datetime.datetime.now())
        tmp_ingestion["STATUS"] = "ok"

        tmp_ingestion = pd.DataFrame.from_dict(tmp_ingestion, orient="index").T

        InsertTableIntoDatabase(tmp_ingestion,
                                "STATUS_TASKS",
                                "TASKS_CHECKER",
                                "Postgres",
                                "Creacard_Calypso",
                                DropTable=False, TableDict=param_task_checker)

    except Exception as e:

        tmp_ingestion["TASK_TYPE"] = "card_status_dayli"
        tmp_ingestion["TASK_DT"] = str(datetime.datetime.now())
        tmp_ingestion["STATUS"] = "failed"

        tmp_ingestion = pd.DataFrame.from_dict(tmp_ingestion, orient="index").T

        InsertTableIntoDatabase(tmp_ingestion,
                                "STATUS_TASKS",
                                "TASKS_CHECKER",
                                "Postgres",
                                "Creacard_Calypso",
                                DropTable=False, TableDict=param_task_checker)

        if logger is not None:
            logger.error(e, exc_info=True)

