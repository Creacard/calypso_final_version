import pandas as pd
import datetime
from Creacard_Utils.read_with_protocole import list_files_protocole
from ingestion_database.ActivationReport.transform_activation import transform_activation_report
from Postgres_Toolsbox import Ingestion



def new_activation_report(database_type, database_name, Schema, Folder, ingestion_params, **kwargs):

    """import transactions in a .csv format to postgres

        Requiered Parameters
        -----------
        credentials: dict
            dictionnary compose of user and pwd
        Schema : str
            Indicates the name of the schema where the table is stores into the database
        Folder: str
            The Folder where the data is stored

        Optional Parameters (**kwargs)
        -----------
        TlbName : str
            Name of the targeted table into the database
        InsertInParrell : Boolean -- default value False
            True if the insertion has to be done in parallel
        InsertInTheSameTable : Boolean -- default value False
            True if the pandas dataframe has to be inserted into the same
            table at each loop
        PreprocessingCsv: dict - optional parameters
            Dictionnary with a function that transform a pandas DataFrame
            and a set of optional arguments with this function.
            Dictionnary args:
                - 'function' = function object
                - 'KeyWords' = dict -- with optional args for the function
        logger: logger object
            logger to get logs from the running function in case
            of errors
        workers: int
            the number of workers to run the code in parallel
        file_date_condition: dict
            a dictionnary that indicates the files that need to be ingested
            based on a dates

        ex:
            file_date_condition = dict()
            date_start = datetime.datetime.now() - datetime.timedelta(days=1)
            date_end = datetime.datetime.now()
            file_date_condition["start"] = [date_start.year, date_start.month, date_start.day]
            file_date_condition["end"] = [date_end.year, date_end.month, date_end.day]

     """


    logger = kwargs.get('logger')
    workers = kwargs.get('workers', 3)
    _file_date_condition = kwargs.get('file_date_condition', None)

    TableParameter = {}
    TableParameter["ActivationTime"] = "timestamp without time zone"
    TableParameter["CardHolderID"] = "VARCHAR (50)"
    TableParameter["FileSource"] = "TEXT"


    ListOfFile = list_files_protocole(ingestion_params["protocole_type"], ingestion_params["protocole_name"], Folder)


    Month = []
    Year = []
    Day = []
    # create a pandas DataFrame that contain
    # file and associated year and month in order
    # to ingest .csv file for each month
    ii = ListOfFile.FileName.str.split('_')
    for i in range(0, len(ii)):
        Year.append(ii[i][-1][0:4])
        Month.append(ii[i][-1][4:6])
        Day.append(ii[i][-1][6:8])

    ListOfFile = pd.concat([ListOfFile, pd.DataFrame(Year, columns=['Year']), pd.DataFrame(Month, columns=['Month']),
                                pd.DataFrame(Day, columns=['Day'])],
                               axis=1)

    ListOfFile["FilePath"] = Folder + ListOfFile["FileName"]
    ListOfFile["FileTime"] = ListOfFile["Year"] + "-" + ListOfFile["Month"] + "-" + ListOfFile["Day"]
    ListOfFile["FileTime"] = pd.to_datetime(ListOfFile.FileTime, errors='coerce')
    ListOfFile["FileTimeDelta"] = ListOfFile["FileTime"] - datetime.timedelta(days=1)

    ListOfFile["Year"] = ListOfFile.FileTimeDelta.dt.year
    ListOfFile["Month"] = ListOfFile.FileTimeDelta.dt.month
    ListOfFile["Day"] = ListOfFile.FileTimeDelta.dt.month

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

    FinalKeywords = dict(function=transform_activation_report, KeyWords=None)



    TlbName = "ACTIVATION_REPORT"

    # Ingestion part
    output_tmp = Ingestion.FromCsvToDataBase(ListOfFile["FilePath"],
                                             database_type=database_type,
                                             database_name=database_name,
                                             Schema=Schema,
                                             ingestion_params=ingestion_params,
                                             TlbName=TlbName,
                                             logger=logger,
                                             TableDict=TableParameter,
                                             InsertInTheSameTable=True,
                                             InsertInParrell=True,
                                             PreprocessingCsv=FinalKeywords,
                                             NumWorkers=workers)

    return output_tmp












