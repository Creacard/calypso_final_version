import pandas as pd
from Postgres_Toolsbox import Ingestion
from ingestion_database.ingest_transactions import CleaningBeforeInsert
import numpy as np
import os
import glob
import datetime
import time
from Creacard_Utils.read_with_protocole import list_files_protocole


def ingest_csv_transactions(database_type, database_name, Schema, Folder, ingestion_params, **kwargs):

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
    ins_in_parallel = kwargs.get('insert_in_parallel', False)


    # Create the dictionnary in order to insure you
    # that at each ingestion of .csv file
    # you keep the same data type into the
    # database
    TableParameter = {}
    TableParameter["Amount"] = "double precision"
    TableParameter["AuthNum"] = "VARCHAR (50)"
    TableParameter["CardHolderID"] = "VARCHAR (50)"
    TableParameter["CardNumber"] = "VARCHAR (50)"
    TableParameter["CardProductName"] = "VARCHAR (20)"
    TableParameter["CardType"] = "VARCHAR (20)"
    TableParameter["CardVPUType"] = "VARCHAR (50)"
    TableParameter["Currency"] = "VARCHAR (20)"
    TableParameter["CurrencyCode"] = "VARCHAR (20)"
    TableParameter["DebitCredit"] = "VARCHAR (20)"
    TableParameter["Description"] = "TEXT"
    TableParameter["DistributorCode"] = "INTEGER"
    TableParameter["Fee"] = "double precision"
    TableParameter["FileSource"] = "TEXT"
    TableParameter["LastName"] = "VARCHAR (50)"
    TableParameter["MCC"] = "TEXT"
    TableParameter["MerchantAddress"] = "TEXT"
    TableParameter["MerchantCity"] = "TEXT"
    TableParameter["MerchantCountry"] = "TEXT"
    TableParameter["MerchantID"] = "VARCHAR(50)"
    TableParameter["MerchantName"] = "TEXT"
    TableParameter["ProgramName"] = "VARCHAR(50)"
    TableParameter["PurseCurrency"] = "VARCHAR (20)"
    TableParameter["Rate"] = "double precision"
    TableParameter["RemainingBalance"] = "double precision"
    TableParameter["Surcharge"] = "double precision"
    TableParameter["SweepCurrency"] = "VARCHAR(20)"
    TableParameter["TermState"] = "TEXT"
    TableParameter["TransactionDB/CR"] = "VARCHAR (40)"
    TableParameter["TransactionID"] = "VARCHAR (40)"
    TableParameter["TransactionResult"] = "VARCHAR (40)"
    TableParameter["TransactionTP"] = "VARCHAR (60)"
    TableParameter["TransactionTime"] = "timestamp without time zone"

    # Create the list of files that must be
    # ingested into the data base
    # I suggest created different folder for the different month &
    # dayli ingestion

    ListOfFile = list_files_protocole(ingestion_params["protocole_type"], ingestion_params["protocole_name"], Folder)

    Month = []
    Year = []
    Day = []
    # create a pandas DataFrame that contain
    # file and associated year and month in order
    # to ingest .csv file for each month
    ii = ListOfFile.FileName.str.split('_')
    for i in range(0, len(ii)):
        print(ii[i][1])
        Day.append(ii[i][1][0:2])
        Month.append(ii[i][1][2:4])
        Year.append("20" + ii[i][1][4:6])

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
                (ListOfFile.loc[:, "FileTimeDelta"] >= datetime.datetime(_file_date_condition["start"][0],
                                                                         _file_date_condition["start"][1],
                                                                         _file_date_condition["start"][2]))
                & (ListOfFile.loc[:, "FileTimeDelta"] <= datetime.datetime(_file_date_condition["end"][0],
                                                                           _file_date_condition["end"][1],
                                                                           _file_date_condition["end"][2]))]
        else:
            ListOfFile = ListOfFile[
                (ListOfFile.loc[:, "FileTimeDelta"] >= datetime.datetime(_file_date_condition["start"][0],
                                                                         _file_date_condition["start"][1],
                                                                         _file_date_condition["start"][2]))]

    tic = time.time()
    ListToIterate = list(np.unique(ListOfFile.Year.astype(str) + "_" + ListOfFile.Month.astype(str)))


    print(ListOfFile)

    # Function for preprocessing the data before the ingestion
    # This is a dictionnary that takes in arguments
    # - function to preprocess the data (output dataframe)
    # - KeysWords of optinal argument for the function
    FinalKeywords = dict(function=CleaningBeforeInsert.cleaning_before, KeyWords=None)

    # iterate on each month in order to create / ingest
    # .csv dayli files into the associated monthly table
    _output_ingestion = dict()
    for k in range(0, len(ListToIterate)):
        ListFinal = list(ListOfFile["FilePath"][(ListOfFile.Month == int(ListToIterate[k].split('_')[1])) & (
                    ListOfFile.Year == int(ListToIterate[k].split('_')[0]))])
        print(ListFinal)
        # Name of the table
        TlbNameMonth = "MONTHLY_TRANSACTIONS_" + ListToIterate[k].split('_')[0] + ListToIterate[k].split('_')[1]
        # Ingestion part
        output_tmp = Ingestion.FromCsvToDataBase(ListFinal,
                                                 database_type=database_type,
                                                 database_name=database_name,
                                                 Schema=Schema,
                                                 ingestion_params=ingestion_params,
                                                 TlbName=TlbNameMonth,
                                                 logger=logger,
                                                 TableDict=TableParameter,
                                                 InsertInTheSameTable=True,
                                                 InsertInParrell=ins_in_parallel,
                                                 PreprocessingCsv=FinalKeywords,
                                                 NumWorkers=workers)

        _output_ingestion[str(ListToIterate[k])] = output_tmp

    toc = time.time() - tic

    print("The ingestion was done in {} seconds".format(toc))

    return _output_ingestion