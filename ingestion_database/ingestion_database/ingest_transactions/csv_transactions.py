import pandas as pd
from BDDTools import Ingestion
from ingestion_database.ingest_transactions import CleaningBeforeInsert
import numpy as np
import os
import glob
import datetime
import time


def ingest_csv_transactions(credentials, Schema, Folder, **kwargs):

    logger = kwargs.get('logger')
    workers = kwargs.get('workers', 3)

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
    os.chdir(Folder)
    ListOfFile = glob.glob('**.csv')
    ListOfFile = pd.DataFrame(ListOfFile, columns=["FileName"])
    Month = []
    Year = []
    Day = []
    # create a pandas DataFrame that contain
    # file and associated year and month in order
    # to ingest .csv file for each month
    ii = ListOfFile.FileName.str.split('_')
    for i in range(0, len(ii)):
        Day.append(ii[i][1][0:2])
        Month.append(ii[i][1][2:4])
        Year.append("20" + ii[i][1][4:6])

    ListOfFile = pd.concat([ListOfFile, pd.DataFrame(Year, columns=['Year']), pd.DataFrame(Month, columns=['Month']),
                            pd.DataFrame(Day, columns=['Day'])],
                           axis=1)

    ListOfFile["FileTime"] = ListOfFile["Year"] + "-" + ListOfFile["Month"] + "-" + ListOfFile["Day"]
    ListOfFile["FileTime"] = pd.to_datetime(ListOfFile.FileTime, errors='coerce')
    ListOfFile["FileTimeDelta"] = ListOfFile["FileTime"] - datetime.timedelta(days=1)

    ListOfFile["Year"] = ListOfFile.FileTimeDelta.dt.year
    ListOfFile["Month"] = ListOfFile.FileTimeDelta.dt.month

    tic = time.time()
    ListToIterate = list(np.unique(ListOfFile.Year.astype(str) + "_" + ListOfFile.Month.astype(str)))

    # Function for preprocessing the data before the ingestion
    # This is a dictionnary that takes in arguments
    # - function to preprocess the data (output dataframe)
    # - KeysWords of optinal argument for the function
    FinalKeywords = dict(function=CleaningBeforeInsert.cleaning_before, KeyWords=None)

    # iterate on each month in order to create / ingest
    # .csv dayli files into the associated monthly table
    for k in range(0, len(ListToIterate)):
        ListFinal = list(ListOfFile["FileName"][(ListOfFile.Month == int(ListToIterate[k].split('_')[1])) & (
                    ListOfFile.Year == int(ListToIterate[k].split('_')[0]))])
        # Name of the table
        TlbNameMonth = "MONTHLY_TRANSACTIONS_" + ListToIterate[k].split('_')[0] + ListToIterate[k].split('_')[1]
        # Ingestion part
        Ingestion.FromCsvToDataBase(ListFinal, credentials, Schema, TlbName=TlbNameMonth, logger=logger,
                                    TableDict=TableParameter, InsertInTheSameTable=True,
                                    InsertInParrell=True, PreprocessingCsv=FinalKeywords, NumWorkers=workers)

    toc = time.time() - tic

    print("The ingestion was done in {} seconds".format(toc))