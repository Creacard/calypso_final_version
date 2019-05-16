""" Postgres database ingestion of dataframe & csv"""

# Author: Justin Valet <jv.datamail@gmail.com>
# Date: 01/11/2018
#

import time
from BDDTools import DbTools as db
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
import multiprocessing
from Utils.SplitData import splitDataFrameIntoSmaller
from BDDTools.DetectTypePostgres import CreateDictionnaryType
from sqlalchemy import *
from BDDTools.DataBaseEngine import ConnectToPostgres

""" Ingestion for sparse pandas DataFrame """

def InsertTableIntoDatabase(Data, TlbName, Schema, credentials, **kwargs):
    """Insert a pandas Dataframe
        Requiered Parameters
        -----------
        engine : sqlalchmey create_engine object
            Engin object  & connection to the database from sqlalchemy
        TlbName : str
            Name of the targeted table into the database
        Schema: str
            Indicate the schema where the table is stores into the database

        Optional Parameters (**kwargs)
        -----------
        logger: logger object
            logger to get logs from the running function in case
            of errors
        TableDict: dict
            Dictionnary with the postgres types associated to the variables
            ingested from the DataFrame
        DropTable : Boolean -- default value False
            True if the table has to be dropped before ingestion
        InsertInParrell : Boolean -- default value False
            True if the insertion has to be done in parallel

    """

    InsertInParrell = kwargs.get('InsertInParrell', False)
    SizeParrell     = kwargs.get('SizeParrell', 10000)
    logger          = kwargs.get('logger', None)
    TableDict       = kwargs.get('TableDict', None)
    DropTable       = kwargs.get('DropTable', False)
    SizeChunck      = kwargs.get('SizeChunck', 10000)
    NumWorkers      = kwargs.get('NumWorkers', 3)

    PostgresConnect = ConnectToPostgres(credentials)
    engine = PostgresConnect.CreateEngine()

    # Test if the targeted schema exists
    try:
        if isinstance(Schema, str):
            if ~db.IsSchemaExist(engine, Schema):
                db.CreateSchema(engine, Schema)
        else:
            raise ValueError("'Schema' must have a string format")
    except:
        raise



    # Variables type in postgres
    if TableDict is not None:
        try:
            # Check if the table exist
            if not db.table_exists(engine, TlbName, Schema):
                db.CreateTable(engine, TlbName, Schema, TableDict)
            else:
                if DropTable:
                    metadata = MetaData()
                    TlbObject = Table(TlbName, metadata, schema=Schema)
                    TlbObject.drop(engine)
                    db.CreateTable(engine, TlbName, Schema, TableDict)
        except Exception as e:
            if logger is not None:
                logger.error(e, exc_info=True)
            else:
                print(e)
    else:
        TableDict = CreateDictionnaryType(Data)
        try:
            # Check if the table exist
            if not db.table_exists(engine, TlbName, Schema):
                db.CreateTable(engine, TlbName, Schema, TableDict)
            else:
                if DropTable:
                    metadata = MetaData()
                    TlbObject = Table(TlbName, metadata, schema=Schema)
                    TlbObject.drop(engine)
                    db.CreateTable(engine, TlbName, Schema, TableDict)
        except Exception as e:
            if logger is not None:
                logger.error(e, exc_info=True)
            else:
                print(e)

    # Insert in paralell or not
    if InsertInParrell:

        del engine

        if NumWorkers is None:
            # Store the number of available workers
            NbWorkers = multiprocessing.cpu_count() - 1
        else:
            NbWorkers = NumWorkers
        # Split the data into different chunck size
        DataSplitted = splitDataFrameIntoSmaller(Data, chunkSize=SizeParrell)
        print("Launch multi-insertion of sample {} rows on {} Workers".format(NbWorkers,SizeParrell))

        tic = time.time()
        p = ThreadPool(NbWorkers)
        p.map(partial(db.InsterToPostgre, engine=None, TlbName=TlbName, schema=Schema, DropTable=False, credentials=credentials), DataSplitted)
        p.close()
        p.join()
        toc = time.time() - tic
        print("The DataFrame was succesfully ingested in parallel into the table {} in {} seconds".format(TlbName, toc))

    else:
        tic = time.time()
        db.InsterToPostgre(Data, engine=engine, TlbName=TlbName, schema=Schema, DropTable=False, SizeChunck=SizeChunck)
        toc = time.time() - tic
        print("The DataFrame was succesfully ingested into the table {} in {} seconds".format(TlbName, toc))

    engine.close()

    return


""" Ingestion for multiple .csv files with the same structure i.e number of columns  """

def FromCsvToDataBase(ListOfPath, credentials, Schema, **kwargs):
    """Insert all .csv from a folder

        Requiered Parameters
        -----------
        ListOfPath: str
            Path of the folder where .csv files are located
        engine : sqlalchmey create_engine object
            Engin object  & connection to the database from sqlalchemy
        Schema: str
            Indicate the schema where the table is stores into the database

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
        TableDict: dict
            Dictionnary with the postgres types associated to the variables
            ingested from the DataFrame

     """

    TlbName              = kwargs.get('TlbName', None)
    InsertInParrell      = kwargs.get('InsertInParrell', False)
    InsertInTheSameTable = kwargs.get('InsertInTheSameTable', False)
    PreprocessingCsv     = kwargs.get('PreprocessingCsv', None)
    logger               = kwargs.get('logger', None)
    TableDict            = kwargs.get('TableDict', None)
    SizeChunck           = kwargs.get('SizeChunck', 10000)
    NumWorkers           = kwargs.get('NumWorkers', 3)

    PostgresConnect = ConnectToPostgres(credentials)
    engine = PostgresConnect.CreateEngine()

    # Test if the targeted schema exists
    if isinstance(Schema, str):
        if ~db.IsSchemaExist(engine, Schema):
            db.CreateSchema(engine, schema)

    if InsertInTheSameTable is not None:
        if TableDict is not None:
            try:
                # Check if the table exist
                # The function will automatically create the
                # if it doesn't exist
                if not db.table_exists(engine, TlbName, Schema):
                    db.CreateTable(engine, TlbName, Schema, TableDict)
            except Exception as e:
                if logger is not None:
                    logger.error(e, exc_info=True)
                else:
                    print(e)
        else:
            raise ValueError("You must specify a dictionnary to insert into the same table")

        # The .csv are ingested in the table if the user choose
        # the insertion in parallel
        if InsertInParrell:

            del engine

            if NumWorkers is None:
                # Store the number of available workers
                NbWorkers = multiprocessing.cpu_count() - 1
            else:
                NbWorkers = NumWorkers
            print(".csv simultaneous ingestion of {} files using {} Workers is launched".format(len(ListOfPath), NbWorkers))

            tic = time.time()
            p = ThreadPool(NbWorkers)
            p.map(partial(db.CsvToDataBase, engine=None, TlbName=TlbName, Schema=Schema, logger=logger, SizeChunck=SizeChunck,
                          PreprocessingCsv=PreprocessingCsv, InsertInTheSameTable=True, credentials=credentials), ListOfPath)
            toc = time.time() - tic
            print(".csv files were succesfully ingested in parallel into the table {} in {} seconds".format(TlbName, toc))

        else:
            tic = time.time()
            for i in ListOfPath:
                db.CsvToDataBase(i, engine=engine, TlbName=TlbName, Schema=Schema,SizeChunck=SizeChunck, PreprocessingCsv=PreprocessingCsv, InsertInTheSameTable=True)
                print("{} was succesfully ingested".format(i))
            toc = time.time() - tic
            print(".csv files were succesfully ingested into the table {} in {} seconds".format(TlbName, toc))

    else:

        # The .csv are ingested in the table if the user choose
        # the insertion in parallel
        if InsertInParrell:

            del engine

            if NumWorkers is None:
                # Store the number of available workers
                NbWorkers = multiprocessing.cpu_count() - 1
            else:
                NbWorkers = NumWorkers
            print(".csv simultaneous ingestion of {} files using {} Workers is launched".format(len(ListOfPath),
                                                                                                NbWorkers))

            tic = time.time()
            p = ThreadPool(NbWorkers)
            p.map(partial(db.CsvToDataBase, engine=None, TlbName=TlbName, Schema=Schema, logger=logger,
                          SizeChunck=SizeChunck,
                          PreprocessingCsv=PreprocessingCsv, InsertInTheSameTable=False, credentials=credentials), ListOfPath)
            toc = time.time() - tic
            print(
                ".csv files were succesfully ingested in parallel into the table {} in {} seconds".format(TlbName, toc))

        else:
            tic = time.time()
            for i in ListOfPath:
                db.CsvToDataBase(i, engine=engine, TlbName=TlbName, Schema=Schema, SizeChunck=SizeChunck,
                                 PreprocessingCsv=PreprocessingCsv, InsertInTheSameTable=False)
                print("{} was succesfully ingested".format(i))
            toc = time.time() - tic
            print(".csv files were succesfully ingested into the table {} in {} seconds".format(TlbName, toc))

        engine.close()

    return
