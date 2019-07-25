""" Database tools for ingestion """

# Author: Justin Valet <jv.datamail@gmail.com>
# Date: 01/11/2018
# Update1: 21/11/2018
# Update2: 12/12/2018
# Update3: 15/05/2019

from sqlalchemy import *
import pandas as pd
from Postgres_Toolsbox.DetectTypePostgres import CreateDictionnaryType
from creacard_connectors.database_connector import connect_to_database
from Creacard_Utils.read_with_protocole import read_csv_protocole


def table_exists(engine, TlbName, schema):
    """Check if a table exist

        Parameters
        -----------
        engine : sqlalchmey create_engine object
            Engine object  & connection to the database from sqlalchemy
        TlbName : str
            Name of the targeted table into the database
        Schema: str
            Indicate the schema where the table is stores into the database

        Returns
        -----------
        res : Boolean
            True if the table exist and false if not

    """
    res = engine.dialect.has_table(engine, TlbName, schema=schema)
    return res

def DeleteRows(engine,TlbName,Schema):
    """Delete data containing into a database table

        Parameters
        -----------
        engine : sqlalchmey create_engine object
            Engine object  & connection to the database from sqlalchemy
        TlbName : str
            Name of the targeted table into the database
        Schema: str
            Indicate the schema where the table is stores into the database

    """
    querydelete = "Delete From " + '"' + Schema + '"' + "."  + '"' + TlbName + '"'
    engine.execute(querydelete)

def InsertToPostgre(Data, TlbName, schema, engine,  **kwargs):
    """Insert a pandas DataFrame into a database table

        Requiered Parameters
        -----------
        engine : sqlalchmey create_engine object
            Engine object  & connection to the database from sqlalchemy
        TlbName : str
            Name of the targeted table into the database
        Schema: str
            Indicate the schema where the table is stores into the database

        Optional Parameters (**kwargs)
        -----------
        logger: logger object
            logger to get logs from the running function in case
            of errors
        DropTable : Boolean -- default value False
            True if the table has to be dropped before ingestion
        SizeChunck : Integer -- default value 10000


    """

    # store the value of each optional argument
    # if an argument is missing, the default value
    # is none
    logger          = kwargs.get('logger', None)
    DropTable       = kwargs.get('TableDict', False)
    SizeChunck      = kwargs.get('SizeChunck', None)
    TableDict       = kwargs.get('TableDict', None)
    _use_credentials     = kwargs.get('_use_credentials', None)
    _use_conf = kwargs.get('_use_conf', None)
    database_type = kwargs.get('database_type', None)
    database_name = kwargs.get('database_name', None)


    _was_engine = True

    if engine is None:
        try:
            engine = connect_to_database(database_type, database_name,
                                                  _use_credentials=_use_credentials,
                                                  _use_conf=_use_conf).CreateEngine()
            _was_engine = False
        except:
            raise

    # add a default value to DropTable
    if DropTable:
        try:

            # Variables type in postgres
            if TableDict is not None:
                # Check if the table exist
                if not table_exists(engine, TlbName, schema):
                    CreateTable(engine, TlbName, schema, TableDict)

                else:
                    if DropTable:
                        metadata = MetaData()
                        TlbObject = Table(TlbName, metadata, schema=schema)
                        TlbObject.drop(engine)
                        CreateTable(engine, TlbName, schema, TableDict)
            else:
                TableDict = CreateDictionnaryType(Data)

                #Check if the table exist
                if not table_exists(engine, TlbName, schema):
                    CreateTable(engine, TlbName, schema, TableDict)
                else:
                    if DropTable:
                        metadata = MetaData()
                        TlbObject = Table(TlbName, metadata, schema=schema)
                        TlbObject.drop(engine)
                        CreateTable(engine, TlbName, schema, TableDict)

            # Insert into the table
            if SizeChunck is None:
                Data.to_sql(TlbName, con=engine, if_exists='append', schema=schema, index=False)
            else:
                Data.to_sql(TlbName, con=engine, if_exists='append', schema=schema, index=False, chunksize=SizeChunck)

        except Exception as e:
            if logger is not None:
                logger.error(e, exc_info=True)
            else:
                print(e)
    else:
        try:
            if SizeChunck is None:
                Data.to_sql(TlbName, con=engine, if_exists='append', schema=schema, index=False)
            else:
                Data.to_sql(TlbName, con=engine, if_exists='append', schema=schema, index=False, chunksize=SizeChunck)
        except Exception as e:
            if logger is not None:
                logger.error(e, exc_info=True)
            else:
                print(e)

    if not _was_engine:
        engine.close()

def CsvToDataBase(FilePath,TlbName,Schema,ingestion_params, **kwargs):
    """Insert a csv using pandas into a database table

        Parameters
        -----------
        FilePath : str
            Path of the folder where .csv files are located
        engine : sqlalchmey create_engine object
            Engin object  & connection to the database from sqlalchemy
        TlbName : str
            Name of the targeted table into the database
        Schema: str
            Indicate the schema where the table is stores into the database
        PreprocessingCsv: dict - optional parameters
            Dictionnary with a function that transform a pandas DataFrame
            and a set of optional arguments with this function.
            Dictionnary args:
                - 'function' = function object
                - 'KeyWords' = dict -- with optional args for the function

    """

    InsertInTheSameTable = kwargs.get('InsertInTheSameTable', None)
    PreprocessingCsv = kwargs.get('PreprocessingCsv', None)
    _use_credentials = kwargs.get('_use_credentials', None)
    _use_conf = kwargs.get('_use_conf', None)
    engine = kwargs.get('use_engine', None)
    logger = kwargs.get('logger', None)
    SizeChunck = kwargs.get('SizeChunck', None)
    database_type = kwargs.get('database_type', None)
    database_name = kwargs.get('database_name', None)

    _was_engine = True

    _num_lines_csv = None

    if engine is None:
        try:
            engine = connect_to_database(database_type, database_name,
                                         _use_credentials=_use_credentials,
                                         _use_conf=_use_conf).CreateEngine()
            _was_engine = False
        except:
            raise

    try:
        if InsertInTheSameTable:
            if PreprocessingCsv is not None:
                F = PreprocessingCsv['function']
                if PreprocessingCsv['KeyWords'] is not None:

                    # read the data from a specific protocole
                    Data = read_csv_protocole(ingestion_params["protocole_type"],
                                       ingestion_params["protocole_name"],
                                       FilePath, ingestion_params["csv_params"])

                    Data = F(Data, FilePath, PreprocessingCsv['KeyWords'])
                    _num_lines_csv = Data.shape[0]
                else:

                    # read the data from a specific protocole
                    Data = read_csv_protocole(ingestion_params["protocole_type"],
                                              ingestion_params["protocole_name"],
                                              FilePath, ingestion_params["csv_params"])

                    Data = F(Data, FilePath)
                    _num_lines_csv = Data.shape[0]
                    InsertToPostgre(Data,
                                TlbName=TlbName,
                                engine=engine,
                                schema=Schema,
                                DropTable=False, SizeChunck=SizeChunck)
                print("file {} was succesfully inserted".format(FilePath))
            else:

                # read the data from a specific protocole
                Data = read_csv_protocole(ingestion_params["protocole_type"],
                                          ingestion_params["protocole_name"],
                                          FilePath, ingestion_params["csv_params"])

                _num_lines_csv = Data.shape[0]
                InsertToPostgre(Data,
                                TlbName=TlbName,
                                engine=engine,
                                schema=Schema,
                                DropTable=False, SizeChunck=SizeChunck)
                print("file {} was succesfully inserted".format(FilePath))
        else:

            if TlbName is None:
                TlbName = FilePath.split('/')[-1].replace(".csv", "")
            if PreprocessingCsv is not None:
                F = PreprocessingCsv['function']
                if PreprocessingCsv['KeyWords'] is not None:

                    # read the data from a specific protocole
                    Data = read_csv_protocole(ingestion_params["protocole_type"],
                                              ingestion_params["protocole_name"],
                                              FilePath, ingestion_params["csv_params"])

                    Data = F(Data, FilePath, PreprocessingCsv['KeyWords'])
                    _num_lines_csv = Data.shape[0]
                else:

                    # read the data from a specific protocole
                    Data = read_csv_protocole(ingestion_params["protocole_type"],
                                              ingestion_params["protocole_name"],
                                              FilePath, ingestion_params["csv_params"])

                    Data = F(Data, FilePath)
                    _num_lines_csv = Data.shape[0]
                    InsertToPostgre(Data,
                                TlbName=TlbName,
                                engine=engine,
                                schema=Schema,
                                DropTable=True, SizeChunck=SizeChunck)
                print("file {} was succesfully inserted".format(FilePath))
            else:
                Data = pd.read_csv(FilePath)
                _num_lines_csv = Data.shape[0]
                InsertToPostgre(Data, engine, TlbName, schema=Schema, DropTable=True, SizeChunck=SizeChunck)
                print("file {} was succesfully inserted".format(FilePath))
    except Exception as e:
        if logger is not None:
            logger.error(e, exc_info=True)
        else:
            print(e)

    if not _was_engine:
        engine.close()

    _outputs = [FilePath, _num_lines_csv]

    return _outputs

def CreateTable(engine, TlbName, schema, TableParameter, **kwargs):


    _keep_order = kwargs.get('keep_order', False)

    CreateQuery = """
        CREATE TABLE "{}"."{}" (


    """.format(schema, TlbName)

    if _keep_order:
        i = 1
        for var, keys in TableParameter.iteritems():
            if i == len(TableParameter):
                ""
                AddColumns = """

                    "{}" {} );

                    """.format(var, keys)

                CreateQuery = CreateQuery + AddColumns
            else:
                AddColumns = """

                    "{}" {} ,

                    """.format(var, keys)

                CreateQuery = CreateQuery + AddColumns
            i = i + 1
    else:
        i = 1
        for var, keys in sorted(TableParameter.iteritems()):
            if i == len(TableParameter):
                AddColumns = """
    
                    "{}" {} );
    
                    """.format(var, keys)

                CreateQuery = CreateQuery + AddColumns
            else:
                AddColumns = """
    
                    "{}" {} ,
    
                    """.format(var, keys)

                CreateQuery = CreateQuery + AddColumns
            i = i + 1

    engine.execute(CreateQuery)


def  TableCharacteristics(TlbName, schema, engine):

    Query = """ Select count(*) as "NumRows" from "{}"."{}" """.format(schema, TlbName)
    Data = pd.read_sql(Query, con=engine)

    Query = """ Select * from "{}"."{}" Limit 1 """.format(schema, TlbName)
    DataBis = pd.read_sql(Query, con=engine)

    return Data.NumRows[0], DataBis.shape[1]

def IsSchemaExist(engine, Schema):

    Query = """
            SELECT
            EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = '{}') as "Result"
            """.format(Schema)

    res = pd.read_sql(Query, con=engine)
    return res.Result[0]


def CreateSchema(engine, Schema):

    Query = """
            CREATE SCHEMA IF NOT EXISTS "{}";
            """.format(Schema)

    engine.execute(Query)

