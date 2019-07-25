import pandas as pd
from Postgres_Toolsbox.DbTools import InsertToPostgre, CreateSchema
from creacard_connectors.database_connector import connect_to_database
import os
import sys
from Postgres_Toolsbox.Ingestion import InsertTableIntoDatabase

def create_update_dictionnaries_categorisation(database_type,database_name):



    engine = connect_to_database(database_type, database_name).CreateEngine()

    CreateSchema(engine, "REFERENTIEL")

    # Conf files location
    if sys.platform == "win32":
        Folder = os.path.expanduser('~') + "\\conf_python\\categorisation_univers\\"
    else:
        Folder = os.environ['HOME'] + "/conf_python/categorisation_univers/"

    # Referentials table
    FileDescription = "description_univers.xlsx"

    DataDescritpion = pd.read_excel(Folder + FileDescription)

    TableParameter = {}
    TableParameter["UNIVERS_DATABASE"] = "VARCHAR (50)"
    TableParameter["SOUS_UNIVERS_DATABASE"] = "VARCHAR (100)"
    TableParameter["UNIVERS"] = "VARCHAR (50)"
    TableParameter["SOUS_UNIVERS"] = "VARCHAR (100)"
    TableParameter["DESCRIPTION"] = "TEXT"

    InsertTableIntoDatabase(DataDescritpion,
                            "UNIVERS_DESCRIPTION",
                            "REFERENTIEL",
                            database_type, database_name,
                            DropTable=True,
                            TableDict=TableParameter)

    del DataDescritpion
    del FileDescription

    # link between MCC and MCC_CODE
    FileMCC = "mcc_code_link.xlsx"

    DataFileMCC = pd.read_excel(Folder + FileMCC, dtype={'MCC_CODE': str, 'MCC': object})

    TableParameter = {}
    TableParameter["MCC_CODE"] = "VARCHAR (20)"
    TableParameter["MCC"] = "TEXT"

    InsertTableIntoDatabase(DataFileMCC,
                            "MCC_CODE_LINK",
                            "REFERENTIEL",
                            database_type, database_name,
                            DropTable=True,
                            TableDict=TableParameter)

    del FileMCC
    del DataFileMCC

    # Ingest MCC_CATEGORIES
    FileMCCCat = "mcc_categories.xlsx"

    DataMCCCat = pd.read_excel(Folder + FileMCCCat)

    TableParameter = {}
    TableParameter["MCC_NAME"] = "TEXT"
    TableParameter["SOUS_UNIVERS"] = "VARCHAR (100)"
    TableParameter["UNIVERS"] = "VARCHAR (50)"
    TableParameter["MCC_CODE"] = "VARCHAR (20)"
    TableParameter["NOTE"] = "INTEGER"

    InsertTableIntoDatabase(DataMCCCat,
                            "MCC_CATEGORIES",
                            "REFERENTIEL",
                            database_type, database_name,
                            DropTable=True,
                            TableDict=TableParameter)

    del FileMCCCat
    del DataMCCCat

    # Regex exclusion
    FileRegexExclu = "regex_merchant.xlsx"

    Data1 = pd.read_excel(Folder + FileRegexExclu, sheet_name='Regex exclu')
    Data1 = Data1[~Data1.UNIVERS.isna()]
    TableParameter = {}
    TableParameter["MCC"] = "TEXT"
    TableParameter["Regex"] = "TEXT"
    TableParameter["UNIVERS"] = "VARCHAR (50)"
    TableParameter["SOUS_UNIVERS"] = "VARCHAR (100)"
    TableParameter["NEW_REGEX"] = "TEXT"
    TableParameter["MCC_CODE"] = "VARCHAR (20)"

    InsertTableIntoDatabase(Data1,
                            "REGEX_EXCLUDED",
                            "REFERENTIEL",
                            database_type, database_name,
                            DropTable=True,
                            TableDict=TableParameter)

    del FileRegexExclu
    del Data1

    # Update MCC code for Regex excluded

    query = """
    UPDATE "REFERENTIEL"."REGEX_EXCLUDED"
    set "MCC_CODE" = T2."MCC_CODE"
    FROM "REFERENTIEL"."MCC_CATEGORIES" AS T2
    where "MCC" = T2."MCC_NAME"
    """

    engine.execute(query)

    # Regex inclusion
    FileRegexExclu = "regex_ajout.xlsx"

    Data1 = pd.read_excel(Folder + FileRegexExclu, sheet_name='Regex ajout')
    Data1 = Data1[~Data1.UNIVERS.isna()]
    TableParameter = {}
    TableParameter["Regex"] = "TEXT"
    TableParameter["UNIVERS"] = "VARCHAR (50)"
    TableParameter["SOUS_UNIVERS"] = "VARCHAR (100)"
    TableParameter["NEW_REGEX"] = "TEXT"

    InsertTableIntoDatabase(Data1,
                            "REGEX_INCLUDED",
                            "REFERENTIEL",
                            database_type, database_name,
                            DropTable=True,
                            TableDict=TableParameter)

    del FileRegexExclu
    del Data1

    engine.close()
