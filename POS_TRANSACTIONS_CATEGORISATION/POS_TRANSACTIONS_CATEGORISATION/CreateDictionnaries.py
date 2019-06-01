import pandas as pd
from BDDTools.DbTools import InsterToPostgre,CreateSchema
from BDDTools.DataBaseEngine import ConnectToPostgres
import os
import sys

def create_update_dictionnaries_categorisation(credentials):


    PostgresConnect = ConnectToPostgres(credentials)
    engine = PostgresConnect.CreateEngine()

    CreateSchema(engine, "REFERENTIEL")

    # Conf files location
    if sys.platform == "win32":
        Folder = os.path.expanduser('~') + "\\conf_python\\categorisation_univers\\"
    else:
        Folder = os.environ['HOME'] + "/conf_python/categorisation_univers/"

    # Referentials table
    FileDescription = "description_univers.xlsx"

    DataDescritpion = pd.read_excel(Folder+FileDescription)

    TableParameter = {}
    TableParameter["UNIVERS_DATABASE"]      = "VARCHAR (50)"
    TableParameter["SOUS_UNIVERS_DATABASE"] = "VARCHAR (100)"
    TableParameter["UNIVERS"]               = "VARCHAR (50)"
    TableParameter["SOUS_UNIVERS"]          = "VARCHAR (100)"
    TableParameter["DESCRIPTION"]           = "TEXT"

    InsterToPostgre(DataDescritpion, engine, "UNIVERS_DESCRIPTION", "REFERENTIEL", DropTable=True, TableDict=TableParameter)

    del DataDescritpion
    del FileDescription

    # link between MCC and MCC_CODE
    FileMCC = "mcc_code_link.xlsx"

    DataFileMCC = pd.read_excel(Folder+FileMCC, dtype={'MCC_CODE': str, 'MCC': object})

    TableParameter = {}
    TableParameter["MCC_CODE"] = "VARCHAR (20)"
    TableParameter["MCC"]      = "TEXT"

    InsterToPostgre(DataFileMCC, engine, "MCC_CODE_LINK", "REFERENTIEL", DropTable=True, TableDict=TableParameter)
    del FileMCC
    del DataFileMCC


    # Ingest MCC_CATEGORIES
    FileMCCCat = "mcc_categories.xlsx"

    DataMCCCat = pd.read_excel(Folder+FileMCCCat)

    TableParameter = {}
    TableParameter["MCC_NAME"]              = "TEXT"
    TableParameter["SOUS_UNIVERS"]          = "VARCHAR (100)"
    TableParameter["UNIVERS"]               = "VARCHAR (50)"
    TableParameter["MCC_CODE"]              = "VARCHAR (20)"
    TableParameter["NOTE"]                  = "INTEGER"

    InsterToPostgre(DataMCCCat,engine, "MCC_CATEGORIES", "REFERENTIEL", DropTable=True, TableDict=TableParameter)
    del FileMCCCat
    del DataMCCCat


    # Regex exclusion
    FileRegexExclu = "regex_merchant.xlsx"

    Data1 = pd.read_excel(Folder+FileRegexExclu, sheet_name='Regex exclu')
    Data1 = Data1[~Data1.UNIVERS.isna()]
    TableParameter = {}
    TableParameter["MCC"]              = "TEXT"
    TableParameter["Regex"]            = "TEXT"
    TableParameter["UNIVERS"]          = "VARCHAR (50)"
    TableParameter["SOUS_UNIVERS"]     = "VARCHAR (100)"
    TableParameter["NEW_REGEX"]        = "TEXT"
    TableParameter["MCC_CODE"]         = "VARCHAR (20)"

    InsterToPostgre(Data1,engine, "REGEX_EXCLUDED", "REFERENTIEL", DropTable=True, TableDict=TableParameter)
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

    Data1 = pd.read_excel(Folder+FileRegexExclu, sheet_name='Regex ajout')
    Data1 = Data1[~Data1.UNIVERS.isna()]
    TableParameter = {}
    TableParameter["Regex"]            = "TEXT"
    TableParameter["UNIVERS"]          = "VARCHAR (50)"
    TableParameter["SOUS_UNIVERS"]     = "VARCHAR (100)"
    TableParameter["NEW_REGEX"]        = "TEXT"

    InsterToPostgre(Data1, engine, "REGEX_INCLUDED", "REFERENTIEL", DropTable=True, TableDict=TableParameter)
    del FileRegexExclu
    del Data1

    engine.close()






