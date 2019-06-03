import pandas as pd
import time
from BDDTools.Ingestion import InsertTableIntoDatabase


def ingestion_card_status(engine, FilePath):


    ColToKeep = ["CardHolderID", "Programme", "MobileNo", "Email", "FirstName",
           "LastName", "Card Status", "Distributor", "Distributor Code",
           "Application Name", "Date of birth", "UpdatedDate", "CountryCode", "PostCode",
           "DepartementFR","Age_Client", "Commune_CodeInsee", "Address1", "KYC Status", "ExpirationDate", "ActivationDate"]

    Data = pd.read_csv(FilePath, sep=";", usecols=ColToKeep)

    TableParameter = {}
    TableParameter["ActivationDate"]    = "timestamp without time zone"
    TableParameter["Address1"]          = "TEXT"
    TableParameter["Age_Client"]        = "INTEGER"
    TableParameter["ApplicationName"]   = "VARCHAR (50)"
    TableParameter["CardHolderID"]      = "VARCHAR (50)"
    TableParameter["CardStatus"]        = "VARCHAR (100)"
    TableParameter["Commune_CodeInsee"] = "VARCHAR (50)"
    TableParameter["CountryCode"]       = "VARCHAR (100)"
    TableParameter["Dateofbirth"]       = "timestamp without time zone"
    TableParameter["DepartementFR"]     = "VARCHAR (30)"
    TableParameter["Distributor"]       = "VARCHAR (100)"
    TableParameter["DistributorCode"]   = "VARCHAR (50)"
    TableParameter["Email"]             = "VARCHAR (100)"
    TableParameter["ExpirationDate"]    = "timestamp without time zone"
    TableParameter["FirstName"]         = "VARCHAR (100)"
    TableParameter["KYCStatus"]         = "VARCHAR (30)"
    TableParameter["LastName"]          = "VARCHAR (100)"
    TableParameter["MobileNo"]          = "VARCHAR (100)"
    TableParameter["PostCode"]          = "VARCHAR (50)"
    TableParameter["Programme"]         = "VARCHAR (50)"
    TableParameter["UpdatedDate"]       = "timestamp without time zone"


    # Replace the symbol "'" in these two columns CardHolderID & MobileNo
    Data["CardHolderID"] = Data["CardHolderID"].str.replace("'", "")
    Data["MobileNo"]     = Data["MobileNo"].str.replace("'", "")

    # Modify the date format
    Data["Date of birth"] = pd.to_datetime(Data["Date of birth"], format='%d/%m/%Y',errors='coerce')
    Data["ActivationDate"]   = pd.to_datetime(Data["ActivationDate"], format='%d/%m/%Y', errors='coerce')

    # rename columns
    Data.columns = Data.columns.str.replace(" ", "")
    Data = Data.reindex_axis(sorted(Data.columns), axis=1)

    tic = time.time()
    InsertTableIntoDatabase(Data, engine=engine, TlbName='CARDINFO', Schema='CUSTOMERS', TableDict=TableParameter,
                            DropTable=True, NumWorkers=4, SizeChunck=10000)
    print("Insertion is done in {} seconds".format(time.time() - tic))

