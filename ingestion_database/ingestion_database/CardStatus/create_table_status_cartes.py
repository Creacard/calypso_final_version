import pandas as pd
import time
import datetime

from Postgres_Toolsbox.Ingestion import InsertTableIntoDatabase
from creacard_connectors.database_connector import connect_to_database
from Creacard_Utils.import_credentials import credentials_extractor

engine = connect_to_database("Postgres","Creacard_Calypso").CreateEngine()

Data = pd.read_csv("/Users/justinvalet/CreacardProject/Data/CardStatus2-2019-07-01.csv",
                   sep=",",
                   header=None,
                   encoding='iso-8859-1')


col = ["CardHolderID", "Cardnumber", "Email", "FirstName",
 "LastName", "City", "Country", "Card Status", "DistributorCode",
 "ApplicationName", "Date of Birth", "SortCodeAccNum", "IBAN",
 "CreatedDate", "UpdatedDate", "Address1", "Address2", "PostCode",
 "KYC Status", "expirydate", "AvailableBalance","UDF2", "NoMobile",
 "Programme", "VPVR"]

Data.columns = col

Data["UpdatedDate"] = pd.to_datetime(Data["UpdatedDate"], format="%b %d %Y %I:%M%p", errors='coerce')
Data["CreatedDate"] = pd.to_datetime(Data["CreatedDate"], format="%b %d %Y %I:%M%p", errors='coerce')
Data["Date of Birth"] = pd.to_datetime(Data["Date of Birth"], format="%b %d %Y %I:%M%p", errors='coerce')

# transform expirydate
Data["expirydate"] = Data["expirydate"].astype(str)
Data["expirydate"] = "20" + Data["expirydate"].str[0:2] + "-" + Data["expirydate"].str[2:] + "-01"
Data["expirydate"] = pd.to_datetime(Data["expirydate"], format='%Y-%m-%d', errors='coerce')


# condition remove address
AdressToRemove = ["77 OXFORD STREET LONDON","17 RUE D ORLEANS","TSA 51760","77 Oxford Street London","36 CARNABY STREET",
"36 CARNABY STREET LONDON","36 CARNABY STREET LONDON","ADDRESS","17 RUE D ORLEANS PARIS","CreaCard Espana S L  Paseo de Gracia 59",
 "36 Carnaby Street London","CREACARD SA Pl  Marcel Broodthaers 8 Box 5","17 Rue D Orleans Paris",
 "CREACARD ESPANA S L  PASEO DE GRACIA 59","CreaCard 17 rue d Orleans","CREACARD SA PL  MARCEL BROODTHAERS 8 BOX 75",
 "CREACARD SA PL  MARCEL BROODTHAERS 8 BOX 75","36 Carnaby Street","77 OXFORD STREET"]

Data = Data[~Data.Address1.isin(AdressToRemove)]

for add in AdressToRemove:

    if Data["Address1"].str.contains(str(add), regex=True).sum() > 0:
        ii = ~Data["Address1"].str.contains(str(add), regex=True).fillna(False)
        Data = Data[ii]


keepcol = ["CardHolderID", "Email", "FirstName",
 "LastName", "City", "Country", "Card Status", "DistributorCode",
 "ApplicationName", "Date of Birth", "IBAN",
 "CreatedDate", "UpdatedDate", "Address1", "Address2", "PostCode",
 "KYC Status", "expirydate", "AvailableBalance", "NoMobile",
 "Programme"]

Data = Data[keepcol]

Data["ActivationDate"] = pd.NaT
Data["IsRenewal"] = 0
Data["RenewalDate"] = pd.NaT

Data = Data[sorted(Data.columns)]

colnames = ["ActivationDate", "Address1", "Address2", "ApplicationName","AvailableBalance",
            "CardStatus", "CardHolderID", "City", "Country", "CreationDate",
            "BirthDate", "DistributorCode", "Email", "FirstName", "IBAN",
            "IsRenewal", "KYC_Status", "LastName", "NoMobile", "PostCode",
            "Programme", "RenewalDate", "UpdateDate", "ExpirationDate"]

Data.columns = colnames
Data = Data[sorted(Data.columns)]

Data.loc[Data.loc[:,"KYC_Status"] == 0, "KYC_Status"] = 'Anonyme'
Data.loc[Data.loc[:,"KYC_Status"] == 1, "KYC_Status"] = 'SDD'
Data.loc[Data.loc[:,"KYC_Status"] == 2, "KYC_Status"] = 'KYC'
Data.loc[Data.loc[:,"KYC_Status"] == 3, "KYC_Status"] = 'KYC LITE'

Data["DistributorCode"] = Data["DistributorCode"].fillna(-1)
Data["DistributorCode"] = Data["DistributorCode"].astype(int)



TableParameter = {}
TableParameter["ActivationDate"]     = "timestamp without time zone"
TableParameter["Address1"]           = "TEXT"
TableParameter["Address2"]           = "TEXT"
TableParameter["ApplicationName"]    = "VARCHAR (50)"
TableParameter["AvailableBalance"]   = "double precision"
TableParameter["BirthDate"]          = "timestamp without time zone"
TableParameter["CardHolderID"]       = "VARCHAR (50)"
TableParameter["CardStatus"]         = "VARCHAR (100)"
TableParameter["City"]               = "VARCHAR (100)"
TableParameter["Country"]            = "VARCHAR (50)"
TableParameter["CreationDate"]       = "timestamp without time zone"
TableParameter["DistributorCode"]    = "INTEGER"
TableParameter["Email"]              = "TEXT"
TableParameter["ExpirationDate"]     = "timestamp without time zone"
TableParameter["FirstName"]          = "TEXT"
TableParameter["IBAN"]               = "TEXT"
TableParameter["IsRenewal"]          = "INTEGER"
TableParameter["KYC_Status"]         = "VARCHAR (50)"
TableParameter["LastName"]           = "TEXT"
TableParameter["NoMobile"]           = "TEXT"
TableParameter["PostCode"]           = "VARCHAR (50)"
TableParameter["Programme"]          = "VARCHAR (50)"
TableParameter["RenewalDate"]        = "timestamp without time zone"
TableParameter["UpdateDate"]         = "timestamp without time zone"


database_type = "Postgres"
database_name = "Creacard_Calypso"

tic = time.time()
InsertTableIntoDatabase(Data,
                        "STATUS_CARTES",
                        "CARD_STATUS",
                        database_type, database_name,
                        DropTable=True,
                        TableDict=TableParameter,
                        InsertInParrell=True,
                        NumWorkers=4,
                        SizeChunck=10000)
print("Insertion is done in {} seconds".format(time.time() - tic))
