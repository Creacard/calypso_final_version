from Creacard_Utils.SplitData import splitDataFrameIntoSmaller
from functools import partial
import multiprocessing
from Postgres_Toolsbox import DbTools as db
from Postgres_Toolsbox.DetectTypePostgres import CreateDictionnaryType
from sqlalchemy import MetaData, Table
from Creacard_Utils.read_with_protocole import *
import time
import datetime
from Postgres_Toolsbox.Ingestion import InsertTableIntoDatabase
from Postgres_Toolsbox.DbTools import insert_into_postgres_copyfrom
from creacard_connectors.database_connector import connect_to_database
from multiprocessing import Pool


def daily_card_status2(Data, filepath, **kwargs):

    logger = kwargs.get('logger', None)



    # Table parameter for the temporary table
    TableParameter = {}
    TableParameter["ActivationDate"] = "timestamp without time zone"
    TableParameter["Address1"] = "TEXT"
    TableParameter["Address2"] = "TEXT"
    TableParameter["ApplicationName"] = "VARCHAR (50)"
    TableParameter["AvailableBalance"] = "double precision"
    TableParameter["BirthDate"] = "timestamp without time zone"
    TableParameter["CardHolderID"] = "VARCHAR (50)"
    TableParameter["CardStatus"] = "VARCHAR (100)"
    TableParameter["City"] = "VARCHAR (100)"
    TableParameter["Country"] = "VARCHAR (50)"
    TableParameter["CreationDate"] = "timestamp without time zone"
    TableParameter["DistributorCode"] = "INTEGER"
    TableParameter["Email"] = "TEXT"
    TableParameter["ExpirationDate"] = "timestamp without time zone"
    TableParameter["FirstName"] = "TEXT"
    TableParameter["IBAN"] = "TEXT"
    TableParameter["IsExcludedAddress"] = "INTEGER"
    TableParameter["IsRenewal"] = "INTEGER"
    TableParameter["KYC_Status"] = "VARCHAR (50)"
    TableParameter["LastName"] = "TEXT"
    TableParameter["LastChangeDate"] = "timestamp without time zone"
    TableParameter["LastAddressDate"] = "timestamp without time zone"
    TableParameter["LastCustomerDate"] = "timestamp without time zone"
    TableParameter["NoMobile"] = "TEXT"
    TableParameter["PostCode"] = "VARCHAR (50)"
    TableParameter["Programme"] = "VARCHAR (50)"
    TableParameter["RenewalDate"] = "timestamp without time zone"
    TableParameter["UpdateDate"] = "timestamp without time zone"

    keepcol = ["CardHolderID", "Email", "FirstName",
               "LastName", "City", "Country", "Card Status", "DistributorCode",
               "ApplicationName", "Date of Birth", "IBAN",
               "CreatedDate", "UpdatedDate", "Address1", "Address2", "PostCode",
               "KYC Status", "expirydate", "AvailableBalance", "NoMobile",
               "Programme"]

    #### Step 1: Extract the data from the file and keep ony updated data
    # extract filedate
    FileName = filepath.split('/')[-1].replace(".csv", "")

    DateFile = pd.to_datetime(FileName.split("-")[1] + "-" + FileName.split("-")[2] + "-" + FileName.split("-")[3])

    # based on the file date, identify the appropriate names of columns
    if DateFile > pd.to_datetime('2019-03-12'):

        col_names = ["CardHolderID", "Cardnumber", "Email", "FirstName",
                     "LastName", "City", "Country", "Card Status", "DistributorCode",
                     "ApplicationName", "Date of Birth", "SortCodeAccNum", "IBAN",
                     "CreatedDate", "UpdatedDate", "Address1", "Address2", "PostCode",
                     "KYC Status", "expirydate", "AvailableBalance", "UDF2", "NoMobile",
                     "Programme", "VPVR"]

    elif DateFile < pd.to_datetime('2019-01-16'):

        col_names = ["CardHolderID", "Cardnumber", "Email", "FirstName",
                     "LastName", "City", "Country", "Card Status", "DistributorCode",
                     "ApplicationName", "Date of Birth", "SortCodeAccNum", "IBAN",
                     "CreatedDate", "UpdatedDate", "Address1", "Address2", "PostCode",
                     "KYC Status", "expirydate"]

    else:

        col_names = ["CardHolderID", "Cardnumber", "Email", "FirstName",
                     "LastName", "City", "Country", "Card Status", "DistributorCode",
                     "ApplicationName", "Date of Birth", "SortCodeAccNum", "IBAN",
                     "CreatedDate", "UpdatedDate", "Address1", "Address2", "PostCode",
                     "KYC Status", "expirydate", "AvailableBalance", "UDF2", "NoMobile",
                     "UDF3", "VPVR"]

    # add the names of columns to the dataframe
    Data.columns = col_names

    # store the missing columns
    missing_columns = list(set(keepcol).difference(col_names))

    if missing_columns:  # if the list is not add new columns to the dataframe
        for col in missing_columns:
            Data[col] = None

    # Store change values

    engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()
    query = """
           select distinct "CardHolderID","CardStatus","KYC_Status" from "CARD_STATUS"."STATUS_CARTES"
       """
    data_current = pd.read_sql(query, con=engine)

    data_current["CardHolderID"] = data_current["CardHolderID"].astype(str)
    data_current["KYC_Status"] = data_current["KYC_Status"].astype(str)
    data_current["CardStatus"] = data_current["CardStatus"].astype(str)

    #### Step 2: Transform the data

    # transform date columns to pd.datetime format in order to have a consistent format
    # of date over the database
    # Only transform updated date
    Data["UpdatedDate"] = pd.to_datetime(Data["UpdatedDate"], format="%b %d %Y %I:%M%p", errors='coerce')
    Data["CreatedDate"] = pd.to_datetime(Data["CreatedDate"], format="%b %d %Y %I:%M%p", errors='coerce')
    Data["Date of Birth"] = pd.to_datetime(Data["Date of Birth"], format="%b %d %Y %I:%M%p", errors='coerce')

    # transform expirydate
    Data["expirydate"] = Data["expirydate"].astype(str)
    Data["expirydate"] = "20" + Data["expirydate"].str[0:2] + "-" + Data["expirydate"].str[2:] + "-01"
    Data["expirydate"] = pd.to_datetime(Data["expirydate"], format='%Y-%m-%d', errors='coerce')

    Data = Data[keepcol]

    # condition remove address
    AddressToRemove = ["77 OXFORD STREET LONDON", "17 RUE D ORLEANS", "TSA 51760", "77 Oxford Street London"
        , "36 CARNABY STREET",
                       "36 CARNABY STREET LONDON", "36 CARNABY STREET LONDON", "ADDRESS", "17 RUE D ORLEANS PARIS"
        , "CreaCard Espana S L  Paseo de Gracia 59",
                       "36 Carnaby Street London", "CREACARD SA Pl  Marcel Broodthaers 8 Box 5",
                       "17 Rue D Orleans Paris",
                       "CREACARD ESPANA S L  PASEO DE GRACIA 59", "CreaCard 17 rue d Orleans"
        , "CREACARD SA PL  MARCEL BROODTHAERS 8 BOX 75",
                       "CREACARD SA PL  MARCEL BROODTHAERS 8 BOX 75", "36 Carnaby Street", "77 OXFORD STREET"]

    Data["IsExcludedAddress"] = (Data.Address1.isin(AddressToRemove)).astype(int)

    Data["ActivationDate"] = pd.NaT
    Data["IsRenewal"] = 0
    Data["RenewalDate"] = pd.NaT
    Data["LastChangeDate"] = pd.NaT
    Data["LastAddressDate"] = pd.NaT
    Data["LastCustomerDate"] = pd.NaT

    Data = Data[sorted(Data.columns)]

    colnames = ["ActivationDate", "Address1", "Address2", "ApplicationName", "AvailableBalance",
                "CardStatus", "CardHolderID", "City", "Country", "CreationDate",
                "BirthDate", "DistributorCode", "Email", "FirstName", "IBAN", "IsExcludedAddress",
                "IsRenewal", "KYC_Status", "LastAddressDate", "LastChangeDate", "LastCustomerDate",
                "LastName", "NoMobile", "PostCode",
                "Programme", "RenewalDate", "UpdateDate", "ExpirationDate"]

    Data.columns = colnames

    Data = Data[sorted(Data.columns)]

    Data.loc[(Data["KYC_Status"] == '0') | (Data["KYC_Status"] == '0.0') | (
            Data["KYC_Status"] == 0), "KYC_Status"] = 'Anonyme'
    Data.loc[(Data["KYC_Status"] == '1') | (Data["KYC_Status"] == '1.0') | (
            Data["KYC_Status"] == 1), "KYC_Status"] = 'SDD'
    Data.loc[(Data["KYC_Status"] == '2') | (Data["KYC_Status"] == '2.0') | (
            Data["KYC_Status"] == 2), "KYC_Status"] = 'KYC'
    Data.loc[(Data["KYC_Status"] == '3') | (Data["KYC_Status"] == '3.0') | (
            Data["KYC_Status"] == 3), "KYC_Status"] = 'KYC LITE'

    Data["DistributorCode"] = Data["DistributorCode"].fillna(-1)
    Data["DistributorCode"] = Data["DistributorCode"].astype(int)

    Data["CardHolderID"] = Data["CardHolderID"].astype(str)
    Data["KYC_Status"] = Data["KYC_Status"].astype(str)
    Data["CardStatus"] = Data["CardStatus"].astype(str)

    Data.loc[Data["DistributorCode"].isin(["203", "914", "915"]), "IsRenewal"] = 1

    Data = Data[sorted(Data.columns)]

    # Delete leading "00" at the start of string.

    Data["NoMobile"] = Data["NoMobile"].str.replace("^00", "", regex=True)

    # replace .0 at the end$

    Data["NoMobile"] = Data["NoMobile"].str.replace("\.0$", "", regex=True)

    # delete only literal '|' from string

    Data["NoMobile"] = Data["NoMobile"].str.replace("\|", "", regex=True)

    # Step 1: Identify

    data_new = Data[["CardHolderID", "CardStatus", "KYC_Status"]]
    outer_join = data_current.merge(data_new, how='outer', indicator=True)
    outer_join = outer_join[outer_join["_merge"] == "right_only"]

    # set 2 : identify new cardholder ID
    new_card_holder_id = set(outer_join["CardHolderID"].unique()).difference(data_current["CardHolderID"].unique())

    ### set 3 : insert old values into changes table
    data_to_change = data_current[data_current["CardHolderID"].isin(
        set(outer_join.loc[~outer_join["CardHolderID"].isin(new_card_holder_id), "CardHolderID"]))]

    FileName = filepath.split('/')[-1].replace(".csv", "")
    DateFile = pd.to_datetime(
        FileName.split("-")[1] + "-" + FileName.split("-")[2] + "-" + FileName.split("-")[3]) - datetime.timedelta(
        days=1)

    data_to_change["dt_change"] = DateFile

    InsertTableIntoDatabase(data_to_change,
                            "CHANGE_STATUS_CARTES",
                            "CARD_STATUS",
                            "Postgres",
                            "Creacard_Calypso",
                            DropTable=False)

    engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()

    TlbName = "STATUS_CARTES"
    schema = "CARD_STATUS"
    database_type = "Postgres"
    database_name = "Creacard_Calypso"

    query_delete = """
    
    delete from "CARD_STATUS"."STATUS_CARTES"
    
    """
    tic = time.time()
    engine.execute(query_delete)
    print("delete took the data {} seconds".format(time.time() - tic))

    engine.close()

    data = splitDataFrameIntoSmaller(Data, chunkSize=100000)

    num_process = int(multiprocessing.cpu_count() / 4)
    tic = time.time()
    pool = Pool(num_process)
    pool.map(partial(insert_into_postgres_copyfrom,
                     database_type=database_type,
                     database_name=database_name,
                     schema=schema,
                     TlbName=TlbName), data)

    pool.close()
    pool.terminate()
    pool.join()
    toc = time.time() - tic

    print("ingestion was done in {} seconds ".format(toc))

    ### update the LastChangeDate columns (KYC & card status)

    con_postgres = connect_to_database(database_type, database_name).CreateEngine()

    query = """

       UPDATE "CARD_STATUS"."STATUS_CARTES"
       SET "LastChangeDate" = T1."max_date"
       FROM( 
       SELECT max("dt_change") as "max_date", "CardHolderID"
       FROM "CARD_STATUS"."CHANGE_STATUS_CARTES"
       GROUP BY "CardHolderID"
       ) as T1
       WHERE "CARD_STATUS"."STATUS_CARTES"."CardHolderID" = T1."CardHolderID"

       """

    con_postgres.execute(query)
    con_postgres.close()

    con_postgres = connect_to_database(database_type, database_name).CreateEngine()

    query = """

       update "CARD_STATUS"."STATUS_CARTES" as T1
       SET "ActivationDate" = "ActivationTime"
       FROM "CARD_STATUS"."ACTIVATION_REPORT" as T2
       WHERE 
       T1."CardHolderID" = T2."CardHolderID" and 
       "ActivationDate" is null 

       """

    con_postgres.execute(query)
    con_postgres.close()
