import pandas as pd
from creacard_connectors.database_connector import connect_to_database
from Postgres_Toolsbox.Ingestion import InsertTableIntoDatabase
import datetime

def daily_card_status2(Data, filepath, database_type, database_name):
    #### constant variables

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
    TableParameter["NoMobile"] = "TEXT"
    TableParameter["PostCode"] = "VARCHAR (50)"
    TableParameter["Programme"] = "VARCHAR (50)"
    TableParameter["RenewalDate"] = "timestamp without time zone"
    TableParameter["UpdateBalanceDate"] = "timestamp without time zone"
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

    # keep track of available balance
    tmp_available_balance = Data[["CardHolderID", "AvailableBalance"]]
    tmp_available_balance["UpdateBalanceDate"] = datetime.datetime.now()

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

    Data = Data[sorted(Data.columns)]

    colnames = ["ActivationDate", "Address1", "Address2", "ApplicationName", "AvailableBalance",
                "CardStatus", "CardHolderID", "City", "Country", "CreationDate",
                "BirthDate", "DistributorCode", "Email", "FirstName", "IBAN", "IsExcludedAddress",
                "IsRenewal", "KYC_Status", "LastName", "NoMobile", "PostCode",
                "Programme", "RenewalDate", "UpdateDate", "ExpirationDate"]

    Data.columns = colnames

    Data["UpdateBalanceDate"] = datetime.datetime.now()

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
    DateFile = pd.to_datetime(FileName.split("-")[1] + "-" + FileName.split("-")[2] + "-" + FileName.split("-")[3]) - datetime.timedelta(days=1)

    data_to_change["dt_change"] = DateFile

    InsertTableIntoDatabase(data_to_change,
                            "CHANGE_STATUS_CARTES",
                            "CARD_STATUS",
                            "Postgres",
                            "Creacard_Calypso",
                            DropTable=False)

    # find new cardholder ID update + card holder ID change

    DateFile = pd.to_datetime(FileName.split("-")[1] + "-" + FileName.split("-")[2] + "-" + FileName.split("-")[3])
    update_set = Data[(Data["UpdateDate"] >= DateFile) & (Data["UpdateDate"] < DateFile + pd.Timedelta(days=1))]
    update_set = update_set.reset_index(drop=True)

    import numpy as np
    kk = pd.DataFrame(
        np.concatenate((outer_join["CardHolderID"].unique(), update_set["CardHolderID"].unique()), axis=0))
    kk.columns = ["CardHolderID"]
    kk = kk[kk["CardHolderID"] != '0']

    to_update = Data[Data["CardHolderID"].isin(kk["CardHolderID"].unique())]

    #### Step 3: Load these data into a temporary table

    con_postgres = connect_to_database(database_type, database_name).CreateEngine()
    query = """
    DROP TABLE IF EXISTS "TMP_UPDATE"."TMP_STATUS_CARTES"
    """
    con_postgres.execute(query)
    con_postgres.close()

    InsertTableIntoDatabase(to_update,
                            "TMP_STATUS_CARTES",
                            "TMP_UPDATE",
                            database_type, database_name,
                            DropTable=True,
                            TableDict=TableParameter,
                            SizeChunck=10000)

    #### Step 5: Update new values

    query_delete = """

       DELETE FROM "CARD_STATUS"."STATUS_CARTES"
       USING "TMP_UPDATE"."TMP_STATUS_CARTES"
       WHERE 
       "CARD_STATUS"."STATUS_CARTES"."CardHolderID" = "TMP_UPDATE"."TMP_STATUS_CARTES"."CardHolderID"

       """

    con_postgres = connect_to_database(database_type, database_name).CreateEngine()
    con_postgres.execute(query_delete)
    con_postgres.close()


    query_update = """
    
    UPDATE "TMP_UPDATE"."TMP_STATUS_CARTES" 
    SET "IsRenewal" = CASE WHEN "DistributorCode" in ('203','914','915') then 1
    else 0
    end 
    """

    con_postgres = connect_to_database(database_type, database_name).CreateEngine()
    con_postgres.execute(query_update)
    con_postgres.close()


    query = """

           INSERT INTO "CARD_STATUS"."STATUS_CARTES"
           SELECT *
           FROM "TMP_UPDATE"."TMP_STATUS_CARTES" 

          """

    con_postgres = connect_to_database(database_type, database_name).CreateEngine()
    con_postgres.execute(query)
    con_postgres.close()

    # drop the temporary table
    con_postgres = connect_to_database(database_type, database_name).CreateEngine()
    query = """
          DROP TABLE IF EXISTS "TMP_UPDATE"."TMP_STATUS_CARTES"
          """
    con_postgres.execute(query)
    con_postgres.close()

    #### Step 6: Update available balance for all CHID

    tlb_param_balance = dict()
    tlb_param_balance["AvailableBalance"] = "double precision"
    tlb_param_balance["CardHolderID"] = "VARCHAR (50)"
    tlb_param_balance["UpdateBalanceDate"] = "timestamp without time zone"

    con_postgres = connect_to_database(database_type, database_name).CreateEngine()
    query = """
       DROP TABLE IF EXISTS "TMP_UPDATE"."TMP_AVAILABLE_BALANCE"
       """
    con_postgres.execute(query)
    con_postgres.close()

    InsertTableIntoDatabase(tmp_available_balance,
                            "TMP_AVAILABLE_BALANCE",
                            "TMP_UPDATE",
                            database_type, database_name,
                            DropTable=True,
                            TableDict=tlb_param_balance,
                            SizeChunck=10000)

    con_postgres = connect_to_database(database_type, database_name).CreateEngine()

    query_balance = """

    UPDATE "CARD_STATUS"."STATUS_CARTES"
    SET "AvailableBalance" = T1."AvailableBalance",
    "UpdateBalanceDate" = T1."UpdateBalanceDate"
    from "TMP_UPDATE"."TMP_AVAILABLE_BALANCE" as T1
    WHERE 
    "CARD_STATUS"."STATUS_CARTES"."CardHolderID" = T1."CardHolderID"

    """
    con_postgres.execute(query_balance)
    con_postgres.close()

    con_postgres = connect_to_database(database_type, database_name).CreateEngine()
    query = """
       DROP TABLE IF EXISTS "TMP_UPDATE"."TMP_AVAILABLE_BALANCE"
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
