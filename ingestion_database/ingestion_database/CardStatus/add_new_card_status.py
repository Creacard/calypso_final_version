import pandas as pd
from creacard_connectors.database_connector import connect_to_database
from Postgres_Toolsbox.Ingestion import InsertTableIntoDatabase


def daily_card_status2(Data, filepath, database_type, database_name):

    #### constant variables

    # Table parameter for the temporary table
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
    TableParameter["IsExcludedAddress"]  = "INTEGER"
    TableParameter["IsRenewal"]          = "INTEGER"
    TableParameter["KYC_Status"]         = "VARCHAR (50)"
    TableParameter["LastName"]           = "TEXT"
    TableParameter["NoMobile"]           = "TEXT"
    TableParameter["PostCode"]           = "VARCHAR (50)"
    TableParameter["Programme"]          = "VARCHAR (50)"
    TableParameter["RenewalDate"]        = "timestamp without time zone"
    TableParameter["UpdateDate"]         = "timestamp without time zone"

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
                     "KYC Status", "expirydate", "AvailableBalance", "UDF2" ,"NoMobile",
                     "UDF3", "VPVR"]

    # add the names of columns to the dataframe
    Data.columns = col_names

    # store the missing columns
    missing_columns = list(set(keepcol).difference(col_names))

    if missing_columns: # if the list is not add new columns to the dataframe
        for col in missing_columns:
            Data[col] = None


    # Only transform updated date
    Data["UpdatedDate"] = pd.to_datetime(Data["UpdatedDate"], format="%b %d %Y %I:%M%p", errors='coerce')

    # just keep cardholderID that ware updated
    Data = Data[Data["UpdatedDate"] >= DateFile]
    Data = Data.reset_index(drop=True)


    if Data.empty:
        print("no new data")
        # change the status of ingestion
    else:

        #### Step 2: Transform the data

        # transform date columns to pd.datetime format in order to have a consistent format
        # of date over the database
        Data["CreatedDate"] = pd.to_datetime(Data["CreatedDate"], format="%b %d %Y %I:%M%p", errors='coerce')
        Data["Date of Birth"] = pd.to_datetime(Data["Date of Birth"], format="%b %d %Y %I:%M%p", errors='coerce')

        # transform expirydate
        Data["expirydate"] = Data["expirydate"].astype(str)
        Data["expirydate"] = "20" + Data["expirydate"].str[0:2] + "-" + Data["expirydate"].str[2:] + "-01"
        Data["expirydate"] = pd.to_datetime(Data["expirydate"], format='%Y-%m-%d', errors='coerce')

        Data = Data[keepcol]


        # condition remove address
        AddressToRemove = ["77 OXFORD STREET LONDON" ,"17 RUE D ORLEANS" ,"TSA 51760" ,"77 Oxford Street London"
                           ,"36 CARNABY STREET",
                           "36 CARNABY STREET LONDON" ,"36 CARNABY STREET LONDON" ,"ADDRESS" ,"17 RUE D ORLEANS PARIS"
                           ,"CreaCard Espana S L  Paseo de Gracia 59",
                           "36 Carnaby Street London" ,"CREACARD SA Pl  Marcel Broodthaers 8 Box 5" ,"17 Rue D Orleans Paris",
                           "CREACARD ESPANA S L  PASEO DE GRACIA 59" ,"CreaCard 17 rue d Orleans"
                           ,"CREACARD SA PL  MARCEL BROODTHAERS 8 BOX 75",
                           "CREACARD SA PL  MARCEL BROODTHAERS 8 BOX 75" ,"36 Carnaby Street" ,"77 OXFORD STREET"]

        Data["IsExcludedAddress"] = (Data.Address1.isin(AddressToRemove)).astype(int)

        Data["ActivationDate"] = pd.NaT
        Data["IsRenewal"] = 0
        Data["RenewalDate"] = pd.NaT

        Data = Data[sorted(Data.columns)]

        colnames = ["ActivationDate", "Address1", "Address2", "ApplicationName" ,"AvailableBalance",
                    "CardStatus", "CardHolderID", "City", "Country", "CreationDate",
                    "BirthDate", "DistributorCode", "Email", "FirstName", "IBAN" ,"IsExcludedAddress",
                    "IsRenewal", "KYC_Status", "LastName", "NoMobile", "PostCode",
                    "Programme", "RenewalDate", "UpdateDate", "ExpirationDate"]

        Data.columns = colnames
        Data = Data[sorted(Data.columns)]

        Data.loc[Data.loc[: ,"KYC_Status"] == 0, "KYC_Status"] = 'Anonyme'
        Data.loc[Data.loc[: ,"KYC_Status"] == 1, "KYC_Status"] = 'SDD'
        Data.loc[Data.loc[: ,"KYC_Status"] == 2, "KYC_Status"] = 'KYC'
        Data.loc[Data.loc[: ,"KYC_Status"] == 3, "KYC_Status"] = 'KYC LITE'

        Data["DistributorCode"] = Data["DistributorCode"].fillna(-1)
        Data["DistributorCode"] = Data["DistributorCode"].astype(int)




        #### Step 3: Load these data into a temporary table

        con_postgres = connect_to_database(database_type, database_name).CreateEngine()
        query = """
        DROP TABLE IF EXISTS "TMP_UPDATE"."TMP_STATUS_CARTES"
        """
        con_postgres.execute(query)
        con_postgres.close()

        InsertTableIntoDatabase(Data,
                                "TMP_STATUS_CARTES",
                                "TMP_UPDATE",
                                database_type, database_name,
                                DropTable=True,
                                TableDict=TableParameter,
                                SizeChunck=10000)

        #### Step 4: Insert old values into the change in card status in order to keep track of changes values

        query = """
        
        INSERT INTO "CARD_STATUS"."CHANGE_STATUS_CARTES"
            SELECT T1.*
            FROM "CARD_STATUS"."STATUS_CARTES" AS T1
            INNER JOIN "TMP_UPDATE"."TMP_STATUS_CARTES" AS T2
            ON T1."CardHolderID" = T2."CardHolderID"
        
        """

        con_postgres = connect_to_database(database_type, database_name).CreateEngine()
        con_postgres.execute(query)
        con_postgres.close()

        #### Step 5: Update new values


        query = """
        
            UPDATE "CARD_STATUS"."STATUS_CARTES"
            SET 
            "ActivationDate" = T2."ActivationDate",
            "Address1" = T2."Address1",
            "Address2" = T2."Address2",
            "ApplicationName" = T2."ApplicationName",
            "AvailableBalance" = T2."AvailableBalance",
            "CardStatus" = T2."CardStatus",
            "City" = T2."City",
            "Country" = T2."Country", 
            "CreationDate" = T2."CreationDate",
            "BirthDate" = T2."BirthDate", 
            "DistributorCode" = T2."DistributorCode",
            "Email" = T2."Email",
            "FirstName" = T2."FirstName",
            "IBAN" = T2."IBAN",
            "IsExcludedAddress" = T2."IsExcludedAddress",
            "IsRenewal" = T2."IsRenewal",
            "KYC_Status" = T2."KYC_Status",
            "LastName" = T2."LastName",
            "NoMobile" = T2."NoMobile",
            "PostCode" = T2."PostCode",
            "Programme" = T2."Programme",
            "RenewalDate" = T2."RenewalDate",
            "UpdateDate" = T2."UpdateDate",
            "ExpirationDate" = T2."ExpirationDate"
            FROM "TMP_UPDATE"."TMP_STATUS_CARTES" AS T2
            WHERE "CARD_STATUS"."STATUS_CARTES"."CardHolderID" = T2."CardHolderID"
        
        
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

