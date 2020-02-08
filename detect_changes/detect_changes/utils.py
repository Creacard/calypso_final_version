import datetime
from Postgres_Toolsbox.Ingestion import InsertTableIntoDatabase
from Creacard_Utils.read_with_protocole import *

def transform_data(Data, filepath):

    keepcol = ["CardHolderID", "Email", "FirstName",
               "LastName", "Date of Birth", "IBAN", "NoMobile",
               "Programme", "Address1", "Address2",
               "PostCode","City"]

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
                     "KYC Status", "expirydate", "AvailableBalance"]

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

    # only extract useful columns
    Data = Data[keepcol]

    Data["Date of Birth"] = pd.to_datetime(Data["Date of Birth"], format="%b %d %Y %I:%M%p", errors='coerce')

    Data["NoMobile"] = Data["NoMobile"].str.replace("^00", "", regex=True)

    # replace .0 at the end$

    Data["NoMobile"] = Data["NoMobile"].str.replace("\.0$", "", regex=True)

    # delete only literal '|' from string

    Data["NoMobile"] = Data["NoMobile"].str.replace("\|", "", regex=True)

    Data.columns = ["CardHolderID", "Email", "FirstName", "LastName",
                        "BirthDate", "IBAN", "NoMobile", "Programme", "Address1",
                        "Address2", "PostCode", "City"]

    return Data


def extract_changes_email(tmp_data, data_current, FileName):


    DateFile = pd.to_datetime(
        FileName.split("-")[1] + "-" + FileName.split("-")[2] + "-" + FileName.split("-")[3].replace(".csv", ""))

    # store new addresses based on new CardHolderID
    new_chid = tmp_data[~tmp_data["CardHolderID"].isin(data_current["CardHolderID"])]

    # tmp_data_current
    tmp_data_current = data_current.copy()

    tmp_data_current = tmp_data_current.set_index('CardHolderID')
    tmp_data_current = tmp_data_current.sort_index()

    tmp_data = tmp_data.set_index('CardHolderID')
    tmp_data = tmp_data[tmp_data.index.isin(tmp_data_current.index)]
    tmp_data = tmp_data.sort_index()

    for col in tmp_data_current.columns:
        if col == "FirstName" or col == "LastName":
            tmp_data_current[col] = tmp_data_current[col].str.replace(" ", "")
        tmp_data_current.loc[tmp_data_current[col].isna(), col] = ""

    for col in tmp_data.columns:
        if col == "FirstName" or col == "LastName":
            tmp_data[col] = tmp_data[col].str.replace(" ", "")
        tmp_data.loc[tmp_data[col].isna(), col] = ""

    tmp_data_current = tmp_data_current[tmp_data_current.index.isin(tmp_data.index)]
    tmp_data_current = tmp_data_current.sort_index()
    tmp_data = tmp_data.sort_index()

    A = tmp_data_current != tmp_data
    A = A.astype(int)

    A["sum_changes"] = A.sum(axis=1)

    A = A[A["sum_changes"] > 0]
    A.columns = ["Is_ch_Email", "Is_ch_FirstName", "Is_ch_LastName",
                 "Is_ch_BirthDate", "Is_ch_IBAN", "Is_ch_NoMobile", "Is_ch_Programme", "total_ch"]

    tmp_insert = data_current[data_current["CardHolderID"].isin(A.index)]
    tmp_insert = tmp_insert.reset_index(drop=True)
    A = A.reset_index(drop=False)
    tmp_insert = pd.merge(tmp_insert, A, on="CardHolderID", how="inner")

    tmp_insert["dt_change"] = DateFile - datetime.timedelta(days=1)

    TableParameter = {}
    TableParameter["CardHolderID"] = "VARCHAR (50)"
    TableParameter["Email"] = "TEXT"
    TableParameter["FirstName"] = "TEXT"
    TableParameter["LastName"] = "TEXT"
    TableParameter["BirthDate"] = "timestamp without time zone"
    TableParameter["IBAN"] = "TEXT"
    TableParameter["NoMobile"] = "TEXT"
    TableParameter["Programme"] = "VARCHAR (50)"
    TableParameter["Is_ch_Email"] = "INTEGER"
    TableParameter["Is_ch_FirstName"] = "INTEGER"
    TableParameter["Is_ch_LastName"] = "INTEGER"
    TableParameter["Is_ch_BirthDate"] = "INTEGER"
    TableParameter["Is_ch_IBAN"] = "INTEGER"
    TableParameter["Is_ch_NoMobile"] = "INTEGER"
    TableParameter["Is_ch_Programme"] = "INTEGER"
    TableParameter["total_ch"] = "INTEGER"
    TableParameter["dt_change"] = "timestamp without time zone"

    schema = "CARD_STATUS"
    TlbName = "CHANGE_CUSTOMERS_CARTES"

    InsertTableIntoDatabase(tmp_insert,
                            TlbName,
                            schema,
                            "Postgres",
                            "Creacard_Calypso",
                            TableDict=TableParameter,
                            DropTable=False)


def extract_change_address(data_current, tmp_data, Filename):

    DateFile = pd.to_datetime(Filename.split("-")[1] + "-" + Filename.split("-")[2] + "-" + Filename.split("-")[3].replace(".csv", ""))

    # store new addresses based on new CardHolderID
    new_addresses = tmp_data[~tmp_data["CardHolderID"].isin(data_current["CardHolderID"])]

    # tmp_data_current
    tmp_data_current = data_current.copy()

    tmp_data_current = tmp_data_current.set_index('CardHolderID')
    tmp_data_current = tmp_data_current.sort_index()

    tmp_data = tmp_data.set_index('CardHolderID')
    tmp_data = tmp_data[tmp_data.index.isin(tmp_data_current.index)]
    tmp_data = tmp_data.sort_index()

    for col in tmp_data_current.columns:
        tmp_data_current.loc[tmp_data_current[col].isna(), col] = ""

    for col in tmp_data.columns:
        tmp_data.loc[tmp_data[col].isna(), col] = ""

    tmp_data_current["PostCode"] = tmp_data_current["PostCode"].astype(str)
    tmp_data["PostCode"] = tmp_data["PostCode"].astype(str)

    tmp_data_current["concat_address"] = tmp_data_current["Address1"].str.lower().str.replace(" ", "") + \
                                         tmp_data_current["Address2"].str.lower().str.replace(" ", "") + \
                                         tmp_data_current["PostCode"].str.lower().str.replace(" ", "") + \
                                         tmp_data_current["Address2"].str.lower().str.replace(" ", "") + \
                                         tmp_data_current["City"].str.lower().str.replace(" ", "")

    tmp_data["concat_address"] = tmp_data["Address1"].str.lower().str.replace(" ", "") + tmp_data[
        "Address2"].str.lower().str.replace(" ", "") + tmp_data["PostCode"].str.lower().str.replace(" ", "") + tmp_data[
                                     "Address2"].str.lower().str.replace(" ", "") + tmp_data[
                                     "City"].str.lower().str.replace(" ", "")


    tmp_data_current = tmp_data_current[tmp_data_current.index.isin(tmp_data.index)]
    tmp_data_current = tmp_data_current.sort_index()
    tmp_data = tmp_data.sort_index()

    A = tmp_data_current["concat_address"] != tmp_data["concat_address"]
    A = A[A == True]

    tmp_insert = data_current[data_current["CardHolderID"].isin(A.index)]

    tmp_insert = pd.concat([tmp_insert, new_addresses], axis=0).reset_index(drop=True)

    tmp_insert["dt_change"] = DateFile - datetime.timedelta(days=1)

    TableParameter = {}
    TableParameter["CardHolderID"] = "VARCHAR (50)"
    TableParameter["Address1"] = "VARCHAR (100)"
    TableParameter["Address2"] = "VARCHAR (50)"
    TableParameter["PostCode"] = "VARCHAR (50)"
    TableParameter["City"] = "VARCHAR (100)"
    TableParameter["dt_change"] = "timestamp without time zone"

    schema = "CARD_STATUS"
    TlbName = "CHANGE_ADDRESSES_CARTES"

    InsertTableIntoDatabase(tmp_insert,
                            TlbName,
                            schema,
                            "Postgres",
                            "Creacard_Calypso",
                            TableDict=TableParameter,
                            DropTable=False)