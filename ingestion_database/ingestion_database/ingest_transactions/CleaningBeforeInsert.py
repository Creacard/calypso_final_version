import pandas as pd
import datetime

# The aim of this function is to preprocess all of .csv files before
# ingesting it into the associated table into the data base
# The file is uploaded from .csv into a pandas DataFrame
# This function return a pandas modified Dataframe
# This function is used into the ingestion function of .csv file as
# an optional argument


def cleaning_before(Data, FilePath):

    # Rename & Upper Case variables
    Data.columns = Data.columns.str.replace(" ", "")

    # Select variables to import

    FileName = FilePath.split('/')[-1]

    Continue = True

    if len(FileName.split("_")) == 3:
        DateFile = FileName.split("_")[1]
        if len(DateFile) == 6:
            DateFile = pd.to_datetime("20" + DateFile[4:6] + "-" + DateFile[2:4] + "-" + DateFile[0:2])
        else:
            Continue = False
    else:
        Continue = False

    if Continue:

        ### Filtering and cleaning ###
        # Remove characters from string into the dataset
        for var in Data.columns[Data.dtypes == 'object']:
            Data[var] = Data[var].str.replace("'", "")

        # Filtering by Transaction TP
        #Data = Data[~Data["TransactionTP"].isin(['ATM Domestic Auth', 'ATM International Auth',
                                                 #'POS Domestic Auth', 'POS International Auth', 'FX Fee'])]

        # Reset the index
        Data = Data.reset_index(drop=True)

        # Combine Date & Time
        Data = pd.concat(
            [Data, pd.DataFrame(Data.Date.astype(str) + " " + Data.Time.astype(str), columns=['TransactionTime'])],
            axis=1)
        Data["TransactionTime"] = pd.to_datetime(Data.TransactionTime, errors='coerce')

        # Filtering by Date
        # convert to Date to datetime
        Data["Date"] = pd.to_datetime(Data["Date"], errors='coerce')
        Data = Data[Data["Date"] == (DateFile - datetime.timedelta(days=1))]
        # Reset the index
        Data = Data.reset_index(drop=True)

        # Change Amount sign after 2018-04-10
        #if DateFile >= pd.to_datetime('2018-04-10'):
            #Data.loc[Data["TransactionTP"].isin(
                #['ATM Domestic', 'ATM International', 'Merchandise Refund Hold Reversals', 'Merchant refunds',
                 #'POS Domestic', 'POS International']), ["Amount"]] = - Data["Amount"]


        Data["Amount"] = Data["Amount"].abs()

        # Drop Data, Time & First Name
        Data = Data.drop(columns=["Date", "Time", "FirstName"])

        CardType = FileName
        if FileName.split("_")[0] == "PCSCHROMEEURRECONCILIATIONDETAILV2":
            CardType = "CHROME"
        elif FileName.split("_")[0] == "PCSFRANCEEURRECONCILIATIONDETAILV2":
            CardType = "BLACK"
        elif FileName.split("_")[0] == "PCSINFINITYEURRECONCILIATIONDETAILV2":
            CardType = "INFINITY"
        else:
            CardType = FileName

        Data["CardType"] = CardType

        # Store the name of the file
        Data["FileSource"] = str(FileName)

        # Reorder variables in order to always insure the order of variables when ingesting
        Data = Data[sorted(Data.columns)]

        col = ['Amount',
               'AuthNum',
               'CardHolderID',
               'CardNumber',
               'CardProductName',
               'CardType',
               'CardVPUType',
               'Currency',
               'CurrencyCode',
               'DebitCredit',
               'Description',
               'DistributorCode',
               'Fee',
               'FileSource',
               'LastName',
               'MCC',
               'MerchantAddress',
               'MerchantCity',
               'MerchantCountry',
               'MerchantID',
               'MerchantName',
               'ProgramName',
               'PurseCurrency',
               'Rate',
               'RemainingBalance',
               'Surcharge',
               'SweepCurrency',
               'TermState',
               'TransactionDB/CR',
               'TransactionID',
               'TransactionResult',
               'TransactionTP',
               'TransactionTime']

        Data = Data.loc[:, col]

    else:
        Data = None

    return Data
