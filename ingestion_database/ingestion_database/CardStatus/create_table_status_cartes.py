import pandas as pd

def create_status_carte_CardStatus2(Data, filepath):

    keepcol = ["CardHolderID", "Email", "FirstName",
     "LastName", "City", "Country", "Card Status", "DistributorCode",
     "ApplicationName", "Date of Birth", "IBAN",
     "CreatedDate", "UpdatedDate", "Address1", "Address2", "PostCode",
     "KYC Status", "expirydate", "AvailableBalance", "NoMobile",
     "Programme"]

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
                     "KYC Status", "expirydate", "AvailableBalance", "UDF2","NoMobile",
                     "UDF3", "VPVR"]

    # add the names of columns to the dataframe
    Data.columns = col_names

    # store the missing columns
    missing_columns = list(set(keepcol).difference(col_names))

    if missing_columns: # if the list is not add new columns to the dataframe
        for col in missing_columns:
            Data[col] = None


    # transform date columns to pd.datetime format in order to have a consistent format
    # of date over the database
    Data["UpdatedDate"] = pd.to_datetime(Data["UpdatedDate"], format="%b %d %Y %I:%M%p", errors='coerce')
    Data["CreatedDate"] = pd.to_datetime(Data["CreatedDate"], format="%b %d %Y %I:%M%p", errors='coerce')
    Data["Date of Birth"] = pd.to_datetime(Data["Date of Birth"], format="%b %d %Y %I:%M%p", errors='coerce')

    # transform expirydate
    Data["expirydate"] = Data["expirydate"].astype(str)
    Data["expirydate"] = "20" + Data["expirydate"].str[0:2] + "-" + Data["expirydate"].str[2:] + "-01"
    Data["expirydate"] = pd.to_datetime(Data["expirydate"], format='%Y-%m-%d', errors='coerce')

    Data = Data[keepcol]


    # condition remove address
    AddressToRemove = ["77 OXFORD STREET LONDON","17 RUE D ORLEANS","TSA 51760","77 Oxford Street London","36 CARNABY STREET",
    "36 CARNABY STREET LONDON","36 CARNABY STREET LONDON","ADDRESS","17 RUE D ORLEANS PARIS","CreaCard Espana S L  Paseo de Gracia 59",
     "36 Carnaby Street London","CREACARD SA Pl  Marcel Broodthaers 8 Box 5","17 Rue D Orleans Paris",
     "CREACARD ESPANA S L  PASEO DE GRACIA 59","CreaCard 17 rue d Orleans","CREACARD SA PL  MARCEL BROODTHAERS 8 BOX 75",
     "CREACARD SA PL  MARCEL BROODTHAERS 8 BOX 75","36 Carnaby Street","77 OXFORD STREET"]

    Data["IsExcludedAddress"] = (Data.Address1.isin(AddressToRemove)).astype(int)

    Data["ActivationDate"] = pd.NaT
    Data["IsRenewal"] = 0
    Data["RenewalDate"] = pd.NaT

    Data = Data[sorted(Data.columns)]

    colnames = ["ActivationDate", "Address1", "Address2", "ApplicationName","AvailableBalance",
                "CardStatus", "CardHolderID", "City", "Country", "CreationDate",
                "BirthDate", "DistributorCode", "Email", "FirstName", "IBAN","IsExcludedAddress",
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



    return Data
