import pandas as pd
import time
import datetime
import glob
import os
import time


from Postgres_Toolsbox.Ingestion import InsertTableIntoDatabase
from creacard_connectors.database_connector import connect_to_database
from Creacard_Utils.import_credentials import credentials_extractor

engine = connect_to_database("Postgres","Creacard_Calypso").CreateEngine()



folder = "/Users/justinvalet/Downloads/wetransfer-872213/CardActivationReport/"
os.chdir(folder)
_file_list = glob.glob('**.csv')
_file_list = pd.DataFrame(_file_list, columns=["FileName"])


data = pd.read_csv(folder+_file_list.FileName[0],
                   sep=",",
                   encoding='iso-8859-1'
                   )

data["CardholderID"] = data["CardholderID"].str.replace("'","")
data["ActivationDate"] = pd.to_datetime(data["ActivationDate"])

data = data[["ActivationDate", "CardholderID"]]



Month = []
Year = []
Day = []
# create a pandas DataFrame that contain
# file and associated year and month in order
# to ingest .csv file for each month
ii = _file_list.FileName.str.split('_')
for i in range(0, len(ii)):
    Year.append(ii[i][-1][0:4])
    Month.append(ii[i][-1][4:6])
    Day.append(ii[i][-1][6:8])

ListOfFile = pd.concat([_file_list, pd.DataFrame(Year, columns=['Year']), pd.DataFrame(Month, columns=['Month']),
                            pd.DataFrame(Day, columns=['Day'])],
                           axis=1)

ListOfFile["FilePath"] = folder + ListOfFile["FileName"]
ListOfFile["FileTime"] = ListOfFile["Year"] + "-" + ListOfFile["Month"] + "-" + ListOfFile["Day"]
ListOfFile["FileTime"] = pd.to_datetime(ListOfFile.FileTime, errors='coerce')
ListOfFile["FileTimeDelta"] = ListOfFile["FileTime"] - datetime.timedelta(days=1)

ListOfFile["Year"] = ListOfFile.FileTimeDelta.dt.year
ListOfFile["Month"] = ListOfFile.FileTimeDelta.dt.month
ListOfFile["Day"] = ListOfFile.FileTimeDelta.dt.month

_file_date_condition = dict()
_file_date_condition["start"] = [2018, 10, 27]

if _file_date_condition is not None:
    if len(_file_date_condition) == 2:
        ListOfFile = ListOfFile[
            (ListOfFile.loc[:, "FileTimeDelta"] >= datetime.datetime(_file_date_condition["start"][0],
                                                                     _file_date_condition["start"][1],
                                                                     _file_date_condition["start"][2]))
            & (ListOfFile.loc[:, "FileTimeDelta"] <= datetime.datetime(_file_date_condition["end"][0],
                                                                       _file_date_condition["end"][1],
                                                                       _file_date_condition["end"][2]))]
    else:
        ListOfFile = ListOfFile[
            (ListOfFile.loc[:, "FileTimeDelta"] >= datetime.datetime(_file_date_condition["start"][0],
                                                                     _file_date_condition["start"][1],
                                                                     _file_date_condition["start"][2]))]


tic = time.time()

FinalData = None

for file in ListOfFile["FilePath"]:

    KeepCol = ["ActivationTime", "CardholderID"]
    Data = pd.read_csv(str(file),
                       sep=",",
                       encoding='iso-8859-1',usecols=KeepCol)
    Data["ActivationTime"] = pd.to_datetime(Data["ActivationTime"])

    FinalData = pd.concat([FinalData, Data], axis=0)

exec_time = time.time() - tic


FinalData.columns = ["ActivationTime", "CardHolderID"]
FinalData = pd.concat([data, FinalData], axis=0)



TableParameter = {}
TableParameter["ActivationTime"] = "timestamp without time zone"
TableParameter["CardHolderID"] = "VARCHAR (50)"

InsertTableIntoDatabase(FinalData,
                        "ACTIVATION_REPORT",
                        "CARD_STATUS",
                        "Postgres",
                        "Creacard_Calypso",
                    DropTable=True, TableDict=TableParameter)



"""

update "CARD_STATUS"."STATUS_CARTES" as T1
SET "ActivationDate" = "ActivationTime"
FROM "CARD_STATUS"."ACTIVATION_REPORT" as T2
WHERE 
T1."CardHolderID" = T2."CardHolderID" 


"""

