from Utils import LogErrorFromInsert
from ingestion_database.ingest_transactions import csv_transactions

credentials = dict()
credentials["user"] = "justin"
credentials["pwd"] = "Thekiller81"


# Create the logger in order to log
# possible errors that could be generated
# during the ingestion
logger = LogErrorFromInsert.CreateLogger("DayliTransaction", "Ingestion_", '/Users/justinvalet/tmp/Log/')


# Indicate the folder on where .csv file are located
Folder = "/Users/justinvalet/CreacardProject/Data/TestIngestion/"

csv_transactions.ingest_csv_transactions(credentials, "test_ingestion", Folder, logger=logger)

