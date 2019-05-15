from Utils import LogErrorFromInsert, SendEmailGmailLog


# Credentials
user = 'justin'
pwd = 'Thekiller81'

# Database connection
Schema = 'TestIngestion'
hostname = '127.0.0.1'
port = '26026'
database = 'Creacard'


from BDDTools.DataBaseEngine import ConnectToPostgres
from BDDTools.Ingestion import InsertTableIntoDatabase
import pandas as pd

PostgresConnect = ConnectToPostgres(user=user, pwd=pwd, hostname=hostname, port=port, database=database)
PostgresConnect = PostgresConnect.CreateEngine()

engine = PostgresConnect._openConnection


query = """

Select "MCC", count(*) as NbRow
from "TRANSACTIONS"."POS_TRANSACTIONS"
group by "MCC"
order by 2 DESC

"""

Data = pd.read_sql(query, con=engine)


# insert into postgres a pandas dataframe

InsertTableIntoDatabase(Data, engine, 'TestIngest', 'REFERENTIEL', DropTable=True)


# Create the logger in order to log
# possible errors that could be generated
# during the ingestion
logger = LogErrorFromInsert.CreateLogger("TestLog", "TestFile")


# Indicate the folder on where .csv file are located
Folder = "/Users/justinvalet/CreacardProject/Data/TestIngestion/"



### Define ingestion parameters ###










# Sending email
Subject = "Ingestion results"
Sender = 'creacardlogs@gmail.com'
pwd = "CreaCard2018"
Receiver = 'valet.justin@yahoo.fr'
Body = """

    Please find attached to this e-mail, the results 
    ingestion

"""
PathToFile = '/Users/justinvalet/testemail.csv'
FileToAttach = 'testemail.csv'

SendEmailGmailLog.EmailingAttachment(Subject, pwd, Sender, Receiver, Body, PathToFile, FileToAttach)