import pandas as pd
from creacard_connectors.database_connector import connect_to_database
import time
from Postgres_Toolsbox.Ingestion import InsertTableIntoDatabase


def create_master_id():

    query = """
    
    CREATE TABLE "CUSTOMERS"."MASTER_ID" as 
    select *, null as "MOBILE_ID", null as "USER_ID", null as "CONTACT_ID"
    from "CUSTOMERS"."TMP_USER_ID"
    
    """

    engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()
    engine.execute(query)
    engine.close()

