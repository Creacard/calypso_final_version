import pandas as pd
from creacard_connectors.database_connector import connect_to_database
import time
from Postgres_Toolsbox.Ingestion import InsertTableIntoDatabase


def create_master_id(schema):

    query = """
    
    CREATE TABLE "{}"."MASTER_ID" as 
    select *, null::integer as "MOBILE_ID", null::integer as "USER_ID", null::integer as "CONTACT_ID", null::integer as "PERSON_ID",  null::integer as "MOVIDON_ID"
    from "{}"."TMP_USER_ID"
    
    """.format(schema, schema)

    engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()
    engine.execute(query)
    engine.close()

