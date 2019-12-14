import pandas as pd
from creacard_connectors.database_connector import connect_to_database
import time
from Postgres_Toolsbox.Ingestion import InsertTableIntoDatabase


def create_no_mobile(schema):

    query = """
    
    CREATE TABLE "{}"."ID_MOBILE"(
    
        "MOBILE_ID" SERIAL,
        "NoMobile" TEXT,
        "NUM_CARTES" INTEGER
    )
    
    """.format(schema)

    engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()
    engine.execute(query)
    engine.close()


    query = """
    
    select "NoMobile", count(*) as "NUM_CARTES"
    from "{}"."TMP_USER_ID"
    group by "NoMobile"
    
    """.format(schema)

    engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()

    data = pd.read_sql(query, con=engine)

    engine.close()

    InsertTableIntoDatabase(data, TlbName="ID_MOBILE", Schema='CUSTOMERS',
                            database_name="Creacard_Calypso",
                            database_type="Postgres",
                            DropTable=False,
                            InstertInParrell=False)



    query = """
    
    
    UPDATE "{}"."MASTER_ID" 
    SET "MOBILE_ID" = T1."MOBILE_ID"
    from "{}"."ID_MOBILE" as T1
    where "{}"."MASTER_ID"."NoMobile" = T1."NoMobile"
    
    
    """.format(schema, schema, schema)

    engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()
    engine.execute(query)
    engine.close()

