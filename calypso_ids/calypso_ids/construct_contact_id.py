import pandas as pd
from creacard_connectors.database_connector import connect_to_database
from Postgres_Toolsbox.Ingestion import InsertTableIntoDatabase


def generate_contact_id_construction(schema):

    # create date ID
    query = """
    select distinct concat("BirthDate","LastName") as "combinaison"
    from "CUSTOMERS"."MASTER_ID"
    where "GoodCombinaison" = 1
    """

    engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()
    data = pd.read_sql(query, con=engine)
    engine.close()

    query = """
    
    CREATE TABLE "{}"."ID_CONTACT"(
    
        "CONTACT_ID" SERIAL,
        "combinaison" TEXT
    )
    
    """.format(schema)

    engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()
    engine.execute(query)
    engine.close()


    InsertTableIntoDatabase(data, TlbName="ID_CONTACT", Schema=schema,
                            database_name="Creacard_Calypso",
                            database_type="Postgres",
                            DropTable=False,
                            InstertInParrell=False)


    query = """
    
    update "CUSTOMERS"."MASTER_ID"
    set "CONTACT_ID" = T1."CONTACT_ID"
    from "CUSTOMERS"."ID_CONTACT" as T1
    where concat("CUSTOMERS"."MASTER_ID"."BirthDate", "CUSTOMERS"."MASTER_ID"."LastName") = T1."combinaison"
    
    """

    engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()
    engine.execute(query)
    engine.close()

    query = """
    
    update "CUSTOMERS"."MASTER_ID"
    set "CONTACT_ID" = concat("USER_ID",'_',"MOBILE_ID")
    where "GoodCombinaison" = 0 and "CONTACT_ID" is null 
    
    """

    engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()
    engine.execute(query)
    engine.close()


