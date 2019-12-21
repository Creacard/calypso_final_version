import pandas as pd
from creacard_connectors.database_connector import connect_to_database
from Postgres_Toolsbox.Ingestion import InsertTableIntoDatabase


def generate_person_id_construction(schema):

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
    
    CREATE TABLE "{}"."ID_PERSON"(
    
        "CONTACT_ID" SERIAL,
        "combinaison" TEXT
    )
    
    """.format(schema)

    engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()
    engine.execute(query)
    engine.close()


    InsertTableIntoDatabase(data, TlbName="ID_PERSON", Schema=schema,
                            database_name="Creacard_Calypso",
                            database_type="Postgres",
                            DropTable=False,
                            InstertInParrell=False)


    query = """
    
    update "CUSTOMERS"."MASTER_ID"
    set "PERSON_ID" = T1."PERSON_ID"
    from "CUSTOMERS"."ID_PERSON" as T1
    where concat("CUSTOMERS"."MASTER_ID"."BirthDate", "CUSTOMERS"."MASTER_ID"."LastName") = T1."combinaison"
    
    """

    engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()
    engine.execute(query)
    engine.close()

    query = """
    
    update "CUSTOMERS"."MASTER_ID"
    set "PERSON_ID" = concat("USER_ID",'_',"MOBILE_ID")
    where "GoodCombinaison" = 0 and "PERSON_ID" is null 
    
    """

    engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()
    engine.execute(query)
    engine.close()


