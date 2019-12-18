import pandas as pd
from creacard_connectors.database_connector import connect_to_database
import time
from Postgres_Toolsbox.Ingestion import InsertTableIntoDatabase
import sys
import os
import json


def create_tmp_id(schema, tlb, schema_main):

    if sys.platform == "win32":
        folder_json = os.path.expanduser('~') + "\\conf_python\\unique_id_conditions.json"
    else:
        folder_json = os.environ['HOME'] + "/conf_python/unique_id_conditions.json"
    with open(folder_json, 'r') as JSON:
        conditions = json.load(JSON)

    condition = conditions["exclusion_cartes"]["request"]
    condition_on_email = conditions["condition_email"]["dataframe"]

    engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()

    query = """

      select "CardHolderID", "NoMobile", lower("Email") as "Email", "FirstName", "LastName", "BirthDate", "PostCode", "Address1", "Address2",
      "ActivationDate"
      from "{}"."{}"
      where {}

      """.format(schema, tlb, condition)

    data = pd.read_sql(query, con=engine)

    engine.close()
    for var in ["FirstName", "LastName", "Address1", "Address2", "PostCode", "Email"]:
        data[var] = data[var].str.encode('utf-8').astype(str)
        data.loc[data[var].isnull(), var] = ""
        data[var] = data[var].str.strip(" ")
        data[var] = data[var].str.replace(" ", "")
        data[var] = data[var].str.lower()

    data["GoodEmail"] = 1
    data.loc[data["Email"].str.contains(condition_on_email, regex=True), "GoodEmail"] = 0

    data["GoodCombinaison"] = 1
    data.loc[(data["LastName"].str.contains(conditions["condition_combinaison"]["LastName"], regex=True)) |
             (data["BirthDate"].isnull()) | (data["BirthDate"].isin(conditions["condition_combinaison"]["BirthDate"].split(","))), "GoodCombinaison"] = 0

    query = """
 
    DROP TABLE IF EXISTS "CUSTOMERS"."TMP_USER_ID" CASCADE

    """

    engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()
    engine.execute(query)
    engine.close()

    query = """
    
    
    CREATE TABLE "{}"."TMP_USER_ID"(
    
        "CardHolderID" VARCHAR(50),
        "NoMobile" TEXT,
        "Email" TEXT,
        "FirstName" TEXT,
        "LastName" TEXT,
        "BirthDate" TEXT,
        "PostCode" TEXT,
        "Address1" TEXT, 
        "Address2" TEXT,
        "ActivationDate" timestamp without time zone,
        "GoodEmail" INTEGER,
        "GoodCombinaison" INTEGER
    )
    
    """.format(schema_main)

    engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()
    engine.execute(query)
    engine.close()

    data = data[~data["NoMobile"].isnull()]

    InsertTableIntoDatabase(data, TlbName="TMP_USER_ID", Schema=schema_main,
                            database_name="Creacard_Calypso",
                            database_type="Postgres",
                            DropTable=False,
                            InstertInParrell=False)












