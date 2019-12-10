from creacard_connectors.database_connector import connect_to_database
from Creacard_Utils.import_credentials import credentials_extractor
import pandas as pd

# extract available schema

engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()

query = """
    select "schema_name"
    from information_schema.schemata
    where "schema_name" !~* '^pg*.' and "schema_name" not in ('information_schema')
"""
data = pd.read_sql(query, con=engine)
list_schema = data["schema_name"].tolist()
del data


# get tables from a schema

query = """

SELECT "table_name"
  FROM "information_schema"."tables"
 WHERE table_schema IN ('{}')

""".format(schema)

data = pd.read_sql(query, con=engine)
list_table = data["table_name"].tolist()
del data


query = """

select column_name,data_type 
from information_schema.columns 
where table_name = 'POS_TRANSACTIONS'
and table_schema IN ('TRANSACTIONS')


"""
data = pd.read_sql(query, con=engine)