import time
import pandas as pd


def ExcludedRegex(NumRow,DataRegex,engine):
    # Step 2.2 looping update
        query = """
        UPDATE "TRANSACTIONS"."POS_TRANSACTIONS"
        SET "UNIVERS" = '{}', 
        "SOUS_UNIVERS" = '{}'
        WHERE "MCC" IN ('{}') AND 
        "MerchantName" ~* '{}' 
        """.format(str(DataRegex.loc[NumRow, "UNIVERS_DATABASE"].encode('ascii', 'ignore')),
                   str(DataRegex.loc[NumRow, "SOUS_UNIVERS_DATABASE"].encode('ascii', 'ignore')),
                   DataRegex.loc[NumRow, "MCC_DATABASE"].encode('ascii', 'ignore').replace("'", "''"),
                   DataRegex.loc[NumRow, "REGEX"].encode('ascii', 'ignore').replace("'", "''"))

        engine.execute(query)
        print(NumRow)

def IncludedRegex(NumRow,DataRegex,engine):
    # Step 2.2 looping update
        query = """
        UPDATE "TRANSACTIONS"."POS_TRANSACTIONS"
        SET "UNIVERS" = '{}', 
        "SOUS_UNIVERS" = '{}'
        WHERE "UNIVERS" = '' and "SOUS_UNIVERS" = '' and "MerchantName" ~* '{}' 
        """.format(str(DataRegex.loc[NumRow, "UNIVERS_DATABASE"].encode('ascii', 'ignore')),
                   str(DataRegex.loc[NumRow, "SOUS_UNIVERS_DATABASE"].encode('ascii', 'ignore')),
                   DataRegex.loc[NumRow, "REGEX"].encode('ascii', 'ignore').replace("'", "''"))

        engine.execute(query)
        print(NumRow)


