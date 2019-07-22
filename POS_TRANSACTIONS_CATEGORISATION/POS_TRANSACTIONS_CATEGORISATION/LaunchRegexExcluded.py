
import pandas as pd
# Database connection
from creacard_connectors.creacard_connectors.database_connector import connect_to_database
import time

def ExcludedRegex(NumRow, DataRegex, engine, TlbName, schema):
    # Step 2.2 looping update
        query = """
        UPDATE "{}"."{}"
        SET "UNIVERS" = '{}', 
        "SOUS_UNIVERS" = '{}'
        WHERE "MCC" IN ('{}') AND 
        "MerchantName" ~* '{}' 
        """.format(schema, TlbName, str(DataRegex.loc[NumRow, "UNIVERS_DATABASE"].encode('ascii', 'ignore')),
                   str(DataRegex.loc[NumRow, "SOUS_UNIVERS_DATABASE"].encode('ascii', 'ignore')),
                   DataRegex.loc[NumRow, "MCC_DATABASE"].encode('ascii', 'ignore').replace("'", "''"),
                   DataRegex.loc[NumRow, "REGEX"].encode('ascii', 'ignore').replace("'", "''"))

        engine.execute(query)
        print(NumRow)

def IncludedRegex(NumRow, DataRegex, engine, TlbName, schema):
    # Step 2.2 looping update
        query = """
        UPDATE "{}"."{}"
        SET "UNIVERS" = '{}', 
        "SOUS_UNIVERS" = '{}'
        WHERE "UNIVERS" = '' and "SOUS_UNIVERS" = '' and "MerchantName" ~* '{}' 
        """.format(schema, TlbName, str(DataRegex.loc[NumRow, "UNIVERS_DATABASE"].encode('ascii', 'ignore')),
                   str(DataRegex.loc[NumRow, "SOUS_UNIVERS_DATABASE"].encode('ascii', 'ignore')),
                   DataRegex.loc[NumRow, "REGEX"].encode('ascii', 'ignore').replace("'", "''"))

        engine.execute(query)
        print(NumRow)



def fill_univers_sous_univers(database_type, database_name, schema, TlbName):

    engine = connect_to_database(database_type, database_name).CreateEngine()



    # Step 1.1 - MCC code note 1 -- update categories and under categories
    query = """
    UPDATE "{}"."{}"
    SET "UNIVERS" = T4."UNIVERS_DATABASE",
        "SOUS_UNIVERS" = T4."SOUS_UNIVERS_DATABASE"
    from(
        -- Join MCC categories, univers database & MCC database 
        select T3."UNIVERS_DATABASE",T3."SOUS_UNIVERS_DATABASE",T2."MCC" as "MCC_DATABASE" 
        from "REFERENTIEL"."MCC_CATEGORIES" as T1 
        INNER JOIN "REFERENTIEL"."MCC_CODE_LINK" as T2 
        ON T1."MCC_CODE" = T2."MCC_CODE"
        INNER JOIN "REFERENTIEL"."UNIVERS_DESCRIPTION" as T3
        ON T1."UNIVERS" = T3."UNIVERS" and 
        T1."SOUS_UNIVERS" = T3."SOUS_UNIVERS"
        WHERE T1."NOTE" in ('1')
        ) as T4
    WHERE T4."MCC_DATABASE" = "MCC"
    """.format(schema,TlbName)

    tic = time.time()
    engine.execute(query)
    print("update of exclusion was done in {} seconds".format(time.time() - tic))

    # Step 1.2 - MCC code note 1 -- Regex excluded
    query = """
        select T3."UNIVERS_DATABASE",T3."SOUS_UNIVERS_DATABASE",T2."MCC" as "MCC_DATABASE",T1."NEW_REGEX" as "REGEX"
        from "REFERENTIEL"."REGEX_EXCLUDED" as T1 
        INNER JOIN "REFERENTIEL"."MCC_CODE_LINK" as T2 
        ON T1."MCC_CODE" = T2."MCC_CODE"
        INNER JOIN "REFERENTIEL"."UNIVERS_DESCRIPTION" as T3
        ON T1."UNIVERS" = T3."UNIVERS" and 
        T1."SOUS_UNIVERS" = T3."SOUS_UNIVERS"
        INNER JOIN "REFERENTIEL"."MCC_CATEGORIES" as T4
        ON T1."MCC_CODE" = T4."MCC_CODE"
        WHERE T4."NOTE" = 1  
    """

    DataRegex = pd.read_sql(query, con=engine)

    # Step 2.2 looping update
    #tic = time.time()
    #p = ThreadPool(3)
    #p.map(
    #    partial(RegexExcluded.ExcludedRegex, DataRegex=DataRegex, engine=engine), range(0, len(DataRegex)))
    #p.close()
    #p.join()


    print("update of exclusion was done in {} seconds".format(time.time() - tic))



    tic = time.time()
    for i in range(0,len(DataRegex)):
        print(i)
        ExcludedRegex(NumRow=i, DataRegex=DataRegex, engine=engine, TlbName=TlbName, schema=schema)

    print("update of exclusion was done in {} seconds".format(time.time() - tic))

    # Step 2.1 - MCC code note 0 and 2 -- update categories and under categories
    query = """
    UPDATE "{}"."{}"
    SET "UNIVERS" = T4."UNIVERS_DATABASE",
        "SOUS_UNIVERS" = T4."SOUS_UNIVERS_DATABASE"
    from(
        -- Join MCC categories, univers database & MCC database 
        select T3."UNIVERS_DATABASE",T3."SOUS_UNIVERS_DATABASE",T2."MCC" as "MCC_DATABASE" 
        from "REFERENTIEL"."MCC_CATEGORIES" as T1 
        INNER JOIN "REFERENTIEL"."MCC_CODE_LINK" as T2 
        ON T1."MCC_CODE" = T2."MCC_CODE"
        INNER JOIN "REFERENTIEL"."UNIVERS_DESCRIPTION" as T3
        ON T1."UNIVERS" = T3."UNIVERS" and 
        T1."SOUS_UNIVERS" = T3."SOUS_UNIVERS"
        WHERE T1."NOTE" in ('2','0')
        ) as T4
    WHERE T4."MCC_DATABASE" = "MCC"
    """.format(schema,TlbName)

    tic = time.time()
    engine.execute(query)
    print("update of exclusion was done in {} seconds".format(time.time() - tic))

    # Step 2.2 - MCC code note 1 -- Regex excluded
    query = """
        select T3."UNIVERS_DATABASE",T3."SOUS_UNIVERS_DATABASE",T2."MCC" as "MCC_DATABASE",T1."NEW_REGEX" as "REGEX"
        from "REFERENTIEL"."REGEX_EXCLUDED" as T1 
        INNER JOIN "REFERENTIEL"."MCC_CODE_LINK" as T2 
        ON T1."MCC_CODE" = T2."MCC_CODE"
        INNER JOIN "REFERENTIEL"."UNIVERS_DESCRIPTION" as T3
        ON T1."UNIVERS" = T3."UNIVERS" and 
        T1."SOUS_UNIVERS" = T3."SOUS_UNIVERS"
        INNER JOIN "REFERENTIEL"."MCC_CATEGORIES" as T4
        ON T1."MCC_CODE" = T4."MCC_CODE"
        WHERE T4."NOTE" in ('0','2')
    """

    DataRegex = pd.read_sql(query, con=engine)

    tic = time.time()
    for i in range(0,len(DataRegex)):
        print(i)
        ExcludedRegex(NumRow=i, DataRegex=DataRegex, engine=engine, TlbName=TlbName, schema=schema)

    print("update of exclusion was done in {} seconds".format(time.time() - tic))

    # step 3 - regex adding

    # Step 3.1 - MCC code note 1 -- Regex excluded
    query = """
      select T3."UNIVERS_DATABASE",T3."SOUS_UNIVERS_DATABASE",
        T1."NEW_REGEX" as "REGEX"
        from "REFERENTIEL"."REGEX_INCLUDED" as T1 
        INNER JOIN "REFERENTIEL"."UNIVERS_DESCRIPTION" as T3
        ON T1."UNIVERS" = T3."UNIVERS" and 
        T1."SOUS_UNIVERS" = T3."SOUS_UNIVERS" 
    """

    DataRegex = pd.read_sql(query, con=engine)

    tic = time.time()
    for i in range(0, len(DataRegex)):
        print(i)
        IncludedRegex(NumRow=i, DataRegex=DataRegex, engine=engine, TlbName=TlbName, schema=schema)

    print("update of exclusion was done in {} seconds".format(time.time() - tic))

    engine.close()
