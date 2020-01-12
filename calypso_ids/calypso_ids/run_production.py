import pandas as pd
from creacard_connectors.database_connector import connect_to_database
import time
from Postgres_Toolsbox.Ingestion import InsertTableIntoDatabase
import sys
import os
import json
import datetime
from calypso_ids.id_generator import compute_user_id


def calypso_ids_production(schema_main, connexion_postgres):
    """Compute new ids and ids that changed overtime

        Parameters
        -----------
        schema_main: str
            schema where ids are stored
        connexion_postgres: str
            name of postgres connexction (referred to conf_python file)

    """

    # extract condition (exclusion) of stored on the computer
    if sys.platform == "win32":
        folder_json = os.path.expanduser('~') + "\\conf_python\\unique_id_conditions.json"
    else:
        folder_json = os.environ['HOME'] + "/conf_python/unique_id_conditions.json"
    with open(folder_json, 'r') as JSON:
        conditions = json.load(JSON)

    condition = conditions["exclusion_cartes"]["request"]
    condition_on_email = conditions["condition_email"]["dataframe"]

    # generate new ids and handling customer's information that changed

    # Step - 0 :
    # - extract new card
    # - extract card that was already associated to a user id but with information that changed
    engine = connect_to_database("Postgres", connexion_postgres).CreateEngine()

    query = """

    select T1."CardHolderID", T1."NoMobile", lower(T1."Email") as "Email",
    T1."FirstName", T1."LastName", T1."BirthDate", T1."PostCode", T1."Address1", T1."Address2",
    T1."ActivationDate"
    from "CARD_STATUS"."STATUS_CARTES" as T1
    left join "CUSTOMERS"."MASTER_ID" as T2
    on T1."CardHolderID" = T2."CardHolderID"
    where (T1."NoMobile" is not null) and (T1."Email" !~* '.*creacard.*|.*prepaidfinancial.*|.*financial.*')
    and T2."USER_ID" is null

    UNION ALL


    select T1."CardHolderID", T1."NoMobile", lower(T1."Email") as "Email",
    T1."FirstName", T1."LastName", T1."BirthDate", T1."PostCode", T1."Address1", T1."Address2",
    T1."ActivationDate"
    from "CARD_STATUS"."STATUS_CARTES" as T1
    Join(
       select "CardHolderID"
       from "CARD_STATUS"."CHANGE_CUSTOMERS_CARTES"
       where "dt_change" >= date(now() - INTERVAL '1 DAY')::timestamp and
       ("Is_ch_BirthDate" = 1 or "Is_ch_Email" = 1 or "Is_ch_LastName" = 1 or "Is_ch_NoMobile" = 1) and 
       "NoMobile" is not null and "Email" !~* '.*creacard.*|.*prepaidfinancial.*|.*financial.*'
    ) as T2
    on T1."CardHolderID" = T2."CardHolderID"

    """

    data = pd.read_sql(query, con=engine)

    engine.close()

    # associtated new ids for identified cards
    if not data.empty:

        for var in ["FirstName", "LastName", "Address1", "Address2", "PostCode", "Email"]:
            data[var] = data[var].str.encode('utf-8').astype(str)
            data.loc[data[var].isnull(), var] = ""
            data[var] = data[var].str.strip(" ")
            data[var] = data[var].str.replace(" ", "")
            data[var] = data[var].str.lower()

        data = data[~data["Email"].str.contains('.*creacard.*|.*prepaidfinancial.*|.*financial.*', regex=True)]

        data["GoodEmail"] = 1
        data.loc[data["Email"].str.contains(condition_on_email, regex=True), "GoodEmail"] = 0

        data["GoodCombinaison"] = 1
        data.loc[(data["LastName"].str.contains(conditions["condition_combinaison"]["LastName"], regex=True)) |
                 (data["BirthDate"].isnull()) | (data["BirthDate"].isin(
            conditions["condition_combinaison"]["BirthDate"].split(","))), "GoodCombinaison"] = 0

        # Delete leading "00" at the start of string.

        data["NoMobile"] = data["NoMobile"].str.replace("^00", "", regex=True)

        # replace .0 at the end$

        data["NoMobile"] = data["NoMobile"].str.replace("\.0$", "", regex=True)

        # delete only literal '|' from string

        data["NoMobile"] = data["NoMobile"].str.replace("\|", "", regex=True)

        query = """

           DROP TABLE IF EXISTS "CUSTOMERS"."TMP_USER_ID" CASCADE

           """

        engine = connect_to_database("Postgres", connexion_postgres).CreateEngine()
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
               "GoodCombinaison" INTEGER,
               "MOBILE_ID" INTEGER,
               "USER_ID" INTEGER,
               "CONTACT_ID" VARCHAR(50),
               "PERSON_ID" INTEGER,
               "MOVIDON_ID" INTEGER
           )

           """.format(schema_main)

        engine = connect_to_database("Postgres", connexion_postgres).CreateEngine()
        engine.execute(query)
        engine.close()

        data = data[~data["NoMobile"].isnull()]
        data["MOBILE_ID"] = None
        data["USER_ID"] = None
        data["CONTACT_ID"] = None
        data["PERSON_ID"] = None
        data["MOVIDON_ID"] = None

        InsertTableIntoDatabase(data, TlbName="TMP_USER_ID", Schema=schema_main,
                                database_name=connexion_postgres,
                                database_type="Postgres",
                                DropTable=False,
                                InstertInParrell=False)

        # STEP 1 : handle new id mobile

        query = """


           update "CUSTOMERS"."TMP_USER_ID"
           set "MOBILE_ID" = T1."MOBILE_ID"
           from "CUSTOMERS"."ID_MOBILE" as T1
           where "CUSTOMERS"."TMP_USER_ID"."NoMobile" = T1."NoMobile"

           """

        engine = connect_to_database("Postgres", connexion_postgres).CreateEngine()
        engine.execute(query)

        query = """

           select "NoMobile", count(*) as "NUM_CARTES"
           from "CUSTOMERS"."TMP_USER_ID"
           where "MOBILE_ID" is null
           group by "NoMobile"

           """

        data = pd.read_sql(query, con=engine)

        engine.close()

        if not data.empty:
            # as the function serial was used to create the table i.e the ID
            # it is not necessary to specified it using python.
            # the function serial associates automatically and incrementally a
            # MOBILE_ID to a new inserted row
            InsertTableIntoDatabase(data, TlbName="ID_MOBILE", Schema='CUSTOMERS',
                                    database_name=connexion_postgres,
                                    database_type="Postgres",
                                    DropTable=False,
                                    InstertInParrell=False)

            query = """

               update "CUSTOMERS"."TMP_USER_ID"
               set "MOBILE_ID" = T1."MOBILE_ID"
               from "CUSTOMERS"."ID_MOBILE" as T1
               where "CUSTOMERS"."TMP_USER_ID"."NoMobile" = T1."NoMobile" and "CUSTOMERS"."TMP_USER_ID"."MOBILE_ID" is null


               """

            engine = connect_to_database("Postgres", connexion_postgres).CreateEngine()
            engine.execute(query)
            engine.close()

            # STEP 2 : handle new CONTACT_ID

        query = """
           update "CUSTOMERS"."TMP_USER_ID"
           set "CONTACT_ID" = "CardHolderID"
           """

        engine = connect_to_database("Postgres", connexion_postgres).CreateEngine()
        engine.execute(query)
        engine.close()

        # STEP 3 : handle new PERSON_ID

        query = """


           update "CUSTOMERS"."TMP_USER_ID"
           set "PERSON_ID" = T1."PERSON_ID"
           from "CUSTOMERS"."ID_PERSON" as T1
           where concat("CUSTOMERS"."TMP_USER_ID"."BirthDate", "CUSTOMERS"."TMP_USER_ID"."LastName") = T1."combinaison"
           and "CUSTOMERS"."TMP_USER_ID"."GoodCombinaison" = 1

           """

        engine = connect_to_database("Postgres", connexion_postgres).CreateEngine()
        engine.execute(query)

        query = """

           select distinct concat("BirthDate", "LastName") as "combinaison"
           from "CUSTOMERS"."TMP_USER_ID"
           where "PERSON_ID" is null and "GoodCombinaison" = 1


           """

        data = pd.read_sql(query, con=engine)

        engine.close()

    if not data.empty:
        # as the function serial was used to create the table i.e the ID
        # it is not necessary to specified it using python.
        # the function serial associates automatically and incrementally a
        # PERSON_ID to a new inserted row
        InsertTableIntoDatabase(data, TlbName="ID_PERSON", Schema='CUSTOMERS',
                                database_name=connexion_postgres,
                                database_type="Postgres",
                                DropTable=False,
                                InstertInParrell=False)

        query = """

           update "CUSTOMERS"."TMP_USER_ID"
           set "PERSON_ID" = T1."PERSON_ID"
           from "CUSTOMERS"."ID_PERSON" as T1
           where concat("CUSTOMERS"."TMP_USER_ID"."BirthDate", "CUSTOMERS"."TMP_USER_ID"."LastName") = T1."combinaison"
           and "CUSTOMERS"."TMP_USER_ID"."GoodCombinaison" = 1 and "CUSTOMERS"."TMP_USER_ID"."PERSON_ID" is null

           """

        engine = connect_to_database("Postgres", connexion_postgres).CreateEngine()
        engine.execute(query)
        engine.close()


    # STEP 4.2: identified cards that can be associated to a know USER_ID
    query = """
       UPDATE "CUSTOMERS"."TMP_USER_ID"
       set "USER_ID" = T1."USER_ID"
       from 
       (select "CardHolderID", "USER_ID"::integer
       from "CUSTOMERS"."MASTER_ID") as T1
       where "CUSTOMERS"."TMP_USER_ID"."CardHolderID" = T1."CardHolderID"
       """
    engine = connect_to_database("Postgres", connexion_postgres).CreateEngine()
    engine.execute(query)
    engine.close()

    query = """

       update "CUSTOMERS"."TMP_USER_ID"
       set "USER_ID" = T1."USER_ID"
       from "CUSTOMERS"."ID_USER" as T1
       where "CUSTOMERS"."TMP_USER_ID"."NoMobile" = T1."NoMobile" and
       "CUSTOMERS"."TMP_USER_ID"."USER_ID" is null

       """

    engine = connect_to_database("Postgres", connexion_postgres).CreateEngine()
    engine.execute(query)
    engine.close()

    query = """

       update "CUSTOMERS"."TMP_USER_ID"
       set "USER_ID" = T1."USER_ID"
       from "CUSTOMERS"."ID_USER" as T1
       where "CUSTOMERS"."TMP_USER_ID"."GoodEmail" = 1 and "CUSTOMERS"."TMP_USER_ID"."Email" = T1."Email" and
       "CUSTOMERS"."TMP_USER_ID"."USER_ID" is null

       """

    engine = connect_to_database("Postgres", connexion_postgres).CreateEngine()
    engine.execute(query)
    engine.close()

    query = """

       update "CUSTOMERS"."TMP_USER_ID"
       set "USER_ID" = T1."USER_ID"
       from "CUSTOMERS"."ID_USER" as T1
       where concat("CUSTOMERS"."TMP_USER_ID"."BirthDate", "CUSTOMERS"."TMP_USER_ID"."LastName") = T1."combinaison"
       and "CUSTOMERS"."TMP_USER_ID"."GoodCombinaison" = 1 and "CUSTOMERS"."TMP_USER_ID"."CONTACT_ID" is null

       """

    engine = connect_to_database("Postgres", connexion_postgres).CreateEngine()
    engine.execute(query)


    # STEP 4.4: Extract all cards (new cards and cards that were already associated to a USER_ID)
    # in order to re-associate USER_ID based on the sorted algorithm in order to always
    # be sure to keep the homogeneity and unicity of the USER_ID
    engine = connect_to_database("Postgres", connexion_postgres).CreateEngine()

    query = """

       select *
       from "CUSTOMERS"."TMP_USER_ID"


       """

    data = pd.read_sql(query, con=engine)
    data["combinaison"] = data["BirthDate"] + data["LastName"]
    data = data[["NoMobile", "Email", "combinaison", "GoodEmail", "GoodCombinaison", "USER_ID"]]

    query = """

    select "NoMobile", "Email", "combinaison", "GoodEmail", "GoodCombinaison", "USER_ID"
    from "CUSTOMERS"."ID_USER"


    """

    data_bis = pd.read_sql(query, con=engine)
    data_bis["USER_ID"] = data_bis["USER_ID"].astype(float)

    data = pd.concat([data, data_bis], axis=0)

    user_id = data[~data.duplicated(keep='first')]

    tic = time.time()
    sorted = False
    while sorted is False:

        tmp_user_id = user_id.groupby("NoMobile")["USER_ID"].min().reset_index()
        tmp_user_id.columns = ["NoMobile", "TMP_USER_ID"]
        user_id = pd.merge(user_id, tmp_user_id, on="NoMobile", how="inner")
        user_id["USER_ID"] = user_id["TMP_USER_ID"]
        user_id = user_id.drop(columns='TMP_USER_ID', axis=1)

        tmp_user_id = user_id[user_id["GoodEmail"] == 1].groupby("Email")["USER_ID"].min().reset_index()
        tmp_user_id.columns = ["Email", "TMP_USER_ID"]
        user_id = pd.merge(user_id, tmp_user_id, on="Email", how="left")
        user_id.loc[~user_id["TMP_USER_ID"].isnull(), "USER_ID"] = user_id["TMP_USER_ID"]
        user_id = user_id.drop(columns='TMP_USER_ID', axis=1)

        tmp_user_id = user_id[user_id["GoodCombinaison"] == 1].groupby("combinaison")["USER_ID"].min().reset_index()
        tmp_user_id.columns = ["combinaison", "TMP_USER_ID"]
        user_id = pd.merge(user_id, tmp_user_id, on="combinaison", how="left")
        user_id.loc[~user_id["TMP_USER_ID"].isnull(), "USER_ID"] = user_id["TMP_USER_ID"]
        user_id = user_id.drop(columns='TMP_USER_ID', axis=1)

        non_unique_num = user_id.groupby("NoMobile")["USER_ID"].nunique().sort_values().reset_index()
        non_unique_num = non_unique_num.loc[non_unique_num["USER_ID"] > 1, "NoMobile"]

        non_unique_email = user_id[user_id["GoodEmail"] == 1].groupby("Email")[
            "USER_ID"].nunique().sort_values().reset_index()
        non_unique_email = non_unique_email.loc[non_unique_email["USER_ID"] > 1, "Email"]

        non_unique_combi = user_id[user_id["GoodCombinaison"] == 1].groupby("combinaison")[
            "USER_ID"].nunique().sort_values().reset_index()
        non_unique_combi = non_unique_combi.loc[non_unique_combi["USER_ID"] > 1, "combinaison"]

        if (len(non_unique_num) > 0) or (len(non_unique_email) > 0) or (len(non_unique_combi) > 0):
            sorted = False
        else:
            sorted = True

    toc = time.time() - tic

    tmp_use_id = user_id[~user_id["USER_ID"].isnull()]

    # STEP 4.5: associate new user_id to cards that already haven't
    user_id = user_id[user_id["USER_ID"].isnull()]
    id_max = int(tmp_use_id["USER_ID"].max())
    user_id = compute_user_id(user_id, last_user_id=id_max)

    # STEP 4.6: Replace all ID_USER table by the new ones
    user_id = pd.concat([user_id, tmp_use_id], axis=0)
    user_id = user_id.reset_index(drop=True)

    query = """
    delete from "CUSTOMERS"."ID_USER"
    """

    engine = connect_to_database("Postgres", connexion_postgres).CreateEngine()

    engine.execute(query)
    engine.close()

    InsertTableIntoDatabase(user_id, TlbName="ID_USER", Schema='CUSTOMERS',
                            database_name=connexion_postgres,
                            database_type="Postgres",
                            DropTable=False,
                            InstertInParrell=False)

    # STEP 5: keep tracks of cards for which the user id changed

    # STEP 5.1: keep tracks of cards for which the user id changed

    query = """
    CREATE TABLE "CUSTOMERS"."TMP_MASTER_ID" as 
    SELECT "CardHolderID", "USER_ID"
    from "CUSTOMERS"."MASTER_ID"

    """
    engine = connect_to_database("Postgres", connexion_postgres).CreateEngine()
    engine.execute(query)
    engine.close()

    # STEP 5.1.1: delete cards already identified from MASTER_ID
    query = """
        delete from "CUSTOMERS"."MASTER_ID"
        where "CardHolderID" in (select T1."CardHolderID"
        from "CUSTOMERS"."TMP_USER_ID" as T1
        inner join "CUSTOMERS"."MASTER_ID" as T2
        ON T1."CardHolderID" = T2."CardHolderID")
        """

    engine = connect_to_database("Postgres", connexion_postgres).CreateEngine()

    engine.execute(query)
    engine.close()

    # STEP 5.2: add new cards to master ID
    engine = connect_to_database("Postgres", connexion_postgres).CreateEngine()
    query = """
    select *
    from "CUSTOMERS"."TMP_USER_ID"

    """
    data = pd.read_sql(query, con=engine)
    engine.close()

    InsertTableIntoDatabase(data, TlbName="MASTER_ID", Schema='CUSTOMERS',
                            database_name=connexion_postgres,
                            database_type="Postgres",
                            DropTable=False,
                            InstertInParrell=False)

    # STEP 5.3: update new user id
    query = """

    UPDATE "CUSTOMERS"."MASTER_ID"
    SET "USER_ID" = NULL
    """

    engine = connect_to_database("Postgres", connexion_postgres).CreateEngine()
    engine.execute(query)
    engine.close()

    query = """
        update "CUSTOMERS"."MASTER_ID"
        set "USER_ID" = T1."USER_ID"
        from(
        select distinct "NoMobile", "USER_ID"
        from "CUSTOMERS"."ID_USER") as T1
        where "CUSTOMERS"."MASTER_ID"."NoMobile" = T1."NoMobile"
    """

    engine = connect_to_database("Postgres", connexion_postgres).CreateEngine()
    engine.execute(query)
    engine.close()

    # STEP 5.4: store user id that changed
    query = """
    select T3.*
    from(

        select T1.*, T2."USER_ID" as "oth_user_id"
        from "CUSTOMERS"."MASTER_ID" as T1
        INNER JOIN "CUSTOMERS"."TMP_MASTER_ID" as T2
        on T1."CardHolderID" = T2."CardHolderID"

        ) as T3
    where T3."USER_ID" <> T3."oth_user_id"

    """

    engine = connect_to_database("Postgres", connexion_postgres).CreateEngine()
    data = pd.read_sql(query, con=engine)
    engine.close()

    data["USER_ID"] = data["oth_user_id"]

    data = data.drop(columns=["oth_user_id"], axis=1)

    data["dt_change"] = datetime.datetime.now() - datetime.timedelta(days=1)

    # insert these cards into the table that allows to track the change of ID
    # overtime
    InsertTableIntoDatabase(data, TlbName="CHANGE_IDS", Schema='CUSTOMERS',
                            database_name=connexion_postgres,
                            database_type="Postgres",
                            DropTable=False,
                            InstertInParrell=False)

    query = """
    drop table "CUSTOMERS"."TMP_MASTER_ID" cascade
    """
    engine = connect_to_database("Postgres", connexion_postgres).CreateEngine()
    engine.execute(query)
    engine.close()

    # STEP 5.5: associated person id for cards which haven't
    query = """
        update "CUSTOMERS"."MASTER_ID"
        set "PERSON_ID" = concat("USER_ID",'_',"MOBILE_ID")
        where "GoodCombinaison" = 0 and "PERSON_ID" is null 

    """
    engine = connect_to_database("Postgres", connexion_postgres).CreateEngine()
    engine.execute(query)
    engine.close()





