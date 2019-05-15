
import pandas as pd
# Database connection
from BDDTools.DataBaseEngine import ConnectToPostgres
import time
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from POS_TRANSACTIONS_CATEGORISATION import RegexExcluded


user = 'justin'
pwd = 'Thekiller81'
hostname = '127.0.0.1'
port = '26026'
database = 'Creacard'

PostgresConnect = ConnectToPostgres(user=user, pwd=pwd, hostname=hostname, port=port, database=database)
PostgresConnect = PostgresConnect.CreateEngine()
engine = PostgresConnect._openConnection



# Step 1.1 - MCC code note 1 -- update categories and under categories
query = """
UPDATE "TRANSACTIONS"."POS_TRANSACTIONS"
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
"""
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
    RegexExcluded.ExcludedRegex(NumRow=i, DataRegex=DataRegex, engine=engine)

print("update of exclusion was done in {} seconds".format(time.time() - tic))

# Step 2.1 - MCC code note 0 and 2 -- update categories and under categories
query = """
UPDATE "TRANSACTIONS"."POS_TRANSACTIONS"
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
"""
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
    RegexExcluded.ExcludedRegex(NumRow=i, DataRegex=DataRegex, engine=engine)

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
for i in range(0,len(DataRegex)):
    print(i)
    RegexExcluded.IncludedRegex(NumRow=i, DataRegex=DataRegex, engine=engine)

print("update of exclusion was done in {} seconds".format(time.time() - tic))
