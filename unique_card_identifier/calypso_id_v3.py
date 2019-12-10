import pandas as pd
from creacard_connectors.database_connector import connect_to_database
import time
from Postgres_Toolsbox.Ingestion import InsertTableIntoDatabase

engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()


query = """
	select distinct T5."Email",T5."NoMobile",T5."combinaison",T5."GoodCombinaison",T5."GoodEmail", count(*)
	from(
	select T3.*, T4."num_nomobile"
	from(
		select T1."CardHolderID", T2."num_email",T1."NoMobile",
		T1."Email",concat(T1."BirthDate",T1."LastName") as "combinaison",
		T1."GoodCombinaison",T1."GoodEmail"
		from "CUSTOMERS"."MASTER_ID" as T1
		left join(
		select count(*) as "num_email", "Email"
		from "CUSTOMERS"."MASTER_ID"
		where "Email" is not null 
		group by "Email"
			) as T2
		ON T1."Email" = T2."Email"
		) as T3
	Left join (
		select count(*) as "num_nomobile", "NoMobile"
		from "CUSTOMERS"."MASTER_ID"
		where "NoMobile" is not null
		group by "NoMobile"
	) as T4
	ON T3."NoMobile" = T4."NoMobile"
	where (T3."num_email" > 1 and T4."num_nomobile" = 1) 
	or (T3."num_email" = 1 and T4."num_nomobile" > 1) or 
	(T3."num_email" is null and T4."num_nomobile" > 1) 
	or (T3."num_email" > 1 and T4."num_nomobile" > 1)) as T5
	group by T5."Email",T5."NoMobile",T5."combinaison",T5."GoodCombinaison", T5."GoodEmail"
	order by 6 DESC
"""

data = pd.read_sql(query, con=engine)

data = data.loc[:, ["NoMobile", "Email", "combinaison", "GoodCombinaison", "GoodEmail"]]

engine.close()



calypso_id = 1
user_id = None

tmp = pd.DataFrame(data.iloc[0,:]).T


tmp = data[((data["NoMobile"] == tmp["NoMobile"][0]) | ((data["Email"] == tmp["Email"][0]) & tmp["GoodEmail"][0] == 1) |
            ((data["combinaison"] == tmp["combinaison"][0]) & tmp["GoodCombinaison"][0] == 1))]
tmp["USER_ID"] = calypso_id
user_id = pd.DataFrame(tmp.copy())

data = data[~data.index.isin(tmp.index)]
data = data.reset_index(drop=True)

tmp = None
calypso_id = calypso_id + 1




tic = time.time()

not_finish = True
while not_finish:


    tmp = pd.DataFrame(data.iloc[0, :]).T

    tmp = data.loc[((data["NoMobile"] == tmp["NoMobile"][0]) | ((data["Email"] == tmp["Email"][0]) & tmp["GoodEmail"][0] == 1) | (
            (data["combinaison"] == tmp["combinaison"][0]) & tmp["GoodCombinaison"][0] == 1)), ["NoMobile", "Email",
                                                                                                "combinaison",
                                                                                                "GoodCombinaison","GoodEmail"]]
    tmp["USER_ID"] = None

    tmp_id = None
    tmp_list = None

    unique_num = list(tmp["NoMobile"].unique())
    unique_mail = list(tmp.loc[tmp["GoodEmail"] == 1, "Email"].unique())
    unique_combi = list(tmp.loc[tmp["GoodCombinaison"] == 1, "combinaison"].unique())

    tmp_list = list(user_id.loc[(user_id["NoMobile"].isin(unique_num)) | (user_id["Email"].isin(unique_mail)) | (
        user_id["combinaison"].isin(unique_combi)), "USER_ID"].unique())

    if tmp_list:
        tmp_id = min(tmp_list)

    if tmp_id:
        tmp["USER_ID"] = tmp_id
        user_id = pd.concat([user_id, tmp], axis=0)

        data = data[~data.index.isin(tmp.index)]

        tmp = None

    else:
        tmp["USER_ID"] = calypso_id
        user_id = pd.concat([user_id, tmp], axis=0)
        data = data[~data.index.isin(tmp.index)]

        tmp = None
        calypso_id = calypso_id + 1

    data = data.reset_index(drop=True)

    if data.empty:
        not_finish = False

toc = time.time() - tic



# re-assigned non unique phone number
non_unique_num = user_id.groupby("NoMobile")["USER_ID"].nunique().sort_values().reset_index()
non_unique_num = non_unique_num.loc[non_unique_num["USER_ID"] > 1, "NoMobile"]


tic = time.time()
for i in range(0, len(non_unique_num)):
    user_id.loc[user_id["NoMobile"] ==non_unique_num.iloc[i], "USER_ID"] = user_id.loc[user_id["NoMobile"] ==non_unique_num.iloc[i], "USER_ID"].min()
toc = time.time() - tic


# re-assigned non unique phone number
non_unique_email = user_id[user_id["GoodEmail"] == 1].groupby("Email")["USER_ID"].nunique().sort_values().reset_index()
non_unique_email = non_unique_email.loc[non_unique_email["USER_ID"] > 1, "Email"]

tic = time.time()
for i in range(0, len(non_unique_email)):
    user_id_unique = list(user_id.loc[user_id["Email"] ==non_unique_email.iloc[i], "USER_ID"].unique())
    user_id.loc[user_id["USER_ID"].isin(user_id_unique), "USER_ID"] = min(user_id_unique)
toc = time.time() - tic


# re-assigned non unique combinaison
non_unique_combi = user_id[user_id["GoodCombinaison"] == 1].groupby("combinaison")["USER_ID"].nunique().sort_values().reset_index()
non_unique_combi = non_unique_combi.loc[non_unique_combi["USER_ID"] > 1, "combinaison"]



tic = time.time()
for i in range(0, len(non_unique_combi)):
    user_id_unique = list(user_id.loc[user_id["combinaison"] ==non_unique_combi.iloc[i], "USER_ID"].unique())
    user_id.loc[user_id["USER_ID"].isin(user_id_unique), "USER_ID"] = min(user_id_unique)
toc = time.time() - tic

user_id = user_id.reset_index(drop=True)


query = """


CREATE TABLE "CUSTOMERS"."ID_USER"(

	"NoMobile" TEXT,
	"Email" TEXT,
	"combinaison" TEXT,
	"GoodCombinaison" INTEGER,
	"GoodEmail" INTEGER,
	"USER_ID" INTEGER
)

"""

engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()
engine.execute(query)
engine.close()



InsertTableIntoDatabase(user_id, TlbName="ID_USER", Schema='CUSTOMERS',
                        database_name="Creacard_Calypso",
                        database_type="Postgres",
                        DropTable=False,
                        InstertInParrell=False)



engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()

query = """


UPDATE "CUSTOMERS"."MASTER_ID"
SET "USER_ID" = T1."USER_ID"
FROM "CUSTOMERS"."ID_USER" as T1
WHERE "CUSTOMERS"."MASTER_ID"."NoMobile" = T1."NoMobile" and "CUSTOMERS"."MASTER_ID"."USER_ID" is null

"""

engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()
engine.execute(query)
engine.close()

query = """

UPDATE "CUSTOMERS"."MASTER_ID"
SET "USER_ID" = T1."USER_ID"
FROM "CUSTOMERS"."ID_USER" as T1
WHERE concat("CUSTOMERS"."MASTER_ID"."BirthDate","CUSTOMERS"."MASTER_ID"."LastName") = T1."Email" 
and T1."GoodEmail" = 1 and "CUSTOMERS"."MASTER_ID"."USER_ID" is null
"""

engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()
engine.execute(query)
engine.close()

query = """

UPDATE "CUSTOMERS"."MASTER_ID"
SET "USER_ID" = T1."USER_ID"
FROM "CUSTOMERS"."ID_USER" as T1
WHERE concat("CUSTOMERS"."MASTER_ID"."BirthDate","CUSTOMERS"."MASTER_ID"."LastName") = T1."combinaison" 
and T1."GoodCombinaison" = 1 and "CUSTOMERS"."MASTER_ID"."USER_ID" is null

"""

engine = connect_to_database("Postgres", "Creacard_Calypso").CreateEngine()
engine.execute(query)
engine.close()



query = """

select distinct T5."Email",T5."NoMobile",T5."combinaison",T5."GoodCombinaison",count(*)
from(
select T3.*, T4."num_nomobile"
from(
	select T1."CardHolderID", T2."num_email",T1."NoMobile",T1."Email",concat(T1."BirthDate",T1."LastName") as "combinaison",
	CASE WHEN T1."LastName" not in ('pcs','prepaid','cashservices','chrome') and "BirthDate" not in ('1988-08-08 00:00:00') then 1
	else 0
	end as "GoodCombinaison"
	from "CUSTOMERS"."MASTER_ID" as T1
	left join(
	select count(*) as "num_email", "Email"
	from "CUSTOMERS"."MASTER_ID"
    where "Email" is not null 
	group by "Email"
		) as T2
	ON T1."Email" = T2."Email"
	) as T3
Left join (
	select count(*) as "num_nomobile", "NoMobile"
	from "CUSTOMERS"."MASTER_ID"
	where "NoMobile" is not null
	group by "NoMobile"
) as T4
ON T3."NoMobile" = T4."NoMobile"
where (T3."num_email" is null and T4."num_nomobile" = 1) 
or (T3."num_email" = 1 and T4."num_nomobile" = 1) or 
(T3."num_email" = 1 and T4."num_nomobile" is null)) as T5
where T5."Email" not in ('1@2.com') and T5."Email" !~* '.*pcs.*|.*creacard.*'
group by T5."Email",T5."NoMobile",T5."combinaison",T5."GoodCombinaison"
order by 6 DESC


"""

data = pd.read_sql(query, con=engine)


# extract max of user ID
query = """
select max("USER_ID")
from "CUSTOMERS"."ID_USER"
"""
max_id_user = pd.read_sql(query, con=engine)

#

unique_combinaison = data[data["GoodCombinaison"] == 1].groupby("combinaison").size().sort_values().reset_index()
unique_combinaison.columns = ["combinaison","count"]

last_user_id = max_id_user.iloc[0] + 1 + len(unique_combinaison)
id_user = pd.DataFrame(range(max_id_user.iloc[0]+ 1, last_user_id))
unique_combinaison["USER_ID"] = id_user


ii = pd.merge(data, unique_combinaison[["combinaison","USER_ID"]], on="combinaison", how="left")

null_data = ii[ii["USER_ID"].isnull()].reset_index(drop=True)
ii = ii[~ii["USER_ID"].isnull()]
ii = ii.reset_index(drop=True)

num_null = null_data.shape[0]
last_user_id = ii["USER_ID"].max() + 1 + num_null
id_user = pd.DataFrame(range(int(ii["USER_ID"].max()) + 1, int(last_user_id)))
null_data["USER_ID"] = id_user

ii = pd.concat([ii, null_data], axis=0)
ii = ii.reset_index(drop=True)
ii = ii.drop(columns="count", axis=1)


InsertTableIntoDatabase(ii, TlbName="ID_USER", Schema='CUSTOMERS',
                        database_name="Creacard_Calypso",
                        database_type="Postgres",
                        DropTable=False,
                        InstertInParrell=False)



query = """

select distinct T3.*, T4."USER_ID"
from(
	select distinct T1.*,concat(T1."BirthDate", T1."LastName") as "combinaison"
	from "CUSTOMERS"."MASTER_ID" as T1
	left join "CUSTOMERS"."MASTER_ID_BIS" as T2
	ON T1."CardHolderID" = T2."CardHolderID" 
	where T2."USER_ID" is null and T1."LastName" not in ('pcs','prepaid','cashservices','chrome') and T1."BirthDate" not in ('1988-08-08 00:00:00')
	) as T3
inner join "CUSTOMERS"."ID_USER" as T4
on T3."NoMobile" = T4."NoMobile"


"""

data = pd.read_sql(query, con=engine)

data = data.drop(columns="combinaison", axis=1)

InsertTableIntoDatabase(data, TlbName="MASTER_ID_BIS", Schema='CUSTOMERS',
                        database_name="Creacard_Calypso",
                        database_type="Postgres",
                        DropTable=False,
                        InstertInParrell=False)


query = """

select distinct T3.*, T4."USER_ID"
from(
	select distinct T1.*,concat(T1."BirthDate", T1."LastName") as "combinaison"
	from "CUSTOMERS"."MASTER_ID" as T1
	left join "CUSTOMERS"."MASTER_ID_BIS" as T2
	ON T1."CardHolderID" = T2."CardHolderID" 
	where T2."USER_ID" is null and T1."LastName" not in ('pcs','prepaid','cashservices','chrome') and T1."BirthDate" not in ('1988-08-08 00:00:00')
	) as T3
inner join "CUSTOMERS"."ID_USER" as T4
on T3."combinaison" = T4."combinaison"

"""


data = pd.read_sql(query, con=engine)

data = data.drop(columns="combinaison", axis=1)

InsertTableIntoDatabase(data, TlbName="MASTER_ID_BIS", Schema='CUSTOMERS',
                        database_name="Creacard_Calypso",
                        database_type="Postgres",
                        DropTable=False,
                        InstertInParrell=False)










