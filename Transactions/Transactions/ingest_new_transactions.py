from creacard_connectors.database_connector import connect_to_database
import datetime
import pandas as pd
from Postgres_Toolsbox.DbTools import CreateTable
from Postgres_Toolsbox.DetectTypePostgres import create_dictionnary_type_from_table
from Postgres_Toolsbox.Ingestion import InsertTableIntoDatabase
from POS_TRANSACTIONS_CATEGORISATION.LaunchRegexExcluded import fill_univers_sous_univers
import time


def add_new_pos_transactions(database_type, database_name, _year, _month, _day, **kwargs):



    _tlbname = kwargs.get('tlbname', "POS_TRANSACTIONS")
    _schema = kwargs.get('schema', "TRANSACTIONS")

    date_start = datetime.datetime(_year, _month, _day)
    date_start_cond = str(date_start)[0:10]
    end_date = date_start + datetime.timedelta(days=1)
    end_date = str(end_date)[0:10]

    engine = connect_to_database(database_type, database_name).CreateEngine()

    # check if the date had already treated
    query = """
    select count(*)
    from "{}"."{}"
    where "TransactionTime" >= '{}' and "TransactionTime" < '{}'
    """.format(_schema,_tlbname,str(date_start), str(end_date))

    data = pd.read_sql(query, con=engine)

    if data.iloc[0, 0] == 0:

        query = """
                        SELECT "CardHolderID","MCC","Amount","MerchantName","TransactionTime","Currency",
                        "CardVPUType", "MerchantAddress", "MerchantCity", "MerchantCountry", "MerchantID", "TransactionID",
                        CASE WHEN "TransactionTP" in ('POS International') then 1 
                        else 0
                        end as "IsPOSInternational",
                        '' as "UNIVERS", '' as "SOUS_UNIVERS"
                        FROM "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}"
                        where "TransactionTP" IN ('POS International','POS Domestic') 
                        and "DebitCredit" IN ('Debit') 
                        and "TransactionResult" = 'APPROVED' and "TransactionTime" >= '{}' and "TransactionTime" < '{}'
        
        """.format(str(date_start.year) + str(date_start.month), date_start_cond, end_date)

        data = pd.read_sql(query, con=engine)

        if not data.empty:

            # Get the type of each variables
            columns_type = create_dictionnary_type_from_table(engine,"POS_TRANSACTIONS")
            # Create the TMP table for POS TRANSACTIONS
            # Drop the table
            query = """
            DROP TABLE IF EXISTS "TMP_UPDATE"."TMP_POS_TRANSACTIONS"
            """

            engine.execute(query)

            CreateTable(engine, "TMP_POS_TRANSACTIONS", "TMP_UPDATE", columns_type,keep_order=True)

            # Insert into table
            InsertTableIntoDatabase(data,
                                    TlbName="TMP_POS_TRANSACTIONS",
                                    Schema="TMP_UPDATE",
                                    database_type=database_type,
                                    database_name=database_name,
                                    DropTable=False)

            tic =time.time()

            fill_univers_sous_univers(database_type, database_name, "TMP_UPDATE", "TMP_POS_TRANSACTIONS")

            print("categorisation was done in {} seconds".format(time.time() - tic))

            engine = connect_to_database(database_type, database_name).CreateEngine()

            query = """
            insert into "{}"."{}"
            select * from "TMP_UPDATE"."TMP_POS_TRANSACTIONS"
            """.format(_schema, _tlbname)

            engine.execute(query)

            # Drop the table
            query = """
            DROP TABLE IF EXISTS "TMP_UPDATE"."TMP_POS_TRANSACTIONS"
            """

            engine.execute(query)

            engine.close()
        else:
            print("Any data for this date")

    else:
        print("this data had been already treated")


def add_new_atm_transactions(database_type, database_name, _year, _month, _day, **kwargs):

    _tlbname = kwargs.get('tlbname', "ATM_TRANSACTIONS")
    _schema = kwargs.get('schema', "TRANSACTIONS")


    date_start = datetime.datetime(_year, _month, _day)
    date_start_cond = str(date_start)[0:10]
    end_date = date_start + datetime.timedelta(days=1)
    end_date = str(end_date)[0:10]

    engine = connect_to_database(database_type, database_name).CreateEngine()

    # check if the date had already treated
    query = """
       select count(*)
       from "{}"."{}"
       where "TransactionTime" >= '{}' and "TransactionTime" < '{}'
       """.format(_schema,_tlbname, date_start_cond, end_date)

    data = pd.read_sql(query, con=engine)

    if data.iloc[0, 0] == 0:

        querytmp = """

                    SELECT "CardHolderID","MCC","Amount","MerchantName","TransactionTime","Currency",
                    "CardVPUType", "MerchantAddress", "MerchantCity", "MerchantCountry", "MerchantID", "TransactionID",
                    CASE WHEN "TransactionTP" in ('ATM International') then 1
                    else 0
                    end as "IsInternational"
                    FROM "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}"
                    where "TransactionTP" IN ('ATM Domestic','ATM International') 
                    and "DebitCredit" IN ('Debit') 
                    and "TransactionResult" = 'APPROVED' and "TransactionTime" >= '{}' and "TransactionTime" < '{}'

           """.format(str(date_start.year) + str(date_start.month), date_start_cond, end_date)

        query = """
           insert into "{}"."{}"
           {}
           """.format(_schema,_tlbname, querytmp)

        engine.execute(query)
        engine.close()

    else:
        print("this data had been already treated")


def add_new_loads_transactions(database_type, database_name, _year, _month, _day, **kwargs):

    _tlbname = kwargs.get('tlbname', "LOADS_TRANSACTIONS")
    _schema = kwargs.get('schema', "TRANSACTIONS")

    date_start = datetime.datetime(_year, _month, _day)
    date_start_cond = str(date_start)[0:10]
    end_date = date_start + datetime.timedelta(days=1)
    end_date = str(end_date)[0:10]

    engine = connect_to_database(database_type, database_name).CreateEngine()

    # check if the date had already treated
    query = """
       select count(*)
       from "{}"."{}"
       where "TransactionTime" >= '{}' and "TransactionTime" < '{}'
       """.format(_schema,_tlbname, date_start_cond, end_date)

    data = pd.read_sql(query, con=engine)

    if data.iloc[0, 0] == 0:

        querytmp = """

                SELECT "CardHolderID","MCC","Amount","TransactionTP","TransactionTime","Currency",
                "CardVPUType", "MerchantAddress", "MerchantCity", "MerchantCountry", "MerchantID", "TransactionID"
                FROM "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}"
                WHERE "DebitCredit" IN ('Credit') and "TransactionResult" = 'APPROVED' 
                AND "TransactionTP" IN ('Voucher load','Terminal Load','Sepa Incoming Payment','Card to Card In','INTERNET DEBIT/CREDIT')
                and "TransactionTime" >= '{}' and "TransactionTime" < '{}'

           """.format(str(date_start.year) + str(date_start.month), date_start_cond, end_date)

        query = """
           insert into "{}"."{}"
           {}
           """.format(_schema, _tlbname, querytmp)

        engine.execute(query)
        engine.close()

    else:
        print("this data had been already treated")

def add_new_others_transactions(database_type, database_name, _year, _month, _day, **kwargs):

    _tlbname = kwargs.get('tlbname', "OTHER_TRANSACTIONS")
    _schema = kwargs.get('schema', "TRANSACTIONS")


    date_start = datetime.datetime(_year, _month, _day)
    date_start_cond = str(date_start)[0:10]
    end_date = date_start + datetime.timedelta(days=1)
    end_date = str(end_date)[0:10]

    engine = connect_to_database(database_type, database_name).CreateEngine()

    # check if the date had already treated
    query = """
       select count(*)
       from "{}"."{}"
       where "TransactionTime" >= '{}' and "TransactionTime" < '{}'
       """.format(_schema,_tlbname, date_start_cond, end_date)

    data = pd.read_sql(query, con=engine)

    if data.iloc[0, 0] == 0:

        querytmp = """

               select "CardHolderID","MCC","Amount","MerchantName","TransactionTP","TransactionTime","Currency",
               "CardVPUType", "MerchantAddress", "MerchantCity", "MerchantCountry", "MerchantID", "TransactionID",
               "DebitCredit",
               CASE 
                   WHEN "TransactionTP" ~* ('reversal') THEN 1
                   ELSE 0
               END AS "IsReversal",

               CASE 
                   WHEN "TransactionTP" ~* ('fee') THEN 1
                   ELSE 0
               END AS "IsFee"

               from "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}"

               WHERE "TransactionTP" NOT IN (
               'ATM Domestic','ATM Domestic Fee','ATM International','ATM International Fee','BalanceInquiry fee','FX Fee',
               'Bank Payment fee','Bank Transfer Fee','Batch Load Fee','Card Fee','Card Load Fee','Card Load at Payzone Fee',
               'Card To Card Transfer Fee','Card to Card In','Cash Advance Fee','Decline Fee','Deposit To Card API Fee',
               'INTERNET DEBIT/CREDIT','IVR Fee','InternetDrCrFee','KYC Card Upgrade Fee','Monthly Fee','POS Domestic',
               'POS International','POS International Fee','Paytrail Load Fee','Post Office Fee','RefundFee','Replacement Card Fee',
               'Replacement Card In','SEPA Outgoing Payment Fee','SMS Balance Inquiry fee','SMS Fee','SMS Lock UnLock Fee',
               'Sepa Credit Fee','Sepa Incoming Payment','Sepa Incoming Payment Fee','Terminal Load','Terminal load fee',
               'Upgrade to Physical Fee','Voucher load','Voucher load fee')

               AND "TransactionTP" !~* ('auth')
               and "TransactionTime" >= '{}' and "TransactionTime" < '{}'

           """.format(str(date_start.year) + str(date_start.month), date_start_cond, end_date)

        query = """
           insert into "{}"."{}"
           {}
           """.format(_schema,_tlbname, querytmp)

        engine.execute(query)
        engine.close()

    else:
        print("this data had been already treated")

def add_fees_others_transactions(database_type, database_name, _year, _month, _day, **kwargs):

    _tlbname = kwargs.get('tlbname', "FEES_TRANSACTIONS")
    _schema = kwargs.get('schema', "TRANSACTIONS")

    date_start = datetime.datetime(_year, _month, _day)
    date_start_cond = str(date_start)[0:10]
    end_date = date_start + datetime.timedelta(days=1)
    end_date = str(end_date)[0:10]

    engine = connect_to_database(database_type, database_name).CreateEngine()

    # check if the date had already treated
    query = """
       select count(*)
       from "{}"."{}"
       where "TransactionTime" >= '{}' and "TransactionTime" < '{}'
       """.format(_schema,_tlbname, date_start_cond, end_date)

    data = pd.read_sql(query, con=engine)

    if data.iloc[0, 0] == 0:

        querytmp = """

                SELECT "CardHolderID","MCC","Fee","Surcharge","TransactionTP","TransactionTime","Currency",
                "CardVPUType", "MerchantAddress", "MerchantCity", "MerchantCountry", "MerchantID", "TransactionID"
                FROM "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}"
                where "DebitCredit" IN ('Debit') and "TransactionTP" ~* 'fee' and "TransactionTP" !~* 'reversal'
                and "TransactionResul" = 'APPROVED' and "TransactionTime" >= '{}' and "TransactionTime" < '{}'

           """.format(str(date_start.year) + str(date_start.month), date_start_cond, end_date)

        query = """
           insert into "{}"."{}"
           {}
           """.format(_schema,_tlbname, querytmp)

        engine.execute(query)

        query_update = """
            update "TRANSACTIONS"."FEES_TRANSACTIONS"
            set "Surcharge" = ABS("Surcharge")
            where "TransactionTP" = 'FX Fee' and "Surcharge" < 0
        """

        engine.execute(query_update)


        engine.close()

    else:
        print("this data had been already treated")