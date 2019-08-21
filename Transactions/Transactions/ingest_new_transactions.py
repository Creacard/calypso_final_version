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
    end_date = date_start + datetime.timedelta(days=1)

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
                        CASE WHEN "TransactionTP" in ('POS International', 'POS International Reversal') then 1 
                        else 0
                        end as "IsPOSInternational",
                        '' as "UNIVERS", '' as "SOUS_UNIVERS"
                        FROM "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}"
                        where "TransactionTP" IN ('POS International','POS Domestic','PurchaseOnUs','POS Domestic Reversal','POS International Reversal') 
                        and "DebitCredit" IN ('Debit') 
                        and "TransactionResult" = 'APPROVED' and "TransactionTime" >= '{}' and "TransactionTime" < '{}'
        
        """.format(str(date_start.year) + str(date_start.month),str(date_start),str(end_date))

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
    end_date = date_start + datetime.timedelta(days=1)

    engine = connect_to_database(database_type, database_name).CreateEngine()

    # check if the date had already treated
    query = """
       select count(*)
       from "{}"."{}"
       where "TransactionTime" >= '{}' and "TransactionTime" < '{}'
       """.format(_schema,_tlbname, str(date_start), str(end_date))

    data = pd.read_sql(query, con=engine)

    if data.iloc[0, 0] == 0:

        querytmp = """

                    SELECT "CardHolderID","MCC","Amount","MerchantName","TransactionTime","Currency",
                    "CardVPUType", "MerchantAddress", "MerchantCity", "MerchantCountry", "MerchantID", "TransactionID",
                    CASE WHEN "TransactionTP" in ('ATM International') then 1
                    else 0
                    end as "IsInternational"
                    FROM "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}"
                    where "TransactionTP" IN ('ATM Domestic','ATM International','ATM WITHDRAWAL-REVERSAL') 
                    and "DebitCredit" IN ('Debit') 
                    and "TransactionResult" = 'APPROVED' and "TransactionTime" >= '{}' and "TransactionTime" < '{}'

           """.format(str(date_start.year) + str(date_start.month), str(date_start), str(end_date))

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
    end_date = date_start + datetime.timedelta(days=1)

    engine = connect_to_database(database_type, database_name).CreateEngine()

    # check if the date had already treated
    query = """
       select count(*)
       from "{}"."{}"
       where "TransactionTime" >= '{}' and "TransactionTime" < '{}'
       """.format(_schema,_tlbname, str(date_start), str(end_date))

    data = pd.read_sql(query, con=engine)

    if data.iloc[0, 0] == 0:

        querytmp = """

                SELECT "CardHolderID","MCC","Amount","TransactionTP","TransactionTime","Currency",
                "CardVPUType", "MerchantAddress", "MerchantCity", "MerchantCountry", "MerchantID", "TransactionID"
                FROM "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}"
                WHERE "DebitCredit" IN ('Credit') and "TransactionResult" = 'APPROVED'
                and "TransactionTime" >= '{}' and "TransactionTime" < '{}'

           """.format(str(date_start.year) + str(date_start.month), str(date_start), str(end_date))

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
    end_date = date_start + datetime.timedelta(days=1)

    engine = connect_to_database(database_type, database_name).CreateEngine()

    # check if the date had already treated
    query = """
       select count(*)
       from "{}"."{}"
       where "TransactionTime" >= '{}' and "TransactionTime" < '{}'
       """.format(_schema,_tlbname, str(date_start), str(end_date))

    data = pd.read_sql(query, con=engine)

    if data.iloc[0, 0] == 0:

        querytmp = """

                SELECT "CardHolderID","MCC","Amount","MerchantName","TransactionTP","TransactionTime","Currency",
                "CardVPUType", "MerchantAddress", "MerchantCity", "MerchantCountry", "MerchantID", "TransactionID"
                FROM "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}"
                where "TransactionTP" IN ('SEPA Outgoing Payment','Representment Credit','Replacement Card Out','Replacement Card In',
                'MoneySend Inter Country','Merchant refunds','Merchandise Refund Hold Reversals','INTERNET DEBIT/CREDIT Refund',
                'INTERNET DEBIT/CREDIT','Expired Card Load','Debit Adjustments','Chargeback credit',
                'Cash Out','Cash Advance Int','Cash Advance','Card to Card Out','Card to Card In','ACP LOAD Debits') 
                and "DebitCredit" IN ('Debit') 
                and "TransactionResult" = 'APPROVED' and "TransactionTime" >= '{}' and "TransactionTime" < '{}'

           """.format(str(date_start.year) + str(date_start.month), str(date_start), str(end_date))

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
    end_date = date_start + datetime.timedelta(days=1)

    engine = connect_to_database(database_type, database_name).CreateEngine()

    # check if the date had already treated
    query = """
       select count(*)
       from "{}"."{}"
       where "TransactionTime" >= '{}' and "TransactionTime" < '{}'
       """.format(_schema,_tlbname, str(date_start), str(end_date))

    data = pd.read_sql(query, con=engine)

    if data.iloc[0, 0] == 0:

        querytmp = """

                SELECT "CardHolderID","MCC","Fee","Surcharge","TransactionTP","TransactionTime","Currency",
                "CardVPUType", "MerchantAddress", "MerchantCity", "MerchantCountry", "MerchantID", "TransactionID"
                FROM "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}"
                where "TransactionTP" NOT IN ('SEPA Outgoing Payment','Representment Credit','Replacement Card Out','Replacement Card In',
                'MoneySend Inter Country','Merchant refunds','Merchandise Refund Hold Reversals','INTERNET DEBIT/CREDIT Refund',
                'INTERNET DEBIT/CREDIT','Expired Card Load','Debit Adjustments','Chargeback credit',
                'Cash Out','Cash Advance Int','Cash Advance','Card to Card Out','Card to Card In','ACP LOAD Debits',
                'ATM Domestic','ATM International','ATM WITHDRAWAL-REVERSAL',
                'POS International','POS Domestic','PurchaseOnUs','POS Domestic Reversal','POS International Reversal') 
                and "DebitCredit" IN ('Debit') 
                and "TransactionResult" = 'APPROVED' and "TransactionTime" >= '{}' and "TransactionTime" < '{}'

           """.format(str(date_start.year) + str(date_start.month), str(date_start), str(end_date))

        query = """
           insert into "{}"."{}"
           {}
           """.format(_schema,_tlbname, querytmp)

        engine.execute(query)
        engine.close()

    else:
        print("this data had been already treated")