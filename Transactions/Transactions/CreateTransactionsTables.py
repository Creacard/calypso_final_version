
def construct_transaction_tables(engine, ListDate, schema):


    # Drop table before
    query_d = """
    DROP TABLE IF EXISTS "{}"."POS_TRANSACTIONS"
    """.format(schema)

    engine.execute(query_d)

    QueryFinal = """
    Create table "{}"."POS_TRANSACTIONS"  as 
    """.format(schema)

    for i in range(0, len(ListDate)):

        if i == 0:

            Query = """
    
                SELECT "CardHolderID","MCC","Amount","MerchantName","TransactionTime","Currency",
                "CardVPUType", "MerchantAddress", "MerchantCity", "MerchantCountry", "MerchantID", "TransactionID",
                CASE WHEN "TransactionTP" in ('POS International', 'POS International Reversal') then 1 
                else 0
                end as "IsPOSInternational", "TransactionTP",
                '' as "UNIVERS", '' as "SOUS_UNIVERS"
                FROM "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}"
                where "TransactionTP" IN ('POS International','POS Domestic','PurchaseOnUs','POS Domestic Reversal','POS International Reversal') 
                and "DebitCredit" IN ('Debit') 
                and "TransactionResult" = 'APPROVED'
    
    
            """.format(ListDate[i])

            QueryFinal = QueryFinal + Query

        else:

            Query = """
    
                    UNION ALL
    
                     SELECT "CardHolderID","MCC","Amount","MerchantName","TransactionTime","Currency",
                     "CardVPUType", "MerchantAddress", "MerchantCity", "MerchantCountry", "MerchantID", "TransactionID",
                     CASE WHEN "TransactionTP" in ('POS International', 'POS International Reversal') then 1 
                     else 0
                     end as "IsPOSInternational", "TransactionTP",
                     '' as "UNIVERS", '' as "SOUS_UNIVERS"
                     FROM "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}"
                     where "TransactionTP" IN ('POS International','POS Domestic','PurchaseOnUs','POS Domestic Reversal','POS International Reversal')
                     and "DebitCredit" IN ('Debit') 
                     and "TransactionResult" = 'APPROVED'
    
    
                 """.format(ListDate[i])

            QueryFinal = QueryFinal + Query

    engine.execute(QueryFinal)

    # Create the index

    Query_index = """
        CREATE INDEX "MCC_index"
        ON "{}"."POS_TRANSACTIONS" USING btree
        ("MCC" bpchar_pattern_ops ASC NULLS LAST)
        TABLESPACE pg_default;
    """.format(schema)
    engine.execute(Query_index)

    # ATM

    query_d = """
    DROP TABLE IF EXISTS "{}"."ATM_TRANSACTIONS"
    """.format(schema)

    engine.execute(query_d)

    QueryFinal = """
    Create table "{}"."ATM_TRANSACTIONS"  as 
    
    """.format(schema)

    for i in range(0, len(ListDate)):

        if i == 0:

            Query = """
    
                SELECT "CardHolderID","MCC","Amount","MerchantName","TransactionTime","Currency",
                "CardVPUType", "MerchantAddress", "MerchantCity", "MerchantCountry", "MerchantID", "TransactionID",
                CASE WHEN "TransactionTP" in ('ATM International') then 1
                else 0
                end as "IsInternational", "TransactionTP"
                FROM "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}"
                where "TransactionTP" IN ('ATM Domestic','ATM International','ATM WITHDRAWAL-REVERSAL') 
                and "DebitCredit" IN ('Debit') 
                and "TransactionResult" = 'APPROVED'
    
    
            """.format(ListDate[i])

            QueryFinal = QueryFinal + Query

        else:

            Query = """
    
                    UNION ALL
    
                    SELECT "CardHolderID","MCC","Amount","MerchantName","TransactionTime","Currency",
                    "CardVPUType", "MerchantAddress", "MerchantCity", "MerchantCountry", "MerchantID", "TransactionID",
                    CASE WHEN "TransactionTP" in ('ATM International') then 1
                    else 0
                    end as "IsInternational", "TransactionTP"
                    FROM "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}"
                    where "TransactionTP" IN ('ATM Domestic','ATM International','ATM WITHDRAWAL-REVERSAL') 
                    and "DebitCredit" IN ('Debit') 
                    and "TransactionResult" = 'APPROVED'
    
    
                 """.format(ListDate[i])

            QueryFinal = QueryFinal + Query

    engine.execute(QueryFinal)

    # Others debit transactions

    query_d = """
    DROP TABLE IF EXISTS "{}"."OTHER_TRANSACTIONS"
    """.format(schema)

    engine.execute(query_d)

    QueryFinal = """
    Create table "{}"."OTHER_TRANSACTIONS"  as 
    
    """.format(schema)

    for i in range(0, len(ListDate)):

        if i == 0:

            Query = """
    
                SELECT "CardHolderID","MCC","Amount","MerchantName","TransactionTP","TransactionTime","Currency",
                "CardVPUType", "MerchantAddress", "MerchantCity", "MerchantCountry", "MerchantID", "TransactionID"
                FROM "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}"
                where "TransactionTP" IN ('SEPA Outgoing Payment','Representment Credit','Replacement Card Out','Replacement Card In',
                'MoneySend Inter Country','Merchant refunds','Merchandise Refund Hold Reversals','INTERNET DEBIT/CREDIT Refund',
                'INTERNET DEBIT/CREDIT','Expired Card Load','Debit Adjustments','Chargeback credit',
                'Cash Out','Cash Advance Int','Cash Advance','Card to Card Out','Card to Card In','ACP LOAD Debits') 
                and "DebitCredit" IN ('Debit') 
                and "TransactionResult" = 'APPROVED'
    
    
            """.format(ListDate[i])

            QueryFinal = QueryFinal + Query

        else:

            Query = """
    
                    UNION ALL
    
                    SELECT "CardHolderID","MCC","Amount","MerchantName","TransactionTP","TransactionTime","Currency",
                    "CardVPUType", "MerchantAddress", "MerchantCity", "MerchantCountry", "MerchantID", "TransactionID"
                    FROM "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}"
                    where "TransactionTP" IN ('SEPA Outgoing Payment','Representment Credit','Replacement Card Out','Replacement Card In',
                   'MoneySend Inter Country','Merchant refunds','Merchandise Refund Hold Reversals','INTERNET DEBIT/CREDIT Refund',
                   'INTERNET DEBIT/CREDIT','Expired Card Load','Debit Adjustments','Chargeback credit',
                   'Cash Out','Cash Advance Int','Cash Advance','Card to Card Out','Card to Card In','ACP LOAD Debits') 
                    and "DebitCredit" IN ('Debit') 
                    and "TransactionResult" = 'APPROVED'
    
    
                 """.format(ListDate[i])

            QueryFinal = QueryFinal + Query

    engine.execute(QueryFinal)

    # FEE



    QueryFinal = """
    Create table "{}"."FEES_TRANSACTIONS"  as 
    
    """.format(schema)

    for i in range(0, len(ListDate)):

        if i == 0:

            Query = """
    
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
                and "TransactionResult" = 'APPROVED'
    
    
            """.format(ListDate[i])

            QueryFinal = QueryFinal + Query

        else:

            Query = """
    
                    UNION ALL
    
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
                    and "TransactionResult" = 'APPROVED'
    
    
                 """.format(ListDate[i])

            QueryFinal = QueryFinal + Query

    engine.execute(QueryFinal)

    # Loads

    query_d = """
    DROP TABLE IF EXISTS "{}"."LOADS_TRANSACTIONS"
    """.format(schema)

    engine.execute(query_d)


    QueryFinal = """
    Create table "{}"."LOADS_TRANSACTIONS"  as 
    
    """.format(schema)

    for i in range(0, len(ListDate)):

        if i == 0:

            Query = """
    
                SELECT "CardHolderID","MCC","Amount","TransactionTP","TransactionTime","Currency",
                "CardVPUType", "MerchantAddress", "MerchantCity", "MerchantCountry", "MerchantID", "TransactionID"
                FROM "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}"
                WHERE "DebitCredit" IN ('Credit') and "TransactionResult" = 'APPROVED'
    
    
            """.format(ListDate[i])

            QueryFinal = QueryFinal + Query

        else:

            Query = """
    
                    UNION ALL
    
                    SELECT "CardHolderID","MCC","Amount","TransactionTP","TransactionTime","Currency",
                    "CardVPUType", "MerchantAddress", "MerchantCity", "MerchantCountry", "MerchantID", "TransactionID"
                    FROM "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}"
                    WHERE "DebitCredit" IN ('Credit') and "TransactionResult" = 'APPROVED'
    
    
                 """.format(ListDate[i])

            QueryFinal = QueryFinal + Query


    engine.execute(QueryFinal)
    engine.close()