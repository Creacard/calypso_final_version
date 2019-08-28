
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
                CASE WHEN "TransactionTP" in ('POS International') then 1 
                else 0
                end as "IsPOSInternational", "TransactionTP",
                '' as "UNIVERS", '' as "SOUS_UNIVERS"
                FROM "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}"
                where "TransactionTP" IN ('POS International','POS Domestic') 
                and "DebitCredit" IN ('Debit') 
                and "TransactionResult" = 'APPROVED'
    
    
            """.format(ListDate[i])

            QueryFinal = QueryFinal + Query

        else:

            Query = """
    
                    UNION ALL
    
                     SELECT "CardHolderID","MCC","Amount","MerchantName","TransactionTime","Currency",
                     "CardVPUType", "MerchantAddress", "MerchantCity", "MerchantCountry", "MerchantID", "TransactionID",
                     CASE WHEN "TransactionTP" in ('POS International') then 1 
                     else 0
                     end as "IsPOSInternational", "TransactionTP",
                     '' as "UNIVERS", '' as "SOUS_UNIVERS"
                     FROM "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}"
                     where "TransactionTP" IN ('POS International','POS Domestic')
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
                where "TransactionTP" IN ('ATM Domestic','ATM International') 
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
                    where "TransactionTP" IN ('ATM Domestic','ATM International') 
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
                'ATM Domestic','ATM Domestic Fee','ATM International','ATM International Fee','BalanceInquiry fee',
                'Bank Payment fee','Bank Transfer Fee','Batch Load Fee','Card Fee','Card Load Fee','Card Load at Payzone Fee',
                'Card To Card Transfer Fee','Card to Card In','Cash Advance Fee','Decline Fee','Deposit To Card API Fee',
                'INTERNET DEBIT/CREDIT','IVR Fee','InternetDrCrFee','KYC Card Upgrade Fee','Monthly Fee','POS Domestic',
                'POS International','POS International Fee','Paytrail Load Fee','Post Office Fee','RefundFee','Replacement Card Fee',
                'Replacement Card In','SEPA Outgoing Payment Fee','SMS Balance Inquiry fee','SMS Fee','SMS Lock UnLock Fee',
                'Sepa Credit Fee','Sepa Incoming Payment','Sepa Incoming Payment Fee','Terminal Load','Terminal load fee',
                'Upgrade to Physical Fee','Voucher load','Voucher load fee')
                
                AND "TransactionTP" !~* ('auth')

    
            """.format(ListDate[i])

            QueryFinal = QueryFinal + Query

        else:

            Query = """
    
                    UNION ALL
    
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
                'ATM Domestic','ATM Domestic Fee','ATM International','ATM International Fee','BalanceInquiry fee',
                'Bank Payment fee','Bank Transfer Fee','Batch Load Fee','Card Fee','Card Load Fee','Card Load at Payzone Fee',
                'Card To Card Transfer Fee','Card to Card In','Cash Advance Fee','Decline Fee','Deposit To Card API Fee',
                'INTERNET DEBIT/CREDIT','IVR Fee','InternetDrCrFee','KYC Card Upgrade Fee','Monthly Fee','POS Domestic',
                'POS International','POS International Fee','Paytrail Load Fee','Post Office Fee','RefundFee','Replacement Card Fee',
                'Replacement Card In','SEPA Outgoing Payment Fee','SMS Balance Inquiry fee','SMS Fee','SMS Lock UnLock Fee',
                'Sepa Credit Fee','Sepa Incoming Payment','Sepa Incoming Payment Fee','Terminal Load','Terminal load fee',
                'Upgrade to Physical Fee','Voucher load','Voucher load fee')
                
                AND "TransactionTP" !~* ('auth')

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
                where "DebitCredit" IN ('Debit') and "TransactionTP" ~* 'fee' and "TransactionTP" !~* 'reversal'
                and "TransactionResult" = 'APPROVED'
    
    
            """.format(ListDate[i])

            QueryFinal = QueryFinal + Query

        else:

            Query = """
    
                    UNION ALL
    
                    SELECT "CardHolderID","MCC","Fee","Surcharge","TransactionTP","TransactionTime","Currency",
                    "CardVPUType", "MerchantAddress", "MerchantCity", "MerchantCountry", "MerchantID", "TransactionID"
                    FROM "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}"
                    where "DebitCredit" IN ('Debit') and "TransactionTP" ~* 'fee' and "TransactionTP" !~* 'reversal'
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
                AND "TransactionTP" IN ('Voucher load','Terminal Load','Sepa Incoming Payment','Card to Card In','INTERNET DEBIT/CREDIT')
    
    
            """.format(ListDate[i])

            QueryFinal = QueryFinal + Query

        else:

            Query = """
    
                    UNION ALL
    
                    SELECT "CardHolderID","MCC","Amount","TransactionTP","TransactionTime","Currency",
                    "CardVPUType", "MerchantAddress", "MerchantCity", "MerchantCountry", "MerchantID", "TransactionID"
                    FROM "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}"
                    WHERE "DebitCredit" IN ('Credit') and "TransactionResult" = 'APPROVED' 
                    AND "TransactionTP" IN ('Voucher load','Terminal Load','Sepa Incoming Payment','Card to Card In','INTERNET DEBIT/CREDIT')
    
    
                 """.format(ListDate[i])

            QueryFinal = QueryFinal + Query


    engine.execute(QueryFinal)
    engine.close()