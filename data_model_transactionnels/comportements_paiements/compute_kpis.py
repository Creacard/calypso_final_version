import pandas as pd
import time
import numpy as np
from Utils.ManipulateDate import ComputePeriodVector,compute_min_max_date


def compute_kpis_transactions(engine, start_date, end_date):

    _final_set = None

    ListDate = ComputePeriodVector(int(start_date.split("_")[0]), int(start_date.split("_")[1]),
                                   int(end_date.split("_")[0]), int(end_date.split("_")[1]))
    min_date, max_date = compute_min_max_date(int(start_date.split("_")[0]), int(start_date.split("_")[1]),
                                              int(end_date.split("_")[0]), int(end_date.split("_")[1]))

    tic = time.time()
    i = 1
    for _date_month in ListDate:

        ### Import Data ###
        _query = """
            SELECT distinct "CardHolderID"
            from "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}" as T1
        """.format(_date_month)
        if i ==1:
            _data_id = pd.read_sql(_query, con=engine)
            _final_set = _data_id
        else:
            _data_id = pd.read_sql(_query, con=engine)
            _final_set = pd.concat(
                [_final_set, _data_id[~_data_id.CardHolderID.isin(_final_set.CardHolderID)].reset_index(drop=True)],
                axis=0).reset_index(drop=True)

        i=i+1

    print("id unique computed in {} seconds".format(time.time() - tic))



    # compute ATM KPI's

    _query_atm = """
    
        select "CardHolderID",avg("Amount") as "avg_daily_amount_atm",count(*) as "frequency_atm",
        min("Amount") as "min_amount_atm",max("Amount") as "max_amount_atm",sum("Amount") as "sum_amount_atm",
        Date_part('days',Now() - max("TransactionTime")) as "recence_atm",
        max("TransactionTime") as "dernier_retrait",
        sum("IsInternational") as "international_frequency_atm"
        from "TRANSACTIONS"."ATM_TRANSACTIONS"
        where "TransactionTime" >= '{}' and "TransactionTime" <= '{}' and "Amount" > 0
        group by "CardHolderID"
        
                """.format(min_date, max_date)

    tic = time.time()

    _tmp = pd.read_sql(_query_atm, con=engine)
    print("Query done in {} seconds".format(time.time() - tic))
    _final_set = pd.merge(_final_set,_tmp,on='CardHolderID',how='left')
    _final_set = _final_set.fillna(0)
    _final_set["international_atm_part"] = np.divide(_final_set["international_frequency_atm"],_final_set["frequency_atm"])
    _final_set = _final_set.fillna(0)

    _query_atm_monthly = """
        select T1."CardHolderID",count(*) as "num_month_atm",avg(T1."sum_amount_atm") as "monthly_avg_amount_atm",
        avg(T1."frequency_atm") as "monthly_average_frequency_atm"
        from(
            select "CardHolderID",count(*) as "frequency_atm",sum("Amount") as "sum_amount_atm",
            EXTRACT(year from "TransactionTime") || '_' || EXTRACT(month from "TransactionTime") as "date_study"
            from "TRANSACTIONS"."ATM_TRANSACTIONS"
            where "TransactionTime" >= '{}' and "TransactionTime" <= '{}' and "Amount" > 0
            group by "CardHolderID",EXTRACT(year from "TransactionTime") || '_' || EXTRACT(month from "TransactionTime")
            ) as T1
        group by T1."CardHolderID"
                        """.format(min_date, max_date)

    tic = time.time()
    _tmp = pd.read_sql(_query_atm_monthly, con=engine)
    print("Query done in {} seconds".format(time.time() - tic))

    _final_set = pd.merge(_final_set,_tmp,on='CardHolderID',how='left')
    _final_set = _final_set.fillna(0)

    # compute LOAD KPI's

    _query_loads =  """
    
        select "CardHolderID",avg("Amount") as "avg_daily_amount_load",count(*) as "frequency_load",
        min("Amount") as "min_amount_load",max("Amount") as "max_amount_load",sum("Amount") as "sum_amount_load",
        Date_part('days',Now() - max("TransactionTime")) as "recence_load",
        max("TransactionTime") as "dernier_chargement"
        from "TRANSACTIONS"."LOADS_TRANSACTIONS"
        where "TransactionTime" >= '{}' and "TransactionTime" <= '{}' and "Amount" > 0
        group by "CardHolderID"
        
                    """.format(min_date, max_date)

    tic = time.time()
    _tmp = pd.read_sql(_query_loads, con=engine)
    print("Query done in {} seconds".format(time.time() - tic))

    _final_set = pd.merge(_final_set,_tmp,on='CardHolderID',how='left')
    _final_set = _final_set.fillna(0)

    _query_loads_monthly = """
    
            select T1."CardHolderID",count(*) as "num_month_load",avg(T1."sum_amount_load") as "monthly_avg_amount_load",
        avg(T1."frequency_load") as "monthly_average_frequency_load"
        from(
            select "CardHolderID",count(*) as "frequency_load",sum("Amount") as "sum_amount_load",
            EXTRACT(year from "TransactionTime") || '_' || EXTRACT(month from "TransactionTime") as "date_study"
            from "TRANSACTIONS"."LOADS_TRANSACTIONS"
            where "TransactionTime" >= '{}' and "TransactionTime" <= '{}' and "Amount" > 0
            group by "CardHolderID",EXTRACT(year from "TransactionTime") || '_' || EXTRACT(month from "TransactionTime")
            ) as T1
        group by T1."CardHolderID"
    
    """.format(min_date, max_date)

    tic = time.time()
    _tmp = pd.read_sql(_query_loads_monthly, con=engine)
    print("Query done in {} seconds".format(time.time() - tic))

    _final_set = pd.merge(_final_set,_tmp,on='CardHolderID',how='left')
    _final_set = _final_set.fillna(0)


    # Compute Other KPI's

    _query_other = """
    
        select "CardHolderID",avg("Amount") as "avg_daily_amount_other",count(*) as "frequency_other",
        min("Amount") as "min_amount_other",max("Amount") as "max_amount_othert",sum("Amount") as "sum_amount_other",
        Date_part('days',Now() - max("TransactionTime")) as "recence_other",
        max("TransactionTime") as "dernier_other"
        from "TRANSACTIONS"."OTHER_TRANSACTIONS"
        where "TransactionTime" >= '{}' and "TransactionTime" <= '{}' and "Amount" > 0
        group by "CardHolderID"
        
                  """.format(min_date,max_date)

    tic = time.time()
    _tmp = pd.read_sql(_query_other, con=engine)
    print("Query done in {} seconds".format(time.time() - tic))

    _final_set = pd.merge(_final_set,_tmp,on='CardHolderID',how='left')
    _final_set = _final_set.fillna(0)

    _query_monthly_other = """
    
            select T1."CardHolderID",count(*) as "num_month_other",avg(T1."sum_amount_other") as "monthly_avg_amount_other",
        avg(T1."frequency_other") as "monthly_average_frequency_other"
        from(
            select "CardHolderID",count(*) as "frequency_other",sum("Amount") as "sum_amount_other",
            EXTRACT(year from "TransactionTime") || '_' || EXTRACT(month from "TransactionTime") as "date_study"
            from "TRANSACTIONS"."LOADS_TRANSACTIONS"
            where "TransactionTime" >= '{}' and "TransactionTime" <= '{}' and "Amount" > 0
            group by "CardHolderID",EXTRACT(year from "TransactionTime") || '_' || EXTRACT(month from "TransactionTime")
            ) as T1
        group by T1."CardHolderID"
    
    """.format(min_date, max_date)

    tic = time.time()
    _tmp = pd.read_sql(_query_monthly_other, con=engine)
    print("Query done in {} seconds".format(time.time() - tic))

    _final_set = pd.merge(_final_set,_tmp,on='CardHolderID',how='left')
    _final_set = _final_set.fillna(0)


    # Pos KPI's

    _query_pos = """
    
     select "CardHolderID",avg("Amount") as "avg_daily_amount_pos",count(*) as "frequency_pos",
    min("Amount") as "min_amount_pos",max("Amount") as "max_amount_pos",sum("Amount") as "sum_amount_pos",
    Date_part('days',Now() - max("TransactionTime")) as "recence_pos",
    max("TransactionTime") as "dernier_achat",
    sum("IsPOSInternational") as "international_frequency_pos"
    from "TRANSACTIONS"."POS_TRANSACTIONS"
    where "TransactionTime" >= '{}' and "TransactionTime" <= '{}' and "Amount" > 0
    group by "CardHolderID"
    
    """.format(min_date, max_date)

    tic = time.time()
    _tmp = pd.read_sql(_query_pos, con=engine)
    print("Query done in {} seconds".format(time.time() - tic))
    _tmp = _tmp.fillna(0)
    _tmp["international_pos_part"] = np.divide(_tmp["international_frequency_pos"],_tmp["frequency_pos"])


    _final_set = pd.merge(_final_set,_tmp,on='CardHolderID',how='left')
    _final_set = _final_set.fillna(0)

    _query_pos_monthly = """
    
    
        select T1."CardHolderID",count(*) as "num_month_pos",avg(T1."sum_amount_pos") as "monthly_avg_amount_pos",
        avg(T1."frequency_pos") as "monthly_average_frequency_pos"
        from(
            select "CardHolderID",count(*) as "frequency_pos",sum("Amount") as "sum_amount_pos",
            EXTRACT(year from "TransactionTime") || '_' || EXTRACT(month from "TransactionTime") as "date_study"
            from "TRANSACTIONS"."POS_TRANSACTIONS"
            where "TransactionTime" >= '{}' and "TransactionTime" <= '{}' and "Amount" > 0
            group by "CardHolderID",EXTRACT(year from "TransactionTime") || '_' || EXTRACT(month from "TransactionTime")
            ) as T1
        group by T1."CardHolderID"
    
                    """.format(min_date, max_date)


    tic = time.time()
    _tmp = pd.read_sql(_query_pos_monthly, con=engine)
    print("Query done in {} seconds".format(time.time() - tic))


    _final_set = pd.merge(_final_set,_tmp,on='CardHolderID',how='left')
    _final_set = _final_set.fillna(0)

    # Fees kpi's
    _query_fees = """
    
            select "CardHolderID",(sum("Fee") + sum("Surcharge")) as "card_ca",
            max("TransactionTime") as "dernier_fees"
            from "TRANSACTIONS"."FEES_TRANSACTIONS"
            where "TransactionTime" >= '{}' and "TransactionTime" <= '{}'
            group by "CardHolderID"
        
                  """.format(min_date, max_date)

    tic = time.time()
    _tmp = pd.read_sql(_query_fees, con=engine)
    print("Query done in {} seconds".format(time.time() - tic))


    _final_set = pd.merge(_final_set,_tmp,on='CardHolderID',how='left')
    _final_set = _final_set.fillna(0)


    # Compute part of ATM, POS and OTHER
    _final_set["atm_part"] = np.divide(_final_set["sum_amount_atm"],(_final_set["sum_amount_atm"]+_final_set["sum_amount_pos"]+_final_set["sum_amount_other"]))
    _final_set["pos_part"] = np.divide(_final_set["sum_amount_pos"],(_final_set["sum_amount_atm"]+_final_set["sum_amount_pos"]+_final_set["sum_amount_other"]))
    _final_set["other_part"] = np.divide(_final_set["sum_amount_other"],(_final_set["sum_amount_atm"]+_final_set["sum_amount_pos"]+_final_set["sum_amount_other"]))

    _final_set = _final_set.fillna(0)
    _final_set = _final_set.replace([np.inf, -np.inf], 0)

    for var in ["dernier_retrait", "dernier_other", "dernier_chargement", "dernier_achat", "dernier_fees"]:
        _final_set.loc[_final_set[var] == 0, var] = pd.NaT
        _final_set[var] = pd.to_datetime(_final_set[var])

    _final_set["derniere_action"] = _final_set[["dernier_retrait","dernier_other","dernier_chargement","dernier_achat","dernier_fees"]].max(axis=1)

    _final_set["derniere_action_nom"] = _final_set[["dernier_retrait","dernier_other","dernier_chargement","dernier_achat","dernier_fees"]].idxmax(axis=1)

    _final_set.loc[_final_set["derniere_action_nom"].isna(), "derniere_action_nom"] = ""

    # Order the columns
    _final_set = _final_set.reindex_axis(sorted(_final_set.columns), axis=1)

    return _final_set


