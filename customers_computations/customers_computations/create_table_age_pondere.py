from Creacard_Utils.ManipulateDate import compute_min_max_date
import datetime

def compute_age_pondere(engine,**kwargs):

    """

    :param engine:
    :param kwargs: period is string (start: "YYYY_MM" "," end: "YYYY_MM") (exemple: "2017_9_2018_9")
    :return:
    """

    period = kwargs.get('period', None)
    add_period_to_table = kwargs.get('add_period_to_table', False)

    if period is not None:

        min_date, max_date = compute_min_max_date(int(period.split("_")[0]), int(period.split("_")[1]),
                                                  int(period.split("_")[2]), int(period.split("_")[3]))

        max_date = datetime.datetime(int(max_date.split("-")[0]),
                                     int(max_date.split("-")[1]),
                                     int(max_date.split("-")[2])) + datetime.timedelta(days=1)
        _add_period = """
        and "TransactionTime" >= '{}' and "TransactionTime" <= '{}' """.format(str(min_date),str(max_date))

        if add_period_to_table:
            _add_period_table = "_" + str(period[0]) + "_" + str(period[1])
        else:
            _add_period_table = ""

    else:
        _add_period = ""
        _add_period_table = ""

    query = """ DROP TABLE IF EXISTS "CARD_STATUS"."card_age_pondere{}" """.format(_add_period_table)
    engine.execute(query)


    query = """
    
    Create Table "CARD_STATUS"."card_age_pondere{}" as 
    
       select T76."CardHolderID",
        sum(T76."age") as "age_pondere"
        FROM(
            select T1."CardHolderID",
            (T1."consumption" / T2."total_consumption") * (T1."annee" - T2."annee_naissance") as "age"
            from (
                SELECT "CardHolderID","annee",sum("consumption") as "consumption"
                FROM (
                    select "CardHolderID",
                    EXTRACT(year from "TransactionTime") as "annee", 
                    sum("Amount") as "consumption"
                    from "TRANSACTIONS"."POS_TRANSACTIONS" 
                    where "Amount" >= 0 {}
                    group by EXTRACT(year from "TransactionTime"),"CardHolderID"
    
                    UNION ALL 
    
                    select "CardHolderID",
                    EXTRACT(year from "TransactionTime") as "annee", 
                    sum("Amount") as "consumption"
                    from "TRANSACTIONS"."ATM_TRANSACTIONS" 
                    where "Amount" >= 0 {}
                    group by EXTRACT(year from "TransactionTime"),"CardHolderID"
    
                    UNION ALL 
    
                    select "CardHolderID",
                    EXTRACT(year from "TransactionTime") as "annee", 
                    sum("Amount") as "consumption"
                    from "TRANSACTIONS"."OTHER_TRANSACTIONS" 
                    where "Amount" >= 0 and "DebitCredit" = 'Debit'
                    and "IsReversal" = 0 and "IsFee" = 0 {}
                    group by EXTRACT(year from "TransactionTime"),"CardHolderID"
    
                    ) as T4
                GROUP BY "CardHolderID","annee"
                ORDER BY "CardHolderID", "annee"
            ) as T1
            JOIN(
                   SELECT T3."CardHolderID", sum(T3."total_consumption") as "total_consumption",
                    T3."annee_naissance"
                    FROM (
                    select T1."CardHolderID", sum(T1."Amount") as "total_consumption",
                    EXTRACT(year from T2."BirthDate") as "annee_naissance"
                    from "TRANSACTIONS"."POS_TRANSACTIONS" as T1
                    
    
                    inner join "CARD_STATUS"."STATUS_CARTES" as T2
                    on T1."CardHolderID" = T2."CardHolderID"
    
                    where T1."Amount" > 0 and 
                    T2."BirthDate" is not null and T2."KYC_Status" in ('KYC','KYC LITE') and 
                    T2."BirthDate" not in ('1988-08-08 00:00:00','1989-09-09 00:00:00','1992-02-02 00:00:00','1930-06-11 00:00:00') and
                    T2."BirthDate" > '1940-01-01 00:00:00'
                    {}
                    group by T1."CardHolderID",EXTRACT(year from T2."BirthDate")
    
                    UNION ALL 
    
                    select T1."CardHolderID", sum(T1."Amount") as "total_consumption",
                    EXTRACT(year from T2."BirthDate") as "annee_naissance"
                    from "TRANSACTIONS"."ATM_TRANSACTIONS" as T1
    
    
                    inner join "CARD_STATUS"."STATUS_CARTES" as T2
                    on T1."CardHolderID" = T2."CardHolderID"
    
                    where T1."Amount" > 0 and 
                    T2."BirthDate" is not null and T2."KYC_Status" in ('KYC','KYC LITE') and 
                    T2."BirthDate" not in ('1988-08-08 00:00:00','1989-09-09 00:00:00','1992-02-02 00:00:00','1930-06-11 00:00:00') and  
                    T2."BirthDate" > '1940-01-01 00:00:00'
                    {}
                    group by T1."CardHolderID",EXTRACT(year from T2."BirthDate")
    
                    UNION ALL
    
                    select T1."CardHolderID", sum(T1."Amount") as "total_consumption",
                    EXTRACT(year from T2."BirthDate") as "annee_naissance"
                    from "TRANSACTIONS"."OTHER_TRANSACTIONS" as T1
    
    
                    inner join "CARD_STATUS"."STATUS_CARTES" as T2
                    on T1."CardHolderID" = T2."CardHolderID"
    
                    where T1."Amount" > 0 and 
                    T2."BirthDate" is not null and T2."KYC_Status" in ('KYC','KYC LITE') and 
                    T2."BirthDate" not in ('1988-08-08 00:00:00','1989-09-09 00:00:00','1992-02-02 00:00:00','1930-06-11 00:00:00') and 
                    T2."BirthDate" > '1940-01-01 00:00:00' and "DebitCredit" = 'Debit'
                    and "IsReversal" = 0 and "IsFee" = 0 {}
                    group by T1."CardHolderID",EXTRACT(year from T2."BirthDate")
    
                    ) as T3
    
                GROUP BY T3."CardHolderID", T3."annee_naissance"
            ) as T2
            ON T1."CardHolderID" = T2."CardHolderID"
        ) as T76
        group by T76."CardHolderID"
    
    """.format(_add_period_table, _add_period,_add_period,_add_period,_add_period,_add_period,_add_period)

    engine.execute(query)
    engine.close()
