""" Monthly datamodel """

# Author: Justin Valet <jv.datamail@gmail.com>
# Date: 15/11/2018
#

import pandas as pd
import numpy as np
from Creacard_Utils.ManipulateDate import ComputePeriodVector


def monthly_transactional_paiements(date_month, engine):

    """
        Parameters
        -----------
        date_month : str
            Date of the month that must be computed
            ex: 20178 --> for August, 2017
        engine : object
            sql alchemy connection database object
    """

    ### Import Data ###
    Query = """
        SELECT *
        from "TRANSACTIONS_MONTHLY"."MONTHLY_TRANSACTIONS_{}"
        where "CardHolderID" <> '0'
    """.format(date_month)

    Data = pd.read_sql(Query, con=engine)

    # Create conditions in order to take into account real credit,debit, fees & Surcharge
    CondFeeDebit = (Data["Amount"] >= 0) & (Data["DebitCredit"] == "Debit") & (Data["Fee"] > 0)
    CondRealDebit = (Data["Amount"] > 0) & (Data["DebitCredit"] == "Debit") & (Data["Fee"] == 0)
    CondRembFees = (Data["Amount"] == 0) & (Data["DebitCredit"] == "Credit")
    CondCredit = (Data["Amount"] > 0) & (Data["DebitCredit"] == "Credit")

    Data["jour_nom"] = Data.TransactionTime.dt.day_name()
    Data["jour_num"] = Data.TransactionTime.dt.day
    Data["heure_num"] = Data.TransactionTime.dt.hour

    # Initialize Day moment
    Data["jour_moment"] = ""
    Data.loc[(Data["heure_num"] >= 0) & (Data["heure_num"] < 5), "jour_moment"] = "nuit"
    Data.loc[(Data["heure_num"] >= 5) & (Data["heure_num"] <= 11), "jour_moment"] = "matin"
    Data.loc[(Data["heure_num"] >= 12) & (Data["heure_num"] <= 14), "jour_moment"] = "midi"
    Data.loc[(Data["heure_num"] > 14) & (Data["heure_num"] <= 19), "jour_moment"] = "apres_midi"
    Data.loc[(Data["heure_num"] > 19) & (Data["heure_num"] <= 23), "jour_moment"] = "soiree"

    # Initialze month moment
    Data["mois_moment"] = ""
    Data.loc[(Data["jour_num"] > 0) & (Data["jour_num"] <= 7), "mois_moment"] = "premiere_semaine"
    Data.loc[(Data["jour_num"] > 7) & (Data["jour_num"] <= 14), "mois_moment"] = "deuxieme_semaine"
    Data.loc[(Data["jour_num"] > 14) & (Data["jour_num"] <= 21), "mois_moment"] = "troisieme_semaine"
    Data.loc[(Data["jour_num"] > 21), "mois_moment"] = "quatrieme_semaine"

    # Initialize week-end / semaine
    Data["is_week_end"] = ""
    Data.loc[Data["jour_nom"].isin(["Saturday", "Sunday"]), "is_week_end"] = "week_end"
    Data.loc[~Data["jour_nom"].isin(["Saturday", "Sunday"]), "is_week_end"] = "semaine"

    # Create the unique set of CardHolderID for this month
    MonthSet = pd.DataFrame(Data.CardHolderID.unique(), columns=['CardHolderID'])

    # Sum of credit Amount
    MonthSet = pd.merge(MonthSet, pd.DataFrame(Data[CondCredit].groupby("CardHolderID")["Amount"].agg(
        [np.sum, np.size, np.max]).reset_index()).rename(
        columns={'CardHolderID': 'CardHolderID',
                 'sum': "amount_load",
                 'size': "frequency_load",
                 'amax': "max_load"})
                        , how='left', on=['CardHolderID', 'CardHolderID'])

    MonthSet = MonthSet.fillna(0)

    MonthSet = pd.merge(MonthSet, pd.DataFrame(Data[CondRealDebit].groupby("CardHolderID")["Amount"].agg(
        [np.sum, np.size, np.max]).reset_index()).rename(
        columns={'CardHolderID': 'CardHolderID',
                 'sum': "amount_debit",
                 'size': "frequency_debit",
                 'amax': "max_debit"})
                        , how='left', on=['CardHolderID', 'CardHolderID'])

    MonthSet = MonthSet.fillna(0)

    # creation of the following indicators:
    # part_debit_credit : part of debit into credit during the month
    # part_credit_begin : part of the credit at the begining of the month
    # part_debit_end : part of the debit at the end of the month
    # remaining_balance_end_month : remaining balance at the end of the month

    # part_debit_credit:
    MonthSet["part_debit_credit"] = np.divide(MonthSet["amount_debit"],
                                              (MonthSet["amount_load"] + MonthSet["amount_debit"]))
    MonthSet = MonthSet.fillna(0)

    # part_credit_begin
    MonthSet = pd.merge(MonthSet, pd.DataFrame(
        Data[(CondCredit) & (Data["mois_moment"] == "premiere_semaine")].groupby("CardHolderID")["Amount"].agg(
            [np.sum]).reset_index()).rename(columns={'CardHolderID': 'CardHolderID',
                                                     'sum': "amount_load_first"})
                        , how='left', on=['CardHolderID', 'CardHolderID'])

    MonthSet = MonthSet.fillna(0)
    MonthSet["part_credit_begin"] = np.divide(MonthSet.amount_load_first, MonthSet.amount_load)
    MonthSet = MonthSet.fillna(0)

    # part_debit_end
    MonthSet = pd.merge(MonthSet, pd.DataFrame(
        Data[(CondRealDebit) & (Data["mois_moment"] == "quatrieme_semaine")].groupby("CardHolderID")["Amount"].agg(
            [np.sum]).reset_index()).rename(columns={'CardHolderID': 'CardHolderID',
                                                     'sum': "amount_debit_end"})
                        , how='left', on=['CardHolderID', 'CardHolderID'])

    MonthSet = MonthSet.fillna(0)
    MonthSet["part_debit_end"] = np.divide(MonthSet.amount_debit_end, MonthSet.amount_debit)
    MonthSet = MonthSet.fillna(0)

    # remaining_balance_end_month
    # compute the vector of max date
    cond_max_date = Data.groupby(("CardHolderID"))["TransactionTime"].transform(max)
    cond_data = Data.loc[cond_max_date == Data["TransactionTime"],["CardHolderID", "RemainingBalance"]]
    del cond_max_date
    cond_data = cond_data.groupby("CardHolderID")["RemainingBalance"].mean().reset_index(drop=False)

    MonthSet = pd.merge(MonthSet, cond_data, how="inner", on="CardHolderID")
    MonthSet = MonthSet.rename(columns={"RemainingBalance": "remaining_balance_end_month"})

    for var in ["jour_moment", "mois_moment", "is_week_end"]:

        # Compute Debit for DayMoment
        for mom in np.unique(Data[var]):
            MonthSet = pd.merge(MonthSet,
                                pd.DataFrame(
                                    Data[(Data[var] == mom) & (CondRealDebit)].groupby(['CardHolderID'])[
                                        'Amount'].agg(
                                        [np.size]).reset_index()).rename(columns={'CardHolderID': 'CardHolderID',
                                                                                  'size': "frequence_debit_" + var + "_" + mom})
                                , how='left', on=['CardHolderID', 'CardHolderID'])
            MonthSet = MonthSet.fillna(0)

    for mom in ["frequence_debit_mois_moment_premiere_semaine", "frequence_debit_mois_moment_deuxieme_semaine",
                "frequence_debit_mois_moment_troisieme_semaine", "frequence_debit_mois_moment_quatrieme_semaine"]:
        if MonthSet.columns.isin([mom]).sum() < 1:
            MonthSet[mom] = 0

    MonthSet = MonthSet.drop(columns=["amount_load", "frequency_load", "max_load", "amount_debit",
                                      "frequency_debit", "max_debit", "amount_debit_end", "amount_load_first"], axis=1)

    MonthSet = MonthSet.reindex_axis(sorted(MonthSet.columns), axis=1)

    return MonthSet



def launch_monthly_kpis(start_date,end_date,engine):

    ListDate = ComputePeriodVector(int(start_date.split("_")[0]), int(start_date.split("_")[1]),
                                   int(end_date.split("_")[0]), int(end_date.split("_")[1]))

    i = 1
    if len(ListDate) > 1:
        for _date_ in ListDate:
            if i > 1:

                _tmp_data = monthly_transactional_paiements(_date_, engine)
                _tmp_data = _tmp_data.set_index("CardHolderID")
                _tmp_data["is_event"] = 1

                _tmp_ = pd.merge(_final_set, pd.DataFrame(_tmp_data["is_event"]), how="outer", left_index=True,
                                 right_index=True)
                _tmp_ = _tmp_.drop(columns="is_event", axis=1)
                _tmp_ = _tmp_.sort_index(axis=0)
                _tmp_ = _tmp_.reindex_axis(sorted(_tmp_.columns), axis=1)
                _tmp_ = _tmp_.fillna(0)

                #
                _tmp_data = pd.merge(_tmp_data, pd.DataFrame(_final_set["num_events"]), how="outer", left_index=True,
                                     right_index=True)
                _tmp_data = _tmp_data.drop(columns="num_events", axis=1)
                _tmp_data = _tmp_data.sort_index(axis=0)
                _tmp_data = _tmp_data.rename(columns={"is_event": "num_events"})
                _tmp_data = _tmp_data.reindex_axis(sorted(_tmp_.columns), axis=1)
                _tmp_data = _tmp_data.fillna(0)

                _final_set = _tmp_
                del _tmp_

                _final_set = _final_set + _tmp_data
                del _tmp_data

            else:
                # 1.1 - Initialization : Compute the first month and push "CardHolderID" as an index
                _final_set = monthly_transactional_paiements(_date_, engine)
                _final_set = _final_set.set_index("CardHolderID")
                # 1.2 - Initialization : Create the vector num of event
                _final_set["num_events"] = 1

            i = i + 1

    else:
        _final_set = monthly_transactional_paiements(ListDate[0], engine)
        _final_set = _final_set.set_index("CardHolderID")
        # 1.2 - Initialization : Create the vector num of event
        _final_set["num_events"] = 1


    return _final_set

def aggregate_monthly_kpis(engine,schema,table):

    _final_set = None

    query = """
    select "CardHolderID","frequence_debit_mois_moment_premiere_semaine", "frequence_debit_mois_moment_deuxieme_semaine",
                    "frequence_debit_mois_moment_troisieme_semaine", "frequence_debit_mois_moment_quatrieme_semaine"
    from "{}"."{}"
    """.format(schema,table)

    data = pd.read_sql(query, con=engine)
    data = data.set_index("CardHolderID")
    data_sum = pd.DataFrame(data.sum(axis=1), columns=["sum_frequency_mois_moment"])

    _final_set = np.divide(data, data_sum)
    _final_set = _final_set.fillna(0)
    _final_set["sum_frequency_mois_moment"] = data_sum["sum_frequency_mois_moment"]

    del data, data_sum

    query = """
    select 
    "CardHolderID",
    "frequence_debit_jour_moment_apres_midi",
    "frequence_debit_jour_moment_matin",
    "frequence_debit_jour_moment_midi",
    "frequence_debit_jour_moment_nuit",
    "frequence_debit_jour_moment_soiree"
    from "TRANSACTIONAL_DATAMODELS"."monthly_kpis"
    """

    data = pd.read_sql(query, con=engine)
    data = data.set_index("CardHolderID")
    data_sum = pd.DataFrame(data.sum(axis=1), columns=["sum_frequency_debit_jour"])

    _final_set = pd.concat([_final_set, np.divide(data, data_sum)], axis=1)
    _final_set = _final_set.fillna(0)
    _final_set["sum_frequency_debit_jour"] = data_sum["sum_frequency_debit_jour"]

    del data, data_sum

    query = """
    select 
    "CardHolderID",
    "frequence_debit_is_week_end_semaine",
    "frequence_debit_is_week_end_week_end"
    from "{}"."{}"
    """.format(schema,table)

    data = pd.read_sql(query, con=engine)
    data = data.set_index("CardHolderID")
    data_sum = pd.DataFrame(data.sum(axis=1), columns=["sum_frequency_is_week_end"])

    _final_set = pd.concat([_final_set, np.divide(data, data_sum)], axis=1)
    _final_set = _final_set.fillna(0)
    _final_set["sum_frequency_is_week_end"] = data_sum["sum_frequency_is_week_end"]

    del data, data_sum

    query = """
    select "CardHolderID",
    ("part_credit_begin" / "num_events") as "avg_part_credit_begin",
    ("part_debit_credit" / "num_events") as "avg_part_debit_credit",
    ("part_debit_end" / "num_events") as "avg_part_debit_end",
    ("remaining_balance_end_month" / "num_events") as "avg_remaining_balance_end_month"
    FROM "{}"."{}"
    """.format(schema,table)

    data = pd.read_sql(query, con=engine)
    data = data.set_index("CardHolderID")
    _final_set = pd.concat([_final_set, data], axis=1)
    _final_set = _final_set.fillna(0)

    del data

    return _final_set