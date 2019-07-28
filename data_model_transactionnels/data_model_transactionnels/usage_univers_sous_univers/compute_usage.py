import time
import pandas as pd
import numpy as np
from Creacard_Utils.ManipulateDate import compute_min_max_date

def compute_usage_details(engine,_aggregate_metric,choose_metric,start_date, end_date):

    min_date, max_date = compute_min_max_date(int(start_date.split("_")[0]), int(start_date.split("_")[1]),
                                              int(end_date.split("_")[0]), int(end_date.split("_")[1]))

    if choose_metric == 'frequency':

        _query = """
            SELECT "CardHolderID","{}",count(*) as "{}"
            FROM "TRANSACTIONS"."POS_TRANSACTIONS"
            where "Amount" > 0 and "{}" <> '' and "TransactionTime" >= '{}' and "TransactionTime" <= '{}'
            GROUP BY "CardHolderID","{}" 
                 """.format(_aggregate_metric, choose_metric,_aggregate_metric, min_date, max_date, _aggregate_metric)

    elif choose_metric == 'amount_paid':

        _query = """
                SELECT "CardHolderID","{}",sum("Amount") as "{}"
                FROM "TRANSACTIONS"."POS_TRANSACTIONS"
                where "Amount" > 0 and "{}" <> '' and "TransactionTime" >= '{}' and "TransactionTime" <= '{}'
                GROUP BY "CardHolderID","{}"
                     """.format(_aggregate_metric, choose_metric, _aggregate_metric, min_date, max_date, _aggregate_metric)

    else:
        print("Please choose an appropriate metric")

    _tic = time.time()
    _data = pd.read_sql(_query, con=engine)
    print("Query was executed in {} seconds".format(time.time() - _tic))

    _tic = time.time()
    _table_mcc = pd.pivot_table(_data, values=choose_metric,
                                index=['CardHolderID'], columns=[_aggregate_metric], aggfunc=np.sum)

    print("Pivot done in {} seconds".format(time.time() - _tic))

    _table_mcc = _table_mcc.fillna(0)

    return _table_mcc



def compute_usage(engine,_aggregate_metric,choose_metric,start_date, end_date,**kwargs):

    _is_prop = kwargs.get('is_prop', False)

    _table_mcc = compute_usage_details(engine, _aggregate_metric, choose_metric,start_date, end_date)

    if _is_prop:
        _col_list = list()
        for _col in _table_mcc.columns:
            _col_list.append(str(_col) + "_Prop")

        _table_mcc.columns = _col_list
        _table_mcc = _table_mcc.fillna(0)

        # Fill NA's
        _table_mcc = np.divide(_table_mcc, pd.DataFrame(_table_mcc.sum(axis=1)))

    return _table_mcc


def main_usage(engine, _aggregate_metric, choose_metric, start_date, end_date, use_prop=True):

    # Compute usage of sous-univers in amount
    data_amount = compute_usage(engine, _aggregate_metric, choose_metric, start_date, end_date, is_prop=use_prop)


    final_set_usage_card = pd.concat([data_amount,pd.DataFrame(data_amount.max(axis=1),
                                        index=data_amount.index, columns=["max_usage_"+_aggregate_metric])], axis=1)

    final_set_usage_card = pd.concat([final_set_usage_card, pd.DataFrame((data_amount.idxmax(axis=1).str[:-5]),
                                        index=data_amount.index, columns=[_aggregate_metric + "_most_used"])], axis=1)

    final_set_usage_card = pd.concat([final_set_usage_card, pd.DataFrame((data_amount > 0).astype(int).sum(axis=1),
                                                                         index=data_amount.index,
                                                                         columns=["num_"+_aggregate_metric])],axis=1)

    return final_set_usage_card