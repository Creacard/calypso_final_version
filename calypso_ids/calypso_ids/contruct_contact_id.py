import pandas as pd
from Levenshtein import distance
from scipy.spatial.distance import pdist, squareform
import numpy as np
import time



def generate_contact_id(data, max_contact_id):

    # my list of strings

    strings = data["combi"].tolist()

    # prepare 2 dimensional array M x N (M entries (3) with N dimensions (1))
    transformed_strings = np.array(strings).reshape(-1 ,1)

    # calculate condensed distance matrix by wrapping the Levenshtein distance function
    distance_matrix = pdist(transformed_strings, lambda x, y: distance(x[0], y[0]))

    # get square matrix
    mm = squareform(distance_matrix)
    mm = pd.DataFrame(mm, index=data["CardHolderID"].tolist() ,columns=data["CardHolderID"].tolist())


    final_id = dict()
    for cardholder in mm.index:
        final_id[cardholder] = list(mm.loc[cardholder, :][mm.loc[cardholder, :] <= 3].index)

    id_contact = 1
    final_contact_id = None
    while len(final_id) > 0:

        tmp_frame = pd.DataFrame(final_id[final_id.keys()[0]])
        tmp_frame["CONTACT_ID"] = id_contact
        tmp_frame.columns = ["CardHolderID", "CONTACT_ID"]

        valuesRemoved = [final_id.pop(key_r, None) for key_r in final_id[final_id.keys()[0]]]

        final_contact_id = pd.concat([final_contact_id, tmp_frame], axis=0)

        id_contact += 1


    data = pd.merge(data, final_contact_id, how="inner", on="CardHolderID")

    non_unique = data.groupby("CardHolderID").size().sort_values().reset_index()
    non_unique.columns = ["CardHolderID", "count_n"]
    non_unique = non_unique.loc[non_unique["count_n"] > 1, "CardHolderID"]

    if len(non_unique) > 0:

        sorted = False
        while sorted is False:

            tmp = data.loc[data["CardHolderID"].isin(list(non_unique)), ["CardHolderID", "CONTACT_ID"]]
            tmp_card = pd.merge(tmp, tmp.groupby("CardHolderID")["CONTACT_ID"].min().reset_index(), how="inner",
                                on="CardHolderID")
            tmp_card.columns = ["CardHolderID", "CONTACT_ID", "TMP_CONTACT_ID"]

            data = pd.merge(data, tmp_card[["CONTACT_ID", "TMP_CONTACT_ID"]], how="left", on="CONTACT_ID")

            data.loc[~data["TMP_CONTACT_ID"].isnull(), "CONTACT_ID"] = data.loc[
                ~data["TMP_CONTACT_ID"].isnull(), "TMP_CONTACT_ID"]
            data = data.drop(columns="TMP_CONTACT_ID", axis=1)
            data = data.groupby(['CardHolderID', 'combi', 'BirthDate', 'BirthDate_ID'])["CONTACT_ID"].min().reset_index()

            non_unique = data.groupby("CardHolderID").size().sort_values().reset_index()
            non_unique.columns = ["CardHolderID", "count_n"]
            non_unique = non_unique.loc[non_unique["count_n"] > 1, "CardHolderID"]

            if len(non_unique) > 0:
                sorted = False
            else:
                sorted = True

    new_contact_id = pd.DataFrame(data["CONTACT_ID"].unique())
    new_contact_id.columns = ["CONTACT_ID"]

    if max_contact_id == 0:
        new_contact_id["TMP_CONTACT_ID"] = pd.DataFrame(range(1, len(new_contact_id) + 1))
    else:
        last_user_id = int(data) + 1 + len(new_contact_id)
        new_contact_id["TMP_CONTACT_ID"] = pd.DataFrame(range(int(data) + 1, int(last_user_id)))

    data = pd.merge(data, new_contact_id, on="CONTACT_ID", how="inner")
    data["CONTACT_ID"] = data["TMP_CONTACT_ID"]
    data = data.drop(columns='TMP_CONTACT_ID', axis=1)
    data["CONTACT_ID"] = data["BirthDate_ID"].astype(str) + "_" + data["CONTACT_ID"].astype(str)

    return data
