import pandas as pd
import time


def compute_user_id(data, **kwargs):

    max_user_id = kwargs.get('last_user_id', 0)

    tic = time.time()

    user_id = data.copy()
    id_user = pd.DataFrame(range(1, len(user_id) + 1))
    user_id = user_id.reset_index(drop=True)
    user_id["USER_ID"] = id_user

    sorted = False
    i = 0
    while sorted is False:

        tmp_user_id = user_id.groupby("NoMobile")["USER_ID"].min().reset_index()
        tmp_user_id.columns = ["NoMobile", "TMP_USER_ID"]
        user_id = pd.merge(user_id, tmp_user_id, on="NoMobile", how="inner")
        user_id["USER_ID"] = user_id["TMP_USER_ID"]
        user_id = user_id.drop(columns='TMP_USER_ID', axis=1)

        tmp_user_id = user_id[user_id["GoodEmail"] == 1].groupby("Email")["USER_ID"].min().reset_index()
        tmp_user_id.columns = ["Email", "TMP_USER_ID"]
        user_id = pd.merge(user_id, tmp_user_id, on="Email", how="left")
        user_id.loc[~user_id["TMP_USER_ID"].isnull(), "USER_ID"] = user_id["TMP_USER_ID"]
        user_id = user_id.drop(columns='TMP_USER_ID', axis=1)

        tmp_user_id = user_id[user_id["GoodCombinaison"] == 1].groupby("combinaison")["USER_ID"].min().reset_index()
        tmp_user_id.columns = ["combinaison", "TMP_USER_ID"]
        user_id = pd.merge(user_id, tmp_user_id, on="combinaison", how="left")
        user_id.loc[~user_id["TMP_USER_ID"].isnull(), "USER_ID"] = user_id["TMP_USER_ID"]
        user_id = user_id.drop(columns='TMP_USER_ID', axis=1)

        non_unique_num = user_id.groupby("NoMobile")["USER_ID"].nunique().sort_values().reset_index()
        non_unique_num = non_unique_num.loc[non_unique_num["USER_ID"] > 1, "NoMobile"]


        non_unique_email = user_id[user_id["GoodEmail"] == 1].groupby("Email")[
            "USER_ID"].nunique().sort_values().reset_index()
        non_unique_email = non_unique_email.loc[non_unique_email["USER_ID"] > 1, "Email"]

        non_unique_combi = user_id[user_id["GoodCombinaison"] == 1].groupby("combinaison")[
            "USER_ID"].nunique().sort_values().reset_index()
        non_unique_combi = non_unique_combi.loc[non_unique_combi["USER_ID"] > 1, "combinaison"]

        if (len(non_unique_num) > 0) or (len(non_unique_email) > 0) or (len(non_unique_combi) > 0):
            sorted = False
        else:
            sorted = True

        i = i + 1

    toc = time.time() - tic

    new_user_id = pd.DataFrame(user_id["USER_ID"].unique())
    new_user_id.columns = ["USER_ID"]

    if max_user_id == 0:
        new_user_id["TMP_USER_ID"] = pd.DataFrame(range(1, len(new_user_id) + 1))
    else:
        last_user_id = int(max_user_id) + 1 + len(new_user_id)
        new_user_id["TMP_USER_ID"] = pd.DataFrame(range(int(max_user_id) + 1, int(last_user_id)))

    user_id = pd.merge(user_id, new_user_id, on="USER_ID", how="inner")
    user_id["USER_ID"] = user_id["TMP_USER_ID"]
    user_id = user_id.drop(columns='TMP_USER_ID', axis=1)

    print("generation of user id took {} seconds".format(toc))

    return user_id