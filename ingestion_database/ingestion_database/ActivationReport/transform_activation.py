import pandas as pd


def transform_activation_report(data, filepath, **kwargs):

    FileName = filepath.split('/')[-1]

    data["CardholderID"] = data["CardholderID"].astype(str).str.replace("'", "")
    data["ActivationDate"] = pd.to_datetime(data["ActivationTime"], format="%d/%m/%Y %H:%M:%S")

    data = data[["ActivationDate", "CardholderID"]]
    data.columns = ["ActivationTime", "CardHolderID"]
    data["FileSource"] = FileName

    return data
