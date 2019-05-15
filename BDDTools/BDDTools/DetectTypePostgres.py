import pandas as pd

def CreateDictionnaryType(Data):

    """Translate types of pandas DataFrame
       into the postgres format for data
       ingestion

        Parameters
        -----------
        Data : DataFrame object
            DataFrame that must be ingested into postgres

        Returns
        -----------
        DataTypes : Dictionnary
            Dictionnary that is compliant with
            the ingestion function expected format
    """

    DataTypes         = pd.DataFrame(Data.dtypes)
    DataTypes         = DataTypes.reset_index(drop=False)
    DataTypes.columns = ["Variables", "Type"]
    DataTypes.Type = DataTypes.Type.astype(str)

    for i in DataTypes.loc[DataTypes.Type.isin(["float64", "float32"]), "Variables"]:
        DataTypes.loc[DataTypes.Variables == i, "Type"] = "double precision"

    for i in DataTypes.loc[DataTypes.Type.isin(["int64", "int32"]), "Variables"]:
        DataTypes.loc[DataTypes.Variables == i, "Type"] = "INTEGER"

    for i in DataTypes.loc[DataTypes.Type == "datetime64[ns]", "Variables"]:
        DataTypes.loc[DataTypes.Variables == i, "Type"] = "timestamp without time zone"

    for i in DataTypes.loc[DataTypes.Type == "object", "Variables"]:
        if max(Data[i]) <= 20:
            DataTypes.loc[DataTypes.Variables == i, "Type"] = "VARCHAR (40)"
        else:
            DataTypes.loc[DataTypes.Variables == i, "Type"] = "TEXT"

    if sum(DataTypes.Type.isin(['double precision', 'INTEGER', 'VARCHAR (40)', 'timestamp without time zone', 'TEXT'])) == DataTypes.shape[0]:
        print("The data dictionnary is well filled")
    else:
        print("not filled")

    DataTypes = DataTypes.reindex(sorted(DataTypes.columns), axis=1)
    DataTypes = DataTypes.set_index('Variables')


    DataTypes = DataTypes.to_dict()
    DataTypes = DataTypes["Type"]

    return DataTypes
