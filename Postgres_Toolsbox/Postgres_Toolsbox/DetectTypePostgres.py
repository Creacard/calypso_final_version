import pandas as pd
from collections import OrderedDict

def CreateDictionnaryType(Data, **kwargs):

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

    _custom_types = kwargs.get('custom_type', None)

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

    if _custom_types is not None:
        for c in _custom_types.keys():
            DataTypes.loc[DataTypes["Variables"] == c, "Type"] = _custom_types[c]


    DataTypes = DataTypes.set_index('Variables')


    DataTypes = DataTypes.to_dict()
    DataTypes = DataTypes["Type"]

    return DataTypes


def create_dictionnary_type_from_table(engine, TlbName):
    get_dictionnary_type = """
    SELECT "column_name", "data_type", "character_maximum_length"
    FROM "information_schema"."columns" WHERE 
    table_name = '{}'
    """.format(TlbName)

    columns_type = pd.read_sql(get_dictionnary_type, con=engine)

    for i in range(0, len(columns_type)):
        if columns_type.iloc[i, 1] == "character varying":
            columns_type.iloc[i, 1] = "VARCHAR" + "(" + str(int(columns_type.iloc[i, 2])) + ")"

    columns_type = columns_type.drop(columns="character_maximum_length", axis=1)
    columns_type = columns_type.set_index("column_name")
    columns_type_dict = columns_type["data_type"].to_dict(into=OrderedDict)

    del columns_type

    return columns_type_dict