""" Function for splitting a data frame into smaller """

def splitDataFrameIntoSmaller(df, chunkSize = 10000):
    """Split a dataframe into smaller

        Parameters
        -----------
        df : pandas DataFrame object
            Pandas DataFrame
        chunkSize : int
            Number of rows by split

        Returns
        -----------
        listOfDf : list
            List that contains all chuncks of the splitted dataframe

    """
    listOfDf = list()
    numberChunks = len(df) // chunkSize + 1
    for i in range(numberChunks):
        listOfDf.append(df[i*chunkSize:(i+1)*chunkSize])
    return listOfDf
