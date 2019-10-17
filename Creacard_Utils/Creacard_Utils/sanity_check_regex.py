import re
import sys
import os
import pandas as pd
import time

def check_regex(Folder):

    FileRegexExclu = "regex_merchant.xlsx"
    Data1 = pd.read_excel(Folder + FileRegexExclu, sheet_name='Regex exclu')

    fileExclu = list()

    tic = time.time()
    for reg in Data1["NEW_REGEX"]:
        try:
            re.search(reg, 'jk')
        except:
            fileExclu.append(reg)
            pass
    toc = time.time() - tic
    print('exclusion regex sanity check was done in {} seconds'.format(toc))
    print('number of exclusion regex not well implemented equals {} regex'.format(len(fileExclu)))


    FileRegexExclu = "regex_ajout.xlsx"
    fileajout = list()
    Data2 = pd.read_excel(Folder + FileRegexExclu, sheet_name='Regex ajout')


    tic = time.time()
    for reg in Data2["NEW_REGEX"]:
        try:
            re.search(reg, 'jk')
        except:
            fileajout.append(reg)
            pass
    toc = time.time() - tic
    print('inclusion regex sanity check was done in {} seconds'.format(toc))
    print('number of regex inclusion not well implemented equals {} regex'.format(len(fileajout)))

    return fileExclu, fileajout