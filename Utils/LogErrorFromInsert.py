
import os
import logging
from logging.handlers import RotatingFileHandler
import datetime

"""
Missing : 
    - argument of the path folder for the logger 
"""

def CreateLogger(Folder,FileName,HomePath):

    # Check if exist
    if not os.path.isdir(HomePath):
        os.makedirs(HomePath)
    # Point to the log type folder
    LogFolder = HomePath + Folder + "/"
    # Check if the log type folder exists if not
    # Create a new one
    if not os.path.isdir(LogFolder):
        os.makedirs(LogFolder)

    now = datetime.datetime.now()
    LogPathFile = LogFolder + FileName + str(now.year) + "_" + str(now.month) + "_" + str(now.day) + '.log'

    ### Create the Logger ###
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
    file_handler = RotatingFileHandler(LogPathFile, 'a')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setLevel(logging.ERROR)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger