import datetime
import pandas as pd


def ComputePeriodVector(StartYear,StartMonth,EndYear,EndMonth):

    start = datetime.datetime(StartYear, StartMonth, 1)
    end = datetime.datetime(EndYear, EndMonth, 1)
    DRange = pd.date_range(str(StartYear) + "-" + str(StartMonth) + "-" + "1", periods=round((end - start).days / 30)+1, freq='M')

    ListDate = []
    for i in range(0,len(DRange)):
        ListDate.append(str(DRange[i].year) + str(DRange[i].month))

    return ListDate

def compute_min_max_date(StartYear,StartMonth,EndYear,EndMonth):

    start = datetime.datetime(StartYear, StartMonth, 1)
    end = datetime.datetime(EndYear, EndMonth, 1)
    DRange = pd.date_range(str(StartYear) + "-" + str(StartMonth) + "-" + "1", periods=round((end - start).days / 30)+1, freq='M')

    min_date = str(DRange.min())[0:7] + "-" + "01"
    max_date = str(DRange.max())[0:10]

    return min_date, max_date


def ComputePeriodVectorDay(StartYear,StartMonth,StartDay,EndYear,EndMonth,EndDay):

    start = datetime.datetime(StartYear, StartMonth, StartDay)
    end = datetime.datetime(EndYear, EndMonth, EndDay)
    DRange = pd.date_range(str(StartYear) + "-" + str(StartMonth) + "-" + str(StartDay), periods=round((end - start).days)+1, freq='D')

    return DRange