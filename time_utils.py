import pandas as pd
import numpy as np
from collections import Counter

def _printFull(df, NAME_DF = ""):
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
        print("PRINTING FULL " + NAME_DF + ": \n", df)

def convertCols2DateTime(df, ls_header, _errors = 'ignore'):
    for h in ls_header:
        df[h] = pd.to_datetime(df[h], errors= _errors)

def fillBombeTimer(df, header_time, header_bomb_header):
    CSGO_BOMB_TIME = 40
    # Create a group identifier for consecutive NaN occurrences
    series_na = df[header_time].isna().ne(df[header_time].isna().shift()).cumsum()
    only_na = series_na[df[header_time].isna()]
    max_occ_index = Counter(only_na).most_common(1)[0][0] #tuple: only intesested in occurence index
    #set everthing else to be nan
    series_na[series_na!=max_occ_index] = np.nan
    #set counting
    series_na[series_na.notna()] = CSGO_BOMB_TIME - series_na[series_na.notna()].notna().cumsum()
    #update original dataframe's bombtimer
    df[header_bomb_header].update(series_na)

def cleanInGameTime(df, header_ingame_time, header_bombtime):

    strSec2TrueSec = lambda int_str_sec: 60 * (int_str_sec//100) + (int_str_sec%100)

    print("strSec2TrueSec(140)",strSec2TrueSec(140))

    #scaling by 100 from 1.40 to 140, meaning 1min and 40 secs
    float_time = pd.to_numeric(df[header_ingame_time], errors= 'coerce') * 100
    float_time = float_time.apply(strSec2TrueSec) #converting 140 (1m40s) to 90sec
    df[header_ingame_time] = float_time

    #fill bomb timer
    fillBombeTimer(df, header_ingame_time, header_bombtime)

    # Create a custom grouping key
    # It will increment whenever a value is not smaller than the previous one
    group_key = (df[header_ingame_time].diff() > -1).cumsum()

    for group_name, group_data in df.groupby(group_key):
        print(f"Group {group_name}:")
        _printFull(group_data[header_ingame_time])