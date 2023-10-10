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
    #check for empty series
    if len(only_na) > 0:
        max_occ_index = Counter(only_na).most_common(1)[0][0] #tuple: only intesested in occurence index
        #set everthing else to be nan
        series_na[series_na!=max_occ_index] = np.nan
        #set counting
        series_na[series_na.notna()] = CSGO_BOMB_TIME - series_na[series_na.notna()].notna().cumsum()
        #update original dataframe's bombtimer
        df[header_bomb_header].update(series_na)

def cleanInGameTime(df, header_ingame_time, header_bombtime):
    #previous function has already turned time from "1:40" to "1.40"
    #this lambda turn float "1.40" 1 min andd 40 sec to "100" int sec
    strSec2TrueSec = lambda int_str_sec: 60 * (int_str_sec//100) + (int_str_sec%100)
    #scaling by 100 from 1.40 to 140, meaning 1min and 40 secs
    df[header_ingame_time]  = pd.to_numeric(df[header_ingame_time], errors= 'coerce') * 100
    float_time  = df[header_ingame_time].apply(strSec2TrueSec) #converting 140 (1m40s) to 90sec
    df[header_ingame_time] = float_time
    
    #fill bomb timer
    fillBombeTimer(df, header_ingame_time, header_bombtime)

    #setting round time
    MAX_ROUND_TIME = 120 #2 minute per round

    #check for empty
    if len(float_time[float_time <= MAX_ROUND_TIME]) > 0:
        index_reasonable_max = float_time[float_time <= MAX_ROUND_TIME].idxmax()
        index_last_valid = float_time.last_valid_index()
        #find the highest in game time recognized
        reasonable_max = int(float_time[index_reasonable_max])
        #create assumed true range to fix time
        round_time_range = float_time[pd.RangeIndex(index_reasonable_max, index_last_valid)]
        #create new round times
        round_time_range = pd.Series(range(reasonable_max, reasonable_max-len(round_time_range), -1))
        #update df
        df[header_ingame_time][pd.RangeIndex(index_reasonable_max, index_last_valid)].update(round_time_range)
        
        if index_reasonable_max > 0:
            preround_index = df[header_ingame_time].index
            preround_index = preround_index[preround_index < index_reasonable_max]
            _printFull(df[header_ingame_time][preround_index], "Everything before max")
