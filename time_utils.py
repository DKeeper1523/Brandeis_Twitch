import pandas as pd
import numpy as np
from utils import printFull
from collections import Counter

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
    MAX_ROUND_TIME = 115 #2 minute per round

    def _setReasonableRange(df, threshhold_max, step = -1, bool_get = False):
        index_rational_max = df[df <= threshhold_max].idxmax()
        index_last_valid = df.last_valid_index()
        #find the highest in game time recognized
        rational_max = int(df[index_rational_max])
        #create assumed true range to fix time
        round_time_range = df[pd.RangeIndex(index_rational_max, index_last_valid)]
        #create new round times
        round_time_range = pd.Series(range(rational_max, rational_max-len(round_time_range), step))
        if bool_get:
            return round_time_range
        else: #set
            #update df
            df[pd.RangeIndex(index_rational_max, index_last_valid)].update(round_time_range)
            #return index of max found
            return index_rational_max

    #check for empty
    if len(float_time[float_time <= MAX_ROUND_TIME]) > 0:
        #try guess the round time range
        index_rational_max = _setReasonableRange(df[header_ingame_time], MAX_ROUND_TIME)
        #0 check to avoid -1 indexing
        if index_rational_max > 0:
            prep_index = df[header_ingame_time].index
            prep_index = prep_index[prep_index < index_rational_max]
            #get everything before rational max
            before_rational_max = df[header_ingame_time][prep_index]
            #if everything before rational max is above prep max; most-likely everything is recognition error
            PREP_MAX=20
            all_big_error = all(before_rational_max>PREP_MAX)
            #extend rational max out
            if all_big_error:
                cur_max = df[header_ingame_time][index_rational_max]
                new_prep = pd.Series(range(len(before_rational_max), 0, -1)) + cur_max
            else:
                # get range guess
                new_prep = pd.Series(range(len(before_rational_max), 0, -1))
            #reset hp
            resetHP(df, prep_index)
            #Set new prep
            # _printFull(new_prep, "guess_prep")
            # df[header_ingame_time][prep_index]= new_prep
            df.loc[prep_index, header_ingame_time].update(new_prep)
            # _printFull(df[header_ingame_time], "New Prep")

def resetHP(df, row_indices, hp_headers = ['Player_HP_'+str(i) for i in range(10)]):
    # ALL_HUNDRED = {hp:100 for hp in hp_headers}
    # for row in row_indices:
    if len(row_indices) > 0:
        df.loc[row_indices, hp_headers] = 100
        print("Has resetted hp on :", row_indices)