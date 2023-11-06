import pandas as pd
import numpy as np
from utils import printFull
from collections import Counter

# import warnings
# warnings.simplefilter(action='ignore', category=FutureWarning)

#Time starter
START_PAST_TIME = 5
#setting round time
MAX_ROUND_TIME = 115 #2 minute per round

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

def resetHP(df, row_indices, hp_headers = ['Player_HP_'+str(i) for i in range(10)]):
    # ALL_HUNDRED = {hp:100 for hp in hp_headers}
    # for row in row_indices:
    if len(row_indices) > 0:
        df.loc[row_indices, hp_headers] = 100
        # print("Hp set to 100 :", row_indices)

def cleanInGameTime(df, header_ingame_time, header_bombtime):
    #previous function has already turned time from "1:40" to "1.40"
    #this lambda turn float "1.40" 1 min andd 40 sec to "100" int sec
    strSec2TrueSec = lambda int_str_sec: 60 * (int_str_sec//100) + (int_str_sec%100)
    #scaling by 100 from 1.40 to 140, meaning 1min and 40 secs
    df[header_ingame_time]  = pd.to_numeric(df[header_ingame_time], errors= 'coerce') * 100
    time_sec  = df[header_ingame_time].apply(strSec2TrueSec) #converting 140 (1m40s) to 90sec
    df[header_ingame_time] = time_sec

    def _setReasonableRange(df, threshhold_max, step = -1, bool_get = False):
        index_rational_max = df[df <= threshhold_max].idxmax()
        index_last_valid = df.last_valid_index()
        #find the highest in game time recognized
        rational_max = int(df[index_rational_max])
        #create assumed true range to fix round time
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
    if len(time_sec[time_sec <= MAX_ROUND_TIME]) > 0:
        #try guess the time range of the round  
        index_rational_max = _setReasonableRange(df[header_ingame_time], MAX_ROUND_TIME)
        cur_max = df[header_ingame_time][index_rational_max]

        #get round prep
        prep_index = df[header_ingame_time].index
        prep_index = prep_index[prep_index < index_rational_max]
        #if there is any prep index before prep index
        if len(prep_index) > 0:
            #if cur max is not 115
            missing_roundtime = pd.Series(range(int(cur_max), MAX_ROUND_TIME), dtype=int) + 1

            #create complete prep
            if len(prep_index) - len(missing_roundtime) > 0 :
                freeze_time = pd.Series(range(len(prep_index) - len(missing_roundtime))) + 1
                complete_prep = pd.concat([missing_roundtime, freeze_time], ignore_index=True)[::-1]
            else:
                complete_prep = missing_roundtime[::-1]

            # complete_prep = pd.concat([missing_roundtime, freeze_time], ignore_index=True)[::-1]
            
            #truncate excess
            complete_prep = complete_prep[-len(prep_index):]
            #set new prep index
            try:
                complete_prep.index = prep_index
            except:
                print(complete_prep, prep_index)
                printFull(df[header_ingame_time].head(10))
                raise Exception("len(complete_prep), len(prep_index)", len(complete_prep), len(prep_index))
            #reset hp along prep rows
            resetHP(df, prep_index)
            # df[header_ingame_time][prep_index]= new_prep
            df.loc[prep_index, header_ingame_time] = complete_prep

    #fill bomb timer
    fillBombeTimer(df, header_ingame_time, header_bombtime)

    #fix instances where bomb timer and round time both has number
    dup_bombAndRound = df.Stage.isna() & df.Stage.isna()
    df.loc[dup_bombAndRound, header_ingame_time] = None

                
def setIngameTimePast(df, header_round_time, header_ingame_time_past):
    #Time starter
    START_PAST_TIME = 5
    #get maximum from round time
    _offset = lambda cur_max: START_PAST_TIME + MAX_ROUND_TIME - cur_max
    #skipping df that is entirely empty
    if not df[header_round_time].isnull().all():
        #get needed indexes
        index_max_round_time = df[header_round_time].idxmax()
        first_index = df.index[0]

        #initial value
        seed = int(_offset(df[header_round_time][index_max_round_time]))
        #translate seed from cur max to the begining
        normal_index_max = index_max_round_time - first_index
        seed -= (normal_index_max)
        time_past = pd.Series(range(seed, seed + len(df)))
        time_past.index = df.index

        #update time past ingame
        df[header_ingame_time_past].update(time_past)

        #remove everything that has a negative value in Time past in game
        df.loc[:,:] = df.loc[df[header_ingame_time_past]>=0, :]


