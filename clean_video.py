from utils import *
from hp_utils import *
from time_utils import *
from kill_utils import count_kills

from split_conjoined_round import split_conjoined_round
from merge_discontinuous_rounds import merge_disontinuous_rounds

from tqdm.auto import tqdm

import threading

#CONSTANTS
ALL_MAP_NAME = ["inferno", 'mirage', 'nuke', 'overpass', 'vertigo', 'ancient', 'anubis']
ALL_BO = ["BO1", "BO3"]

#NEW HEADERS
T0_MAP_SCORE = 'Team0_Map_Score'
T1_MAP_SCORE = 'Team1_Map_Score'
#TIME RELATED HEADERS
ROUND_TIME = 'Ingame_Time_Left'
BOMB_TIME = 'Ingame_Bomb_Time'
INGAME_TIME_PASSED = 'Ingame_Time_Passed'

#round scoure header
ROUND_SCORE = ['Score_0', 'Score_1']
#Headers
HP_HEADERS = ['Player_HP_'+str(i) for i in range(10)]
GAME_ID = 'GameID'
#To Record Game Played 
dict_stage_map_team = dict(dict())

#Additional Stages
ADDITIONAL_STAGE = ['Champions', 'GrandFinal', 'ShowMatch']

#round_id
round_id= 0
# Lock to control access to the shared resource
lock = threading.Lock()

def cleanVideoDf(file_name, pbar_pos, df_video, df_info, min_row_per_group):
    global round_id
    #init dependent constant
    ALL_TEAM_NAME =  [x.replace(" ", "") for x in df_info['Team']]
    ALL_GROUP_STAGE = [x.lower() for x in df_info['From'].unique()] + ADDITIONAL_STAGE

    insertTimers(df_video, [BOMB_TIME, INGAME_TIME_PASSED])
    insertMapScores(df_video, [T1_MAP_SCORE, T0_MAP_SCORE])

    #init final df
    final = pd.DataFrame()

    #Main loop to change each group
    for df_merged in tqdm(merge_disontinuous_rounds(df_video), desc=file_name, position=pbar_pos, leave=False):
        for round in split_conjoined_round(df_merged):
            #acquire lock
            lock.acquire()

            #add round id
            round.insert(0, 'Round_ID', round_id)
            round_id+=1
            
            #relase lock
            lock.release()

            if len(round) >= min_row_per_group:            
                #Show Round Start and End
                # show_column = ['Timestamp', 'Stage', 'Map', 'Ingame_Time_Left']
                # print(round.loc[:, show_column].iloc[[0,-1], :])

                #Set Round Scores
                round.loc[:, ROUND_SCORE] = round.loc[:, ROUND_SCORE].bfill().ffill().fillna(0)
                setCol2Mode(round, ['Score_0', 'Score_1'])

                #cleaning stage
                fixBO1Stage(round)
                addStageSep(round)

                #format cleaning
                fix_col_with_fun(round, ['Stage'], lambda x: stage(x, ALL_GROUP_STAGE))
                fix_col_with_fun(round, [ROUND_TIME], time)
                fix_col_with_fun(round, HP_HEADERS, hp)

                #Coercily convert all HP to numeric
                #   - to counter cases where strings are in HP fields
                convertCols2Numeric(round, HP_HEADERS, _errors = 'coerce')

                fillNaCols(round, HP_HEADERS)
                fillNaCols(round, ['Stage'], fFill=True)

                #Special Cleaning:
                #set stage to mode
                setCol2Mode(round, ['Stage', 'Team_0', 'Team_1'])
                fixBO3(round)

                #Fix Time - need to fix time before
                cleanInGameTime(round, ROUND_TIME, BOMB_TIME)
                #set time past ingmae
                setIngameTimePast(round, ROUND_TIME, INGAME_TIME_PASSED) #bug: lossing entire round

                #convert numeric, ensure type is safe for HP fix
                ls_2int = [ROUND_TIME, BOMB_TIME, T1_MAP_SCORE, T0_MAP_SCORE, 'Score_0', 'Score_1'] + HP_HEADERS
                #cast numberic types
                convertCols2Numeric(round, ls_2int, _errors = 'coerce')

                #Fix HP (MUST BE TYPE SAFE)
                #  - ensuring that all hp is in descending order
                ensureColsOrder(round, HP_HEADERS)

                #Fix Kills
                count_kills(round)

                #fix map, team, BO
                fix_col_with_replace(round, ["Map"], ALL_MAP_NAME)
                round.loc[:,'Map'] = round['Map'].apply(str).apply(str.capitalize)
                fix_col_with_replace(round, ["Team_0", "Team_1"], ALL_TEAM_NAME)
                fix_col_with_replace(round, ["BO"], ALL_BO)

                #set cols to mode
                setCol2Mode(round, ['Map', 'Team_0', 'Team_1', 'BO'])
            
                #update
                final = pd.concat([final, round], axis=0)

    #  - spliting stage into multiple col
    split_stage(final, t0_score_header = T0_MAP_SCORE, t1_score_header=T1_MAP_SCORE)

    #refreshing stages with unreadable stages
    mask = (final.Stage == "Grandfinal") | (final.Stage == "Showmatch")
    final.loc[mask, ROUND_SCORE] = 0

    #insert date and csv
    insertDateAndStream(final, file_name)
    final.insert(loc=1, column = GAME_ID, value = pd.Series(dtype=int))

    #unify columns to 
    setCol2Mode(final, ['Stream', 'Date'])
    #reset time_stamp
    final.reset_index(drop=True, inplace=True)
    final.insert(1, "Time_Stamp", final.index)
    return  final[final['Stage'].notnull()]

def test(group, str_index):
    #checking the first index of every round
    start_index = str(group.index[0])
    #if the start index is 6027, stop
    print('start_index', start_index)
    print('str(type(start_index))', str(type(start_index)))
    print('start index == 6207', start_index == '6207')
    if start_index == str_index: 
        #force print everything in pandas dataframe
        pd.set_option('display.max_rows', None)
        print('--------------------------------------------------')
        print('group', group[['Ingame_Time_Passed', 'Ingame_Time_Left']])
        raise Exception("stop")
