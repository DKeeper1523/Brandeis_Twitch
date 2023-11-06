from utils import *
from hp_utils import *
from time_utils import *
from truncate_utils import truncateVideo
from kill_utils import count_kills

from tqdm.auto import tqdm

#CONSTANTS
ALL_MAP_NAME = ["inferno", 'mirage', 'nuke', 'overpass', 'vertigo', 'ancient', 'anubis', 'dust ii', 'train', 'cache']
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

def cleanVideoDf(file_name, pbar_pos, df_video, df_info, min_row_per_group):
    #init dependent constant
    ALL_TEAM_NAME =  list(df_info['Team'])
    ALL_GROUP_STAGE = [x.lower() for x in df_info['From'].unique()] + ADDITIONAL_STAGE

    #remove every 1001 row
    truncateVideo(df_video)

    # REMOVING UNUSED ROWS
    df_video = df_video[df_video['Stage'].notnull()]

    #Main loop to change each group
    insertTimers(df_video, [BOMB_TIME, INGAME_TIME_PASSED])
    insertMapScores(df_video, [T1_MAP_SCORE, T0_MAP_SCORE])

    #init final df
    final = pd.DataFrame()

    #
    for i, group in tqdm(groupDf(df_video), desc=file_name, leave=True, position=pbar_pos):
        if len(group) >= min_row_per_group:

            #Set Round Scores
            group.loc[:, ROUND_SCORE] = group.loc[:, ROUND_SCORE].bfill().ffill().fillna(0)
            setCol2Mode(group, ['Score_0', 'Score_1'])

            # print(f'Group {i}:')
            #map raw to clean counter part
            fix_col_with_replace(group, ["Map"], ALL_MAP_NAME, True)
            group['Map'] = group['Map'].apply(str.capitalize)
            fix_col_with_replace(group, ["Team_0", "Team_1"], ALL_TEAM_NAME, True)
            fix_col_with_replace(group, ["BO"], ALL_BO, True)
            
            #cleaning stage
            fixBO1Stage(group)
            addStageSep(group)

            #fix bo
            setCol2Mode(group, ['BO'])
            # printFull(group.loc[:, ['Stage', 'BO']].head(5), 'head')

            #format cleaning
            fix_col_with_fun(group, ['Stage'], lambda x: stage(x, ALL_GROUP_STAGE))
            fix_col_with_fun(group, [ROUND_TIME], time)
            fix_col_with_fun(group, HP_HEADERS, hp)

            #Coercily convert all HP to numeric
            #   - to counter cases where strings are in HP fields
            convertCols2Numeric(group, HP_HEADERS, _errors = 'coerce')

            fillNaCols(group, HP_HEADERS)
            fillNaCols(group, ['Stage'], fFill=True)

            #Special Cleaning:
            #set stage to mode
            setCol2Mode(group, ['Stage'])
            fixBO3(group)

            #  - spliting stage into multiple col
            split_stage(group, t0_score_header = T0_MAP_SCORE, t1_score_header=T1_MAP_SCORE)

            #Fix Time - need to fix time before
            cleanInGameTime(group, ROUND_TIME, BOMB_TIME)
            #set time past ingmae
            setIngameTimePast(group, ROUND_TIME, INGAME_TIME_PASSED)

            #convert numeric, ensure type is safe for HP fix
            ls_2int = [ROUND_TIME, BOMB_TIME, T1_MAP_SCORE, T0_MAP_SCORE, 'Score_0', 'Score_1'] + HP_HEADERS
            #cast numberic types
            convertCols2Numeric(df_video, ls_2int, _errors = 'coerce')

            #Fix HP (MUST BE TYPE SAFE)
            #  - ensuring that all hp is in descending order
            ensureColsOrder(group, HP_HEADERS)

            #Fix Kills
            count_kills(group)

            #Fix Map Score numbers:
            # ensureColsOrder(group, [T1_MAP_SCORE, T0_MAP_SCORE], order = 'ascend')
        
            #update
            final = pd.concat([final, group], axis=0)
            # print("group updated")

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
