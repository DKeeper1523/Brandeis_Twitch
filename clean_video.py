from utils import *
from hp_utils import *
from time_utils import *


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

def cleanVideoDf(df_video, df_info):
    #init dependent constant
    ALL_TEAM_NAME =  list(df_info['Team'])
    ALL_GROUP_STAGE = [x for x in df_info['From'].unique()]

    # ALL_GROUP_STAGE = [x.lower() for x in df_info['From'].unique()]

    #REMOVING UNUSED ROWS
    df_video = df_video[df_video['Stage'].notnull()]

    #Headers
    HP_HEADERS = ['Player_HP_'+str(i) for i in range(10)]

    #Main loop to change each group
    insertTimers(df_video, [BOMB_TIME, INGAME_TIME_PASSED])
    insertMapScores(df_video, [T1_MAP_SCORE, T0_MAP_SCORE])

    #there are instances of very small group (<=3 row) has mis identified rows
    MINIMUM_ROW_PER_GROUP = 3
    for i, group in groupDf(df = df_video):
        if len(group) <= MINIMUM_ROW_PER_GROUP:
            df_video.drop(group.index, inplace=True)
        else:
            #Set Round Scores
            setCol2Mode(group, ['Score_0', 'Score_1'])

            print(f'Group {i}:')
            #map raw to clean counter part
            fix_col_with_replace(group, ["Map"], ALL_MAP_NAME, True)
            fix_col_with_replace(group, ["Team_0", "Team_1"], ALL_TEAM_NAME, True)
            fix_col_with_replace(group, ["BO"], ALL_BO, True)
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
            # setCol2Mode(group, ['Stage'])

            #  - spliting  stage into multiple col
            split_success = split_stage(group, t0_score_header = T0_MAP_SCORE, t1_score_header=T1_MAP_SCORE)

            printFull(group.iloc[:, :5], "After Split")

            #Fix Time - need to fix time before
            cleanInGameTime(group, ROUND_TIME, BOMB_TIME)

            #Fix HP
            #  - ensuring that all hp is in descending order
            ensureColsDesend(group, HP_HEADERS)

            #update
            df_video.update(group)
            print("group updated")

            #NYI removing group with failed split
            if split_success:
                #update map type
                ls_2int = [ROUND_TIME, BOMB_TIME, T1_MAP_SCORE, T0_MAP_SCORE, 'Score_0', 'Score_1']
                convertCols2Numeric(df_video, ls_2int, _errors = 'coerce')
            else:
                print("STAGE_SPLIT Failed: because stage is not recognizable\n - dropping group index: ", group.index)
                df_video.drop(group.index, inplace = True)
            # if i > 10:
            #     break

        #convert hp to int type
        convertCols2Numeric(df_video, HP_HEADERS)
        # print(df_video.dtypes)
    return df_video
