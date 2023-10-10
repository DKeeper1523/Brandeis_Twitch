from utils import *
from hp_utils import *
from time_utils import *
#Constants
# path_analysis = "E:\dev\Python\CS_Twitch\\video_analysis.csv"
# path_info = "E:\dev\Python\CS_Twitch\\basic_information.csv"

path_analysis = "/Users/tianyaoh/Desktop/dev/CS_Twitch/Brandeis_Twitch_RA/video_analysis.csv"
path_info = "/Users/tianyaoh/Desktop/dev/CS_Twitch/Brandeis_Twitch_RA/basic_information.csv"

#init loggers
file_name = path_analysis[path_analysis.rindex('/')+1:-4]
init_log(log_name = file_name + ".log")

#read csv to pd.dataframe
df_analysis = pd.read_csv(path_analysis)
df_info = pd.read_csv(path_info)

#REMOVING UNUSED ROWS
df_analysis = df_analysis[df_analysis['Stage'].notnull()]

#CONSTANTS
ALL_MAP_NAME = ["inferno", 'mirage', 'nuke', 'overpass', 'vertigo', 'ancient', 'anubis', 'dust ii', 'train', 'cache']
ALL_TEAM_NAME =  list(df_info['Team'])
ALL_GROUP_STAGE = list(df_info['From'].unique())
ALL_BO = ["BO1", "BO3"]

#NEW HEADERS
T0_MAP_SCORE = 'Team0_Map_Score'
T1_MAP_SCORE = 'Team1_Map_Score'

ROUND_TIME = 'Ingame_Time_Left'
BOMB_TIME = 'Ingame_Bomb_Time'

if __name__ == "__main__":
    df_result = None
    MINIMUM_ROW_PER_GROUP = 3

    #Headers
    HP_HEADERS = ['Player_HP_'+str(i) for i in range(10)]

    #Main loop to change each group
    insertBombTimer(df_analysis, BOMB_TIME)
    insertMapScores(df_analysis, [T1_MAP_SCORE, T0_MAP_SCORE])

    # """
    for i, group in groupDf(df = df_analysis):
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
        bfillCols(group, HP_HEADERS)
        # """
        #Special Cleaning:
        #set stage to mode
        setCol2Mode(group, ['Stage'])
        #  - spliting  stage into multiple col
        split_success = split_stage(group, t0_score_header = T0_MAP_SCORE, t1_score_header=T1_MAP_SCORE)

        #Fix Time
        #First modify
        cleanInGameTime(group, ROUND_TIME, BOMB_TIME)

        #Fix HP
        #  - ensuring that all hp is in descending order
        ensureColsDesend(group, HP_HEADERS)
        #NYI removing group with failed split
        if split_success:
            #update
            df_analysis.update(group)
            #update map type
            ls_2int = [T1_MAP_SCORE, T0_MAP_SCORE, 'BO', 'Score_0', 'Score_1']
            convertCols2Numeric(df_analysis, ls_2int)
        else:
            print("STAGE_SPLIT Failed: because stage is not recognizable\n - dropping group index: ", group.index)
            df_analysis.drop(group.index, inplace = True)
        if i > 1:
          break
    # """

    #convert hp to int type
    convertCols2Numeric(df_analysis, HP_HEADERS)
    # print(df_analysis.dtypes)
    #write to csv
    df_analysis.to_csv(file_name + "_CLEAN.csv", index = False)