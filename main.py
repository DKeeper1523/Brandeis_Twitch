from utils import *
from hp_utils import *
from time_utils import *

#adding command line running capbility
import argparse
import os

# Create the parser and add arguments
parser = argparse.ArgumentParser(
    prog = 'CSGO_Video_analysis_cleaner',
    description= 'clean csgo twitch stream data '
)
#required arguments for directory for 
parser.add_argument(dest='src', type=str, help="Looks under the current working directory for the directory containing raw video analysis data (it should have folders for each stream)")
parser.add_argument(dest='path_csv_info', type=str, help="CSV containing basic infommation for team competing")
parser.add_argument('-out', type=str, help="directory for storing cleaned video data")
# parser.add_argument(dest='info', type=str, help="Directory containing video analysis folder")

# Parse and print the results
args = parser.parse_args()

#get dir_src
dir_src = args.src
if not os.path.exists(dir_src):
    raise Exception("cwd: ",os.getcwd(),"\n Current working directory does not contain raw data folder\n Make sure that raw data folder is under the current working directory")
#get dir_out
dir_out = 'clean_data'if args.out is None else args.out
#create dir_out if it does not exist
if not os.path.exists(dir_out):
    print(2234234)
    os.makedirs(dir_out)
    print(dir_out, ' created under the current working directory.')

#Get df_info
path_info = args.path_csv_info
if not os.path.isfile(path_info):
    raise Exception("basic_information.csv does not exist! This file is required.")
else:
    #check if file is accessible
    if not os.access(path_info, os.R_OK):
        raise Exception("basic_information.csv cannot be read! Read permission is required! please make .csv readable.")
#init info dataframe
df_info = pd.read_csv(path_info)

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
INGAME_TIME_PASSED = 'Ingame_Time_Passed'

if __name__ == "__main__":
    #csv names
    CSV_TEXT = "/audio_text_analysis.csv"
    CSV_VIDEO = "/video_analysis.csv"
    CSV_AUDIO = "/audio_analysis.csv"

    #brows immediate sub directories
    gen_sub = (os.walk(dir_src))
    gen_sub.__next__() #skiping first
    for x in gen_sub: 
        name_dataset = x[0]
        #reading
        df_text = pd.read_csv(name_dataset + CSV_TEXT)
        df_video = pd.read_csv(name_dataset + CSV_VIDEO)
        df_audio = pd.read_csv(name_dataset + CSV_AUDIO)

        print("len text: ", len(df_text))
        print("len vieo: ", len(df_video))
        print("len audio: ", len(df_audio))
  

        print(str(type(x)))
"""
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

#REMOVING UNUSED ROWS
df_analysis = df_analysis[df_analysis['Stage'].notnull()]


if __name__ == "__main__":
    df_result = None
    MINIMUM_ROW_PER_GROUP = 3

    #Headers
    HP_HEADERS = ['Player_HP_'+str(i) for i in range(10)]

    #Main loop to change each group
    insertTimers(df_analysis, [BOMB_TIME, INGAME_TIME_PASSED])
    insertMapScores(df_analysis, [T1_MAP_SCORE, T0_MAP_SCORE])

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

        #Special Cleaning:
        #set stage to mode
        setCol2Mode(group, ['Stage'])
        #  - spliting  stage into multiple col
        split_success = split_stage(group, t0_score_header = T0_MAP_SCORE, t1_score_header=T1_MAP_SCORE)

        #Fix Time
        #First modify
        cleanInGameTime(group, ROUND_TIME, BOMB_TIME)

        # printFull(group[ROUND_TIME])

        #Fix HP
        #  - ensuring that all hp is in descending order
        ensureColsDesend(group, HP_HEADERS)

        #update
        df_analysis.update(group)
        print("group updated")

        #NYI removing group with failed split
        if split_success:
            #update map type
            ls_2int = [ROUND_TIME, BOMB_TIME, T1_MAP_SCORE, T0_MAP_SCORE, 'Score_0', 'Score_1']
            convertCols2Numeric(df_analysis, ls_2int, _errors = 'coerce')
        else:
            print("STAGE_SPLIT Failed: because stage is not recognizable\n - dropping group index: ", group.index)
            df_analysis.drop(group.index, inplace = True)

        # if i >3:
        #     break

    #convert hp to int type
    convertCols2Numeric(df_analysis, HP_HEADERS)
    # print(df_analysis.dtypes)
    #write to csv
    df_analysis.to_excel(file_name + "_CLEAN.xlsx", index = False)

    """