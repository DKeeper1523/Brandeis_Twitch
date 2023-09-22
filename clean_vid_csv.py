import pandas as pd
import numpy as np
import difflib
from fuzzywuzzy import process
import re
import logging

#Constants
path_analysis = "E:\dev\Python\CS_Twitch\\video_analysis.csv"
path_info = "E:\dev\Python\CS_Twitch\\basic_information.csv"

#Configure log file
log_name = path_analysis[path_analysis.rindex('\\')+1:-4] + ".log"
logging.basicConfig(filename=log_name, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.FileHandler(log_name, mode='w')

df_analysis = pd.read_csv(path_analysis)
df_info = pd.read_csv(path_info)

#First Step, Ensure game view
df_analysis = df_analysis.loc[df_analysis["Stage"].notnull()]

def GetMostSimilar(base, ls_guess, guess_up = False, guess_low = False, n = 1, cutoff = 0.01):
    ls_guess = list(ls_guess)
    if guess_up:
        ls_guess = [x.upper() for x in ls_guess]
    elif guess_low:
        ls_guess = [x.lower() for x in ls_guess]
    return process.extractOne(base, ls_guess)[0]

#Fix map name
#Create a mapping from original name to the mostlikely name
def createDictGuess(ls_base_name, ls_target_name):
    dict_truth = {}
    for name in ls_base_name:
        most_likely = GetMostSimilar(str(name).lower(), ls_target_name)
        dict_truth[name] = most_likely
    return dict_truth

#To fix certain column with an iterable(usually ) ground truth
def fix_col_with_replace(ls_col_id, ls_truth, df_target = df_analysis):
    #Collect all the columns that needs to be fixed
    #   - assume that they are in the same domain
    #       -e.g play1_name, player2_name are within the same domain; map and hp are not in the same domain
    ls_bad = set()
    for column_name in ls_col_id:
        ls_bad = ls_bad|set(df_target[column_name])

    dict_guess = createDictGuess(ls_bad, ls_truth)
    for key in dict_guess.keys():
        for columnName in ls_col_id:
            df_target[columnName] = df_target[columnName].replace(key, dict_guess[key])
            print("In ", column_name, "replaced ", key, " with ", dict_guess[key])
    pass


#Constants
path_analysis = "E:\dev\Python\CS_Twitch\\video_analysis.csv"
path_info = "E:\dev\Python\CS_Twitch\\basic_information.csv"

#Configure log file
log_name = path_analysis[path_analysis.rindex('\\')+1:-4] + ".log"
logging.basicConfig(filename=log_name, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.FileHandler(log_name, mode='w')

df_analysis = pd.read_csv(path_analysis)
df_info = pd.read_csv(path_info)

#First Step, Ensure game view
df_analysis = df_analysis.loc[df_analysis["Stage"].notnull()]
 
#FIX MAP NAMES:
#All csgo maps
ls_map_name = ["inferno", 'mirage', 'nuke', 'overpass', 'vertigo', 'ancient', 'anubis', 'dust ii', 'train', 'cache']
fix_col_with_replace(["Map"], ls_map_name)

#FIX TEAM NAMES
ls_team_name =  list(df_info['Team'])
fix_col_with_replace(["Team_0", "Team_1"], ls_team_name)

ls_BO = ["BO1", "BO3"]
fix_col_with_replace(["BO"], ls_BO)

#print(df_analysis)

test_pd = df_info['Team']

#needs a function that operate on cell, so "fun" needs to return a string
def fix_col_with_fun(ls_col_name, fun):
    for col_name in ls_col_name:
        df_analysis[col_name] = df_analysis[col_name].apply(fun)

def _time(txt):
    #use regex fix time
    def sub4colon (txt):
        return re.sub(r'(\w+)\W(\w+)', r'\1:\2', str(txt))
    return sub4colon(txt)

# #Fix Stage
def _stage(text):
    ls_group_stage = set(df_info['From'])
    #strip white space
    text = text.strip()
    pattern = r'(\S+\s?)([\d|o|O]\W?\s?[\d|o|O])'
    match = re.search(pattern, str(text))
    try:
        return GetMostSimilar(match.group(1), ls_group_stage) + " "+ match.group(2)
    except:
        logging.error("<STAGE> Unable to convert \'" + text + "\' to appropriate format like \'LEGEND 0-0\'")

def _hp(text):
    pattern = r'\\p{P}'
    no_punc = re.sub(pattern, '', text)
    return no_punc

# #use regex fix time
fix_col_with_fun(['Stage'], _stage)
# fix_col_with_fun(['Ingame_Time_Left'], _time)
fix_col_with_replace(['Player_HP_'+str(i) for i in range(10)], [str(x) for x in list(range(101))])

print(df_analysis['BO'].head(10))
