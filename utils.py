import pandas as pd
import numpy as np
from statistics import mode
from fuzzywuzzy import process, fuzz
import re
import logging

#CONSTANTS
STAGE_NAME = "Stage"
T_STAMP = "Timestamp"

def init_log(log_name):
    logging.basicConfig(filename=log_name, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.FileHandler(log_name, mode='w')
    print("logger initialized")

def insertBombTimer(df, bt_header):
    #   - str used here is saving info for further modification
    _insertEmptyColumns(df, [bt_header], i_insert=5, _dtype = str) 

def insertMapScores(df, map_score_headers):
    _insertEmptyColumns(df, map_score_headers, i_insert=2, _dtype= int)

#column with be inserted in order, first in list will be the last place inserted
def _insertEmptyColumns(df, ls_col_name, i_insert, _dtype):
    for col_name in ls_col_name:
        print("inserted ", col_name)
        df.insert(loc = i_insert, column = col_name, value = pd.Series(dtype = _dtype))

def bfillCols(df, ls_header):
    for h in ls_header:
        df[h].bfill(inplace=True)

def convertCols2Numeric(df, ls_header, _errors = 'ignore'):
    for h in ls_header:
        df[h] = pd.to_numeric(df[h], errors= _errors, downcast='integer')

def groupDf(df):
    def _group_consec_int(ints):
        int_diff = ints.diff()
        groups = (int_diff > 1).cumsum()
        return groups
    grouped_df = df.groupby(_group_consec_int(df[df[STAGE_NAME].notnull()][T_STAMP]))
    return grouped_df

def mapMostSimilar(base, ls_guess, guess_up = False, guess_low = False, n = 1, cutoff = 0.01):
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
        most_likely = mapMostSimilar(str(name).lower(), ls_target_name)
        dict_truth[name] = most_likely
    return dict_truth

#To fix certain column with an iterable ground truth
def fix_col_with_replace(df, ls_col, ls_truth, setCol2Mode_ = False):
    #Collect all the columns that needs to be fixed
    #   - assume that they are in the same domain
    #       -e.g play1_name, player2_name are within the same domain; map and hp are not in the same domain
    ls_bad = set()
    for column_name in ls_col:
        ls_bad = ls_bad|set(df[column_name])

    dict_guess = createDictGuess(ls_bad, ls_truth)
    for key in dict_guess.keys():
        for columnName in ls_col:
            df[columnName] = df[columnName].replace(key, dict_guess[key])

    #set the entire column to be the mode of the column
    if setCol2Mode_:
        setCol2Mode(df, ls_col)

#set the entire colume to its respective mode (most frequent value)
def setCol2Mode(df, ls_col):
    for col in ls_col:
        try:
            mode = df[col].mode()[0]
        except:
            logging.error("unable to find mode in group")
            return
        df[col] = mode
        #fill na
        df[col].fillna(mode)

#needs a function that operate on cell, so "func" needs to return a string
def fix_col_with_fun(df, cols, func):
    for col_name in cols:
        df[col_name] = df[col_name].apply(func)

#S2 here is a single 
subS1WithS2 = lambda s1, s2, txt, toLower = False: re.sub('|'.join([*s1]), s2, txt) if toLower == False else re.sub('|'.join([*s1]), s2, txt.lower())
subZeros = lambda string, zero_target = 'oOu', toLower = False: subS1WithS2(zero_target,'0', string, toLower) 
subOnes = lambda string, one_target = 'ji', toLower = False: subS1WithS2(one_target,'1', string, toLower) 

#To fix time
#   - we are preparing to fix time: (1) convert everything to a float number, as they r ez
def time(txt):
    #use regex fix time
    def subChar (txt):
        return re.sub(r'(\w+)\W(\w+)', r'\1.\2', str(txt))
    formatted = subChar(subOnes(subZeros(str(txt), toLower=True)))
    return formatted

# #Fix Stage
def stage(text, true_groupstages):
    #strip white space
    text = text.strip()
    pattern = r'(\S+\s?)([\d|o|O])\s?\W?\s?([\d|o|O])'
    match = re.search(pattern, str(text))
    if match is None:
        logging.error("[mapMostSimilar Failed]: <'Stage'> Unable to convert \'" + text + "\' to appropriate format like \'LEGEND 0-0\'")
        #Here we can return a placeholder for unreadable Stage name
        #return '[Unreadable stage]'
    else:
        #find closest matched stage
        group1 = mapMostSimilar(match.group(1), true_groupstages)
        map_zero = {'o':'0','O':'0', '0':'0'}
        comb2 = map_zero[match.group(2)] + '-' + map_zero[match.group(3)]
        return group1 + " "+ comb2

#hp return integer
def hp(text):
    #Define helper functions
    # 10/7/2023 cut off is lowered to 66
    def isNum(text, str_num, cutoff = 99, match_partial = False):
        ratio_100 = fuzz.partial_ratio('100', text) if match_partial else fuzz.ratio(str_num, text)
        return True if ratio_100 > cutoff else False
    
    punc_pattern = r'\W'
    # Remove all punctuation
    no_punc = re.sub(punc_pattern, '', text).strip().lower() #remove white space, convert to lower

    zero_ready = subZeros(no_punc)
    one_ready = subOnes(zero_ready)
    
    #match 100 as it counts for significant portion of data, and lots of error
    if isNum(one_ready, '0'):
        return 0
    elif isNum(one_ready, '100', match_partial=True):
        return 100
    
    #drop all letters
    try:
        only_digits = int(re.sub('[a-z]','', one_ready)[:2])
        #if digits are greater than 100, remove the last digit
        if only_digits > 100:
            only_digits = only_digits//10
        return only_digits
    except:
        logging.error("<HP> Unable to convert \'" + text + "\' to appropriate hp integer \'[0-100]\'")

#splitting stage into three different columns
def split_stage(df, t0_score_header, t1_score_header):
    #first split
    try:
        df[['Stage', 'Stage_Scores']] = df['Stage'].str.split(' ', n = 1, expand=True)
    except:
        logging.error("split_stage Failed: unable to split Stage into 3; probably because group is too small")
        return False
    #second split
    df[[t1_score_header, t0_score_header]] = df['Stage_Scores'].str.split('-', n = 1, expand = True)
    #Move column
    for col_name in [t1_score_header, t1_score_header]:
        col = df.pop(col_name)
        df.insert(2, col_name, pd.to_numeric(col)) #converted to integer
        setCol2Mode(df, [col_name])
    #dropping the Stage_Scores
    df.drop('Stage_Scores', axis = 1, inplace = True)
    return True
