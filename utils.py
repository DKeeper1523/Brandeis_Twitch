import pandas as pd
import numpy as np
from statistics import mode
from fuzzywuzzy import process, fuzz
import re
import logging

#CONSTANTS
STAGE_NAME = "Stage"
T_STAMP = "Timestamp"

#init needed lambda's
#S2 here is a single 
subS1WithS2 = lambda s1, s2, txt, toLower = False: re.sub(r'[' + re.escape(s1) + ']', s2, txt) if toLower == False else re.sub('|'.join([*s1]), s2, txt.lower())
subZeros = lambda string, zero_target = 'oOu', toLower = False: subS1WithS2(zero_target,'0', string, toLower) 
subOnes = lambda string, one_target = 'ji', toLower = False: subS1WithS2(one_target,'1', string, toLower) 
subTwos = lambda string, two_target = 'z', toLower = False: subS1WithS2(two_target,'2', string, toLower) 

def printFull(df, NAME_DF = ""):
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
        print("PRINTING FULL " + NAME_DF + ": \n", df)

def init_log(log_name):
    logging.basicConfig(filename=log_name, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.FileHandler(log_name, mode='w')
    # print("logger initialized")

def insertTimers(df, ls_headrs):
    #   - str used here is saving info for further modification
    _insertEmptyColumns(df, ls_headrs, i_insert=5, _dtype = str) 

def insertMapScores(df, map_score_headers):
    _insertEmptyColumns(df, map_score_headers, i_insert=2, _dtype= int)

def insertDateAndStream(df, csv_name, loc=1):
    date = csv_name[csv_name.index('/')+1:csv_name.index('_')]
    stream = csv_name[csv_name.index(' ')+1]
    df.insert(loc, 'Stream', stream)
    df.insert(loc, 'Date', date)

#column with be inserted in order, first in list will be the last place inserted
def _insertEmptyColumns(df, ls_col_name, i_insert, _dtype):
    for col_name in ls_col_name:
        # print("inserted ", col_name)
        df.insert(loc = i_insert, column = col_name, value = pd.Series(dtype = _dtype))

def addStageSep(df):
    _addSpace = lambda txt, index = -3: txt[:index] + " " + txt[index:]
    #remove all space then add the space in the right place
    # df.loc[:,'Stage'] = df.Stage.str.replace(r'\s', '').apply(_addSpace)
    df.loc[:,'Stage'] = df.Stage.apply(lambda x: re.sub(r'\s+', '', x)).apply(_addSpace)


def fixBO1Stage(df):
    # BO is BO1 and stage has no digit
    bo1 = (df['BO'] == 'BO1')
    no_digit = ~(df['Stage'].str.contains(r'\d|-|\s', regex = True))
    zero_zero = " 0-0"
    df.loc[ bo1&no_digit, 'Stage'] = df.loc[ bo1&no_digit, 'Stage'] + zero_zero

def fixBO3(df):
    no_zero = df['Stage'].str.contains(r'[1-9]', regex = True)
    no_zero.fillna(False, inplace = True)
    df.loc[no_zero, 'BO'] = 'BO3'

def fillNaCols(df, ls_header, bfill = True, fFill = False):
    for h in ls_header:
        if fFill:
            df[h].ffill(inplace=True)
        if bfill:
            df[h].bfill(inplace=True)

def convertCols2Numeric(df, ls_header, _errors = 'ignore'):
    df.loc[:,ls_header] = df.loc[:,ls_header].apply(lambda dataframe: pd.to_numeric(dataframe, errors= _errors, downcast='integer'))

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
            if mode == "":
                logging.warn("mode is empty string")
        except:
            logging.error("unable to find mode in group")
            return
        df.loc[:, col] = mode

#needs a function that operate on cell, so "func" needs to return a string
def fix_col_with_fun(df, cols, func):
    for col_name in cols:
        df[col_name] = df[col_name].apply(func)

#To fix time
#   - we are preparing to fix time: (1) convert everything to a float number, as they r ez
def time(txt):
    #use regex fix time
    def subChar (txt):
        return re.sub(r'(\w+)\W(\w+)', r'\1.\2', str(txt))
    formatted = subChar(subOnes(subZeros(str(txt), toLower=True)))
    return formatted

def stage(text, true_groupstages):
    TARGETS_ONE = '!ije'
    TARGETS_TWO = 'z?7s'

    #strip white space
    no_space = text.strip().lower()
    # pattern = r'(\S+)\s([\d|o|O])\s?\W?\s?([\d|o|O])'
    pattern = r'(\S+)\s((.*))'

    match = re.search(pattern, str(no_space))
    if match is None:
        logging.error("[stage() match-stage Failed]: <'Stage'> Unable to convert \'" + text + "\' to appropriate format like \'LEGEND 0-0\'")
        # print("[stage() match-stage Failed]: <'Stage'> Unable to convert \'" + text + "\' to appropriate format like \'LEGEND 0-0\'")
    else:
        #find closest matched stage
        group1 = mapMostSimilar(match.group(1), true_groupstages)
        scores = match.group(2).strip()
        #replacing possibles for 0 and 1
        scores = subTwos(subOnes(subZeros(scores), TARGETS_ONE), TARGETS_TWO)
        #replace 2
        scores = re.sub(TARGETS_TWO, '2', scores)
        formatted = scores[0] + '-' + scores[-1]
        if not scores[0].isdigit() or  not scores[0].isdigit():
            # print("WARNING: [stage() digit-recognition]: original text", text, " scores: ", scores[0] + ';' + scores[-1])
            logging.error("[stage() digit-recognition]: scores: " + str(scores))
        return group1.capitalize() + " "+ formatted

#hp return integer
def hp(text):
    #Define helper functions
    # 10/7/2023 cut off is lowered to 66
    def isNum(text, str_num, cutoff = 99, match_partial = False):
        ratio_100 = fuzz.partial_ratio('100', text) if match_partial else fuzz.ratio(str_num, text)
        return True if ratio_100 > cutoff else False
    
    punc_pattern = r'\W'
    # Remove all punctuation
    no_punc = re.sub(punc_pattern, '', str(text)).strip().lower() #remove white space, convert to lower

    zero_ready = subZeros(no_punc)
    one_ready = subOnes(zero_ready)
    
    #match 100 as it counts for significant portion of data, and lots of error
    if isNum(one_ready, '0'):
        return 0
    elif isNum(one_ready, '100', match_partial=True):
        return 100
    
    #drop all letters
    only_digits = re.findall(r'\d', one_ready)
    only_digits = ''.join(only_digits)[:2]
    if len(only_digits) == 0:
        int_hp = np.nan
    else:
        int_hp = int(only_digits[:2])
    
    #if digits are greater than 100, remove the last digit
    if int_hp > 100:
        int_hp = int_hp//10
    return int_hp

#splitting stage into three different columns
def split_stage(df, t0_score_header, t1_score_header):
    if df['Stage'].isna().all():
        return False
    else:
        #first split
        try:
            #           Here n=1 allows for split when cell is NA
            df[['Stage', 'Stage_Scores']] = df['Stage'].str.split(' ', n=1, expand=True)
        except:
            logging.error("split_stage Failed: unable to split Stage (", str(df['Stage']) ,") into 3; probably because group is too small")
        #     return False
        #second split
        df[[t1_score_header, t0_score_header]] = df['Stage_Scores'].str.split('-', n = 1, expand = True)
        #Move column
        for col_name in [t1_score_header, t1_score_header]:
            col = df.pop(col_name)
            df.insert(2, col_name, pd.to_numeric(col, errors='coerce')) #converted to integer
            setCol2Mode(df, [col_name])
        #dropping the Stage_Scores
        df.drop('Stage_Scores', axis = 1, inplace = True)
        return True
