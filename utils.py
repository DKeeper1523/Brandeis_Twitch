import pandas as pd
import numpy as np
from fuzzywuzzy import process, fuzz
import re
import logging

#custom functions for round cleaning
from split_conjoined_round import merge_disontinuous_rounds
from split_conjoined_round import split_conjoined_round

#CONSTANTS
STAGE_NAME = "Stage"
T_STAMP = "Timestamp"

#init needed lambda's
#lambda here is mainly for cleaninng up fuzzy round scores
subS1WithS2 = lambda s1, s2, txt, toLower = False: re.sub(r'[' + re.escape(s1) + ']', s2, txt) if toLower == False else re.sub('|'.join([*s1]), s2, txt.lower())
subZeros = lambda string, zero_target = 'oOu', toLower = False: subS1WithS2(zero_target,'0', string, toLower) 
subOnes = lambda string, one_target = 'ji', toLower = False: subS1WithS2(one_target,'1', string, toLower) 
subTwos = lambda string, two_target = 'z', toLower = False: subS1WithS2(two_target,'2', string, toLower) 

def printFull(df, NAME_DF = ""):
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
        print("PRINTING FULL " + NAME_DF + ": \n", df)

# Initializes a logger to record debug messages to a file.
def init_log(log_name):
    logging.basicConfig(filename=log_name, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.FileHandler(log_name, mode='w')
    # print("logger initialized")

# Insert columns for timing into a DataFrame at specified positions.
def insertTimers(df, ls_headrs):
    #   - str used here is saving info for further modification
    _insertEmptyColumns(df, ls_headrs, i_insert=5, _dtype = str) 

# Insert columns for map scores into a DataFrame at specified positions.
def insertMapScores(df, map_score_headers):
    _insertEmptyColumns(df, map_score_headers, i_insert=2, _dtype= int)

# Extracts date and stream information from a `csv_name` string and inserts them into a DataFrame.
def insertDateAndStream(df, csv_name, loc=1):
    date = csv_name[csv_name.index('/')+1:csv_name.index('_')]
    stream = csv_name[csv_name.index(' ')+1]
    df.insert(loc, 'Stream', stream)
    df.insert(loc, 'Date', date)

# Extracts date and stream information from a `csv_name` string and inserts them into a DataFrame.
def _insertEmptyColumns(df, ls_col_name, i_insert, _dtype):
    for col_name in ls_col_name:
        # print("inserted ", col_name)
        df.insert(loc = i_insert, column = col_name, value = pd.Series(dtype = _dtype))

# Adds a space in the stage information at a specified index to ensure proper formatting.
def addStageSep(df):
    _addSpace = lambda txt, index = -3: txt[:index] + " " + txt[index:]
    #remove all space then add the space in the right place
    df.loc[:, 'Stage'] = df.loc[:, 'Stage'].apply(lambda x: re.sub(r'\s+', '', str(x))).apply(_addSpace)


# Corrects stage information based on match format (BO1 or BO3).
def fixBO1Stage(df):
    # BO is BO1 and stage has no digit
    bo1 = (df['BO'] == 'BO1')
    no_digit = ~(df['Stage'].astype(str).str.contains(r'\d|-|\s', regex=True))
    zero_zero = " 0-0"
    df.loc[ bo1&no_digit, 'Stage'] = df.loc[ bo1&no_digit, 'Stage'] + zero_zero

def fixBO3(df):
    no_zero = df['Stage'].str.contains(r'[1-9]', regex = True)
    no_zero.fillna(False, inplace = True)
    df.loc[no_zero, 'BO'] = 'BO3'


# Fills NA/NaN values in specified columns with forward or backward filling.
def fillNaCols(df, ls_header, bfill = True, fFill = False):
    for h in ls_header:
        if fFill:
            df[h].ffill(inplace=True)
        if bfill:
            df[h].bfill(inplace=True)

# Converts specified columns to numeric types, with an option to fill missing values with zeros.
def convertCols2Numeric(df, ls_header, _errors = 'ignore', bool_fillzero = False):
    df.loc[:,ls_header] = df.loc[:,ls_header].apply(pd.to_numeric, errors=_errors, downcast='integer')
    #fill na
    if bool_fillzero:
        df.loc[:, ls_header] = df.loc[:, ls_header].fillna(value=0)

# Groups a DataFrame based on discontinuous rounds and splits conjoined rounds.
def groupDf(df):
    # grouped_df = df.groupby((df[T_STAMP].diff()>1).cumsum(), sort=False) #BEFORE 12/22/2023

    grouped_df = (round for df_merged in merge_disontinuous_rounds(df) for round in split_conjoined_round(df_merged))
    return grouped_df

# Finds the most similar string in a list to a base string, with options for case sensitivity and a cutoff for match quality.
def mapMostSimilar(base, ls_guess, guess_up = False, guess_low = False, cutoff = 30):
    ls_guess = list(ls_guess)
    if guess_up:
        ls_guess = [x.upper() for x in ls_guess]
    elif guess_low:
        ls_guess = [x.lower() for x in ls_guess]
    
    if isinstance(base, str):
        guess = process.extractOne(base, ls_guess, score_cutoff=cutoff)
        return guess[0] if guess is not None else ''
    else:
        return ''


# Replaces values in specified columns based on the closest match from a list of truth values. It can also set the entire column to the mode.
def fix_col_with_replace(df, ls_col, ls_truth, setCol2Mode_ = False):
    #Collect all the columns that needs to be fixed
    #   - assume that they are in the same domain
    #       -e.g play1_name, player2_name are within the same domain; map and hp are not in the same domain
    ls_bad = pd.unique(df[ls_col].values.ravel('K'))

    for bad in ls_bad:
        guess = mapMostSimilar(bad, ls_truth)
        df.loc[:,ls_col] = df.loc[:,ls_col].replace(bad, guess)

    #set the entire column to be the mode of the column
    if setCol2Mode_:
        setCol2Mode(df, ls_col)

#set the entire colume to its respective mode (most frequent value)
def setCol2Mode(df, ls_col):
    for col in ls_col:
        try:
            mode = df.loc[df[col].notna(), col].mode()[0]
            if mode == "":
                logging.warn("mode is empty string")
        except:
            logging.error("unable to find mode in group")
            return
        df.loc[:, col] = mode

# Applies a custom function to fix values in specified columns.
def fix_col_with_fun(df, cols, func):
    for col_name in cols:
        df[col_name] = df[col_name].apply(func)


###### Functions designed to clean and format specific types of data related to time, stage information, and health points.
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
    #first split
    try:
        #           Here n=1 allows for split when cell is NA
        df[['Stage', 'Stage_Scores']] = df['Stage'].str.split(' ', n=1, expand=True)
    except:
        logging.error("split_stage Failed: unable to split Stage (", str(df['Stage']) ,") into 3; probably because group is too small")

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


