import pandas as pd
from collections import OrderedDict

#from utils import fix_col_with_replace

# path_hltv = "clean_data\\hltv_scrape.csv"
path_hltv = "/Users/tianyaohu/Desktop/dev/CS_Twitch/clean_data/hltv_scrape.csv"

df_hltv = pd.read_csv(path_hltv)

#import base csv
path_base = '/Users/tianyaohu/Desktop/dev/CS_Twitch/Brandeis_Twitch_RA/basic_information.csv'
df_base = pd.read_csv(path_base)

#cleaning of df_hltv
#(1) change date format
#convert date to datetime
df_hltv['Date'] = pd.to_datetime(df_hltv['Date'], format="%d/%m/%y").dt.strftime('%Y-%m-%d')

#(2) add map_id col to df_hltv
map_id_keys = ['T0', 'T1', 'Map']
map_id = ((df_hltv[map_id_keys].shift() != df_hltv[map_id_keys]).any(axis=1)).cumsum()
df_hltv.insert(1, 'Map_ID', map_id)

#insert BO column in the last column
df_hltv.insert(len(df_hltv.columns), 'BO', 'BO1')

def adjust_stream_score(df, ls_indices):
    CT_WINS = ['stopwatch', 'bomb_defused', 'ct_win']
    #T_WINS = ['t_win', 'bomb_exploded']
    for index in ls_indices:
        if df.loc[index, 'Outcome'] in CT_WINS: 
            bool_t0_ct = df.loc[index, 'Side_T0'] == 'ct'
            if bool_t0_ct:
                df.loc[index, 'Score_Stream_T0'] = df.loc[index, 'Score_T0'] - 1
                df.loc[index, 'Score_Stream_T1'] = df.loc[index, 'Score_T1']
            else:
                df.loc[index, 'Score_Stream_T0'] = df.loc[index, 'Score_T0'] 
                df.loc[index, 'Score_Stream_T1'] = df.loc[index, 'Score_T1'] - 1
        else: #t wins
            bool_t0_t = df.loc[index, 'Side_T0'] == 't'
            if bool_t0_t:
                df.loc[index, 'Score_Stream_T0'] =  df.loc[index, 'Score_T0'] - 1
                df.loc[index, 'Score_Stream_T1'] = df.loc[index, 'Score_T1']
            else:
                df.loc[index, 'Score_Stream_T0'] = df.loc[index, 'Score_T0'] 
                df.loc[index, 'Score_Stream_T1'] =  df.loc[index, 'Score_T1'] - 1
    return df

def injectBO(df, prev_info, curr_info, prev_index, curr_index):
    if prev_info is not None:
        #preserving only date, t0, t1 for both prev and curr
        prev_no_map = prev_info[:1] + prev_info[2:]
        curr_no_map = curr_info[:1] + curr_info[2:]
        if set(prev_no_map) == set(curr_no_map):
            df.loc[prev_index, 'BO'] = 'BO3'
            df.loc[curr_index, 'BO'] = 'BO3'
            # print('df.loc[prev_index, BO]', df.loc[prev_index, 'BO'])
            # print('df.loc[curr_index, BO]', df.loc[curr_index, 'BO'])
            # raise Exception('STOP')

def swap_score(df, index):
    df.loc[index, ['Score_T0', 'Score_T1']]= df.loc[index, ['Score_T1', 'Score_T0']].values
    return df

def score_alignment_checker(df, indices):
    CT_WINS = ['stopwatch', 'bomb_defused', 'ct_win']

    prev_score_t0 = 0
    prev_score_t1 = 1


    for index in indices:
        # print('index', index)

        score_t0_diff_one = (df.loc[index, 'Score_T0'] - prev_score_t0) == 1
        score_t1_diff_one = (df.loc[index, 'Score_T1'] - prev_score_t1) == 1

        #update prev score
        prev_score_t0 = df.loc[index, 'Score_T0']
        prev_score_t1 = df.loc[index, 'Score_T1']

        if df.loc[index, 'Outcome'] in CT_WINS: 

            bool_t0_ct = df.loc[index, 'Side_T0'] == 'ct'
            #CT WON: if t0 is ct and score_t0 is not one, swap, or if t0 is not ct and score_t0 is one, swap
            if bool_t0_ct and score_t1_diff_one or not bool_t0_ct and score_t0_diff_one: 
                #swap score 1 with score 0
                df =swap_score(df, index)
        else: #t wins
            bool_t0_t = df.loc[index, 'Side_T0'] == 't'

            # print('bool_t0_t', bool_t0_t)
            # print("bool_t0_t and score_t1_diff_one or not bool_t0_t and score_t0_diff_one", bool_t0_t and score_t1_diff_one or not bool_t0_t and score_t0_diff_one)

            #T WON: if t0 is ct and score_t0 is not one, swap
            if bool_t0_t and score_t1_diff_one or not bool_t0_t and score_t0_diff_one: 
                #swap score 1 with score 0
                df = swap_score(df, index)



map_groups = df_hltv.groupby(['Date', 'Map', 'T0', 'T1'], sort=False)

prev_info = None
prev_indices = None

i = 0

num_row = 40

print("before")
print('df_hltv', df_hltv.loc[ df_hltv.index[:num_row], ['T0', 'T1', 'Side_T0', 'Side_T1', 'Outcome', 'Score_T0', 'Score_T1']])

# score_alignment_checker(df_hltv)

# print("after")
# print('df_hltv', df_hltv.loc[ df_hltv.index[:num_row], ['T0', 'T1', 'Side_T0', 'Side_T1', 'Outcome', 'Score_T0', 'Score_T1']])

for match_info, df_match in map_groups:
    #print indicationn for processing
    print('match_info', match_info)

    #align score
    score_alignment_checker(df_hltv, df_match.index)

    #add stream score after alignment
    # df_hltv['Score_Stream_T0'] = df_hltv['Score_T0']
    # df_hltv['Score_Stream_T1'] = df_hltv['Score_T1']

    # inject stream score
    df_hltv = adjust_stream_score(df_hltv, df_match.index)
    
    injectBO(df_hltv, prev_info, match_info, prev_indices, df_match.index)

    # set prev
    prev_info = match_info
    prev_indices = df_match.index


########### TESTING SWAPPER #############
# print("after")
# print('df_hltv', df_hltv.loc[ df_hltv.index[:num_row], ['T0', 'T1', 'Side_T0', 'Side_T1', 'Outcome', 'Score_T0', 'Score_T1']])


# df_hltv.to_csv('clean_data/formatted_hltv_scrape.csv', index=False)
df_hltv.to_csv('/Users/tianyaohu/Desktop/dev/CS_Twitch/clean_data/formatted_hltv_scrape.csv', index=False)


