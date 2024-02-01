import pandas as pd
from utils import *

from sliding_window import slidingWindow

def _get_mid_round( df_prev, range_start, range_end):
    mid_round = pd.DataFrame(index=range(range_start, range_end), columns=df_prev.columns)
    #set range
    mid_round.loc[:, 'Timestamp'] = range(range_start, range_end)
    #get the last row of df_prev except the time stamp column
    last_row = df_prev.iloc[-1, 1:]
    #set all row of df_dirty to last_row (except the time stamp column)
    mid_round.iloc[:, 1:] = last_row.values
    #return
    return mid_round

#the resulting df after merging should not have an empty cell in stage column
def merge_disontinuous_rounds(df):
    #mask has stage
    mask = df['Stage'].notnull()

    #group df by consecutive values of Has Stage
    group = df.groupby(mask.diff().ne(0).cumsum(),sort=False)

    #lambda get score at score from df
    GetScoreAtIndex = lambda df, pos_index: [x for x in df.loc[df.index[pos_index], ['Score_0', 'Score_1']].values if x not in ['', np.nan, None]]

    MAX_SEP = 16
    W = slidingWindow(3)

    #convert grouby to list of indicies
    ls_indicies =  list(group.groups.values())
    #iterate n+1 times to prevent full window exit from loop
    for i in range(len(ls_indicies)+1):
        round = df.loc[ls_indicies[i],:] if i < len(ls_indicies) else None
        #check if window is full
        if W.isFull():
            #get first and last round
            first_round = W.getFirst()
            last_round = W.getLast()
            #(1): check if the last index of the first round in window and the first index of the last round is within MAX_SEP
            first_round_last_index = first_round.index[-1]
            last_round_first_index = last_round.index[0]
            #(2): check the intersection bewteen current and last round's score is greater than 0
            first_round_score = GetScoreAtIndex(W.getFirst(), -1)
            last_round_score = GetScoreAtIndex(W.getLast(), 0)
            #if both conditions are met, combine first and last round and yield
            if  first_round_last_index + MAX_SEP >= last_round_first_index and sum([first in last_round_score for first in first_round_score]) > 0:
                #get mid round
                mid_round = _get_mid_round(first_round, first_round_last_index+1, last_round_first_index)
                #combine first and last round
                combined = pd.concat([first_round, mid_round,last_round], axis=0)
                yield combined
                #earse all
                W.earseAll()
                continue
            #the window is full, so we will 
            else:
                #check if the first round's stage is none
                if first_round.loc[:, 'Stage'].notnull().values.all():
                    #add current round
                    # print('first_round_last_index + MAX_SEP >= last_round_first_index', first_round_last_index + MAX_SEP >= last_round_first_index)
                    # print('first_round_score', first_round_score)
                    # print('last_round_score', last_round_score)
                    yield first_round
                #regardless of the first round's stage, we will add the current round
                W.add(round)
        else:
            W.add(round)
    # print('finished main loop')
    #yield all none None in window
    for frame in W.getData():
        #check if window is full and all stage is not null
        if frame is not None and frame['Stage'].notnull().values.all():
            yield frame

#============= Two part merge ==============

def two_part_merge(df1, df2):
    #get last index of df1 and first index of df2
    last_index_df1 = df1.index[-1]
    first_index_df2 = df2.index[0]

    #if there is seperation, we add the mid round
    if first_index_df2 - last_index_df1 > 1:
        #create mid round with the indices of df1 and df2
        mid_round = _get_mid_round(df1, last_index_df1+1, first_index_df2)
        return pd.concat([df1, mid_round, df2], axis=0)
    else:
        return pd.concat([df1, df2], axis=0)
        
def two_part_merge_logic(df1, df2):
    MAX_SEP = 16

    #get last index of df1 and first index of df2
    last_index_df1 = df1.index[-1]
    first_index_df2 = df2.index[0]

    #if df1's last index and df2's first index is within MAX_SEP
    # - AND df1's score and df2's score has at least one intersection
    score_intersc = sum([x in df2.loc[first_index_df2, ['Score_0', 'Score_1']].values for x in df1.loc[last_index_df1, ['Score_0', 'Score_1']].values])
    seperation = first_index_df2 - last_index_df1
    if seperation <= MAX_SEP and score_intersc > 0:
        return True
    else:
        return False

#============= End of Two part merge ==============


def merge_disontinuous_rounds_v2(df):
    all_sub_rounds = list(merge_disontinuous_rounds(df))

    ptr_frt = 0
    ptr_bck = 1

    for i in range(len(all_sub_rounds)-1):
        frt = all_sub_rounds[ptr_frt]
        bck = all_sub_rounds[ptr_bck]
        if two_part_merge_logic(frt, bck):
            all_sub_rounds[ptr_frt] = two_part_merge(frt, bck)
            #remove bck
            all_sub_rounds.pop(ptr_bck)
            #do not move pointer
        else:
            ptr_frt += 1
            ptr_bck += 1

    #yield all merged rounds from all_sub_rounds
    for round in all_sub_rounds:
        yield round


#============= Test ==============
def test_csv(path_to_data, start_index, end_index):
    df_raw = pd.read_csv(path_to_data)

    show_column = ['Timestamp', 'Stage', 'Map', 'Ingame_Time_Left', 'Score_0', 'Score_1']
    focus_column = df_raw.columns
    target_range = df_raw.loc[start_index:end_index, focus_column]
    #forceprint everything
    pd.set_option('display.max_rows', None)
    for round in merge_disontinuous_rounds_v2(target_range):
        print(round.loc[:, show_column])
        print('--------------------------------------------------')
    # print(target_range[show_column + ['Has Stage']])


if __name__ == "__main__":
    first_path_tested = "/Users/tianyaohu/Desktop/dev/CS_Twitch/rawdata/2023-05-10_Stream A/video_analysis.csv"

    # path_info = '/Users/tianyaohu/Desktop/dev/CS_Twitch/Brandeis_Twitch_RA/basic_information.csv'
    # df_info = pd.read_csv(path_info)
    # ALL_TEAM_NAME =  list(df_info['Team']) + [' ']

    # test_csv(first_path_tested, 8863, 9020)

    second_path_testing = "/Users/tianyaohu/Desktop/dev/CS_Twitch/rawdata/2023-05-08_Stream A/video_analysis.csv"
    test_csv(second_path_testing, 39776, 39920)


