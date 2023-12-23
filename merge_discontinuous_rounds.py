import pandas as pd
from utils import *

from sliding_window import slidingWindow

def _cleanRoundFromPrev(df_dirty, df_prev):
    #get the last row of df_prev except the time stamp column
    last_row = df_prev.iloc[-1, 1:]
    #set all row of df_dirty to last_row (except the time stamp column)
    df_dirty.iloc[:, 1:] = last_row.values
        
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
                mid_round = W.getMid()
                #clean mid round based on the first round
                _cleanRoundFromPrev(mid_round, first_round)
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

if __name__ == "__main__":
    path = "/Users/tianyaoh/Desktop/dev/CS_Twitch/rawdata/2023-05-10_Stream A/video_analysis.csv"
    df_raw = pd.read_csv(path)

    path_info = '/Users/tianyaoh/Desktop/dev/CS_Twitch/Brandeis_Twitch_RA/basic_information.csv'
    df_info = pd.read_csv(path_info)

    ALL_TEAM_NAME =  list(df_info['Team']) + [' ']

    start_index = 8863
    end_index = 9020

    show_column = ['Timestamp', 'Stage', 'Map', 'Ingame_Time_Left', 'Score_0', 'Score_1']
    interested_column = df_raw.columns
    target_range = df_raw.loc[start_index:end_index, interested_column]
    #forceprint everything
    pd.set_option('display.max_rows', None)
    for round in merge_disontinuous_rounds(target_range):
        print(round.loc[:, show_column])
        print('--------------------------------------------------')
    print(target_range[show_column + ['Has Stage']])