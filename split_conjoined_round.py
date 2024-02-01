import pandas as pd
from utils import *

from merge_discontinuous_rounds import merge_disontinuous_rounds
from sliding_window import slidingWindow

#The aim here is to split rounds that has multiple rounds (runing continuously)
def split_conjoined_round(df_has_stage):
    MINIMUM_ROUND_LENGTH = 20
    if len(df_has_stage) <= MINIMUM_ROUND_LENGTH:
        return df_has_stage
    else:
        #create mask column with three conditions: cell, previous, and next cell has number
        cur_has_number = df_has_stage['Ingame_Time_Left'].str.contains(r'\d').fillna(False)
        prev_has_number = df_has_stage['Ingame_Time_Left'].shift(1).str.contains(r'\d').fillna(False)
        nxt_has_number = df_has_stage['Ingame_Time_Left'].shift(-1).str.contains(r'\d').fillna(False)

        #any two of the three conditions are true
        mask = sum([cur_has_number, prev_has_number, nxt_has_number]) >= 2
        df_has_stage.loc[:, 'Has_Num'] = mask

        #init sliding window
        bool_window = slidingWindow(3)
        round_window = slidingWindow(3)

        #groupby based on consecutive values of cur_prev_Has_Number
        ls_indices = list(mask.groupby(mask.diff().ne(0).cumsum(),sort=False).groups.values())

        for i in range(len(ls_indices)+1):
            bool_has_num = df_has_stage.loc[ls_indices[i][0], 'Has_Num'] if i < len(ls_indices) else None
            round = df_has_stage.loc[ls_indices[i],:] if i < len(ls_indices) else None
            #assuming that both bool window and round window are of the same size
            if bool_window.isFull():
                #first match the size 3 pattern with bool window
                if bool_window.getData() == [False, True, False]:
                    #concatenate rounsd in round window, and yield df without "has_num" column
                    yield pd.concat(round_window.getData(), axis=0).drop('Has_Num', axis = 1)
                    #clear round window
                    bool_window.earseAll()
                    round_window.earseAll()
                    continue
                elif [bool_window.getFirst(), bool_window.getMid()] == [True, False]:
                    #concatenate rounsd in round window
                    yield pd.concat([round_window.getFirst(), round_window.getMid()], axis=0).drop('Has_Num', axis = 1)
                    #store the last round in temp
                    temp_round = round_window.getLast()
                    temp_bool = bool_window.getLast()
                    #clear round window
                    bool_window.earseAll()
                    round_window.earseAll()
                    #add last back to round window and bool window
                    bool_window.add(temp_bool)
                    round_window.add(temp_round)
                    continue
                #this means that there is un seen pattern, raise exception
                else:
                    print(bool_window.getData())
                    raise Exception("Unseen pattern in bool window")
            # if bool window is not full
            else: 
                #before add last, check if the last index of last round in window is 1 less than the first index of current round
                if not round_window.isEmpty() and round is not None:
                    if round_window.getLast().index[-1] + 1 != round.index[0]:
                        #if not, then yield all the round in current window window
                        yield pd.concat(round_window.getData(), axis=0).drop('Has_Num', axis = 1)
                        #clear round window
                        bool_window.earseAll()
                        round_window.earseAll()
                #add to bool window and round window
                bool_window.add(bool_has_num)
                round_window.add(round)

        #check if window is not empty, combine all rounds in window, and yield 
        if not bool_window.isEmpty():
            yield pd.concat(round_window.getData(), axis=0).drop('Has_Num', axis = 1)


if __name__ == "__main__":
    path = "/Users/tianyaoh/Desktop/dev/CS_Twitch/rawdata/2023-05-10_Stream A/video_analysis.csv"
    df_raw = pd.read_csv(path)

    path_info = '/Users/tianyaoh/Desktop/dev/CS_Twitch/Brandeis_Twitch_RA/basic_information.csv'
    df_info = pd.read_csv(path_info)

    ALL_TEAM_NAME =  list(df_info['Team']) + [' ']

    start_index = 9623
    end_index = 9785

    show_column = ['Timestamp', 'Stage', 'Map', 'Ingame_Time_Left']
    target_range = df_raw.loc[start_index:end_index, show_column]

    #init final
    final = []

    i = 0
    #force print everything in pandas dataframe
    pd.set_option('display.max_rows', None)
    for df_merged in merge_disontinuous_rounds(df_raw):
        for round in split_conjoined_round(df_merged):
            print(round.loc[:, show_column].iloc[[0,-1], :])
            print('---------')
            i += 1
        if i > 2:
            break
