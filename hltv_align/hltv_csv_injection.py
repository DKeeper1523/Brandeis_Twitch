import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz, process
from tqdm import tqdm

from collections import OrderedDict, defaultdict
import itertools

path_hltv = "clean\\formatted_hltv_scrape.csv"
path_all = "clean\\all.csv"

df_hltv = pd.read_csv(path_hltv)
df_all = pd.read_csv(path_all)

#To prepare for alignment, insert a column for alignment indication

df_all.insert(1, 'hltv_aligned?', False)

all_teams = np.concatenate([df_all[t].unique() for t in ['Team_0', 'Team_1']])# + df_all['Team_1'].unique()
current_teams = np.concatenate([df_hltv[t].unique() for t in ['T0', 'T1']]) #+ df_hltv['T1'].unique()

#remove spaces within all_teams and current_teams
all_teams = [x.replace(' ', '') for x in all_teams]
current_teams = [x.replace(' ', '') for x in current_teams]

#get teamname mapping from all to hltv
map_all2hltv = {t:process.extractOne(t, current_teams)[0] for t in all_teams}


#convert dict hltiv
hltv_group_key = ['Date', 'Map_ID', 'T0', 'T1', 'Map']
hltv_groupby = df_hltv.groupby(hltv_group_key, sort=False)

#init dict hltv
# dict_hltv = OrderedDict()
dict_hltv = defaultdict(dict)
for n, group in hltv_groupby:
    date = n[0]
    teams = n[1:]
    dict_hltv[date][teams] = group

# print('first_level_keys',dict_hltv.keys())
temp = [list(dict_hltv[key].keys()) for key in dict_hltv.keys()]
# print('second_level keys', temp)

# print('----------------')

# print("dict_hltv.keys()", dict_hltv.keys())


#FULL_HP_CONDITION: from player_hp_1 to player_hp_9, all 100
# CONDITION_FULL_HP = (df_all['Player_HP_0'] == 100) & (df_all['Player_HP_1'] == 100) & (df_all['Player_HP_2'] == 100) & (df_all['Player_HP_3'] == 100) & (df_all['Player_HP_4'] == 100) & (df_all['Player_HP_5'] == 100) & (df_all['Player_HP_6'] == 100) & (df_all['Player_HP_7'] == 100) & (df_all['Player_HP_8'] == 100) & (df_all['Player_HP_9'] == 100)

#STAGE2SKIP
STAGE2SKIP = ['Showmatch']

#create groupby for all.csv
all_groupkey = ['Date', 'Team_0', 'Team_1','Map']
all_groupby = df_all.groupby(all_groupkey, sort=False)

#init final: to store aligned data
final = pd.DataFrame(columns=df_all.columns)

for map_played, group in all_groupby:
    # print('map_played', map_played)
    #get stage
    stage = group['Stage'].unique()[0]
    #get match id
    game_id = group['GameID'].unique()[0]
    #strange case where team1==team2
    bool_sameteam = map_played[1] == map_played[2]
    if any([stage in STAGE2SKIP, bool_sameteam]):
        final = pd.concat([final, group], axis=0)
        continue

    # print('group', group)
    date = pd.to_datetime(map_played[0])
    plus_one = date + pd.Timedelta(days=1)
    ls_dates = [date, plus_one]
    ls_str_dates = [x.strftime('%Y-%m-%d') for x in ls_dates]
    #get team names
    t0_t1_map = [x.replace(' ', '') for x in map_played[1:]]
    t0_t1_map[:2] = [map_all2hltv[x] for x in t0_t1_map[:2]]

    #choose dated dict
    ls_dict = [dict_hltv[x] for x in ls_str_dates if x in dict_hltv.keys()]
    
    #get all possible keys
    ls_keys = [list(x.keys()) for x in ls_dict]

    #choose keys based on team names
    match = lambda key: sum([t in key for t in t0_t1_map])
    # print("t0_t1_map", t0_t1_map)
    THRESHOLD = 2
    # print('ls_keys', ls_keys)
    chosen_key = [[(key, match(key)) for key in keys if match(key)>=THRESHOLD] for keys in ls_keys]
    
    #max keys
    try:
        max_keys = [max(x, key=lambda x:x[1]) for x in chosen_key]
    except:
        print('failed finding acceptable keys for GameID', game_id)
        # print('Here are the keys shown in search', chosen_key)
        #temporary solution: skipping the game that failed to find acceptable keys
        final = pd.concat([final, group], axis=0)
        continue
    dict_index = max_keys.index(max(max_keys, key=lambda x:x[1]))
    #choose the max key
    max_key = max_keys[dict_index][0]
    count = max_keys[dict_index][1]
    # if count < 3:
        # print("Warning key found has incomplete matching information: num Match", count)
        # print("max_keys", max_keys)
        # print('max_key', max_key)

        # raise Exception()
    print('max_key', max_key)
    #hltv alignment data
    hltv_align = ls_dict[dict_index][max_key] ###########################IMPORTANT##################

    #drop used key
    ls_keys[dict_index].remove(max_key)

    #chop group into rounds based on HP
    index_fullHP = group["Ingame_Time_Passed"].index
    #consecutive index
    round_group = index_fullHP.groupby((group["Ingame_Time_Passed"].diff() != 1).cumsum())

    #before aligning data, first align team names
    bool_t0_aligned = hltv_align['T0'].unique()[0] == t0_t1_map[0]

    for num_round, round_index in round_group.items():
        #trusting FCFS; after checking stream, there is a replay of particular rounds
        #  - where the stream overlay is exactly the same as the normal match except it is replay
        #  - To avoid this, we will only trust the first n rounds played for all matches
        #  - As for program, we here check if the current round number is greater or equal to the hltv record
        if num_round < len(hltv_align):
            # print('num_round', num_round)

            #first get correct round index for hltv_align
            hltv_index = hltv_align.index[num_round-1] #num_round starts from 1

            #Align map
            group.loc[round_index,'Map'] = hltv_align.iloc[num_round-1]['Map']

            #align everything except Map
            if bool_t0_aligned:
                #BO
                # round['BO'] = hltv_align['BO']

                #round score
                group.loc[round_index,'Score_0'] = hltv_align.loc[hltv_index, 'Score_Stream_T0']
                group.loc[round_index,'Score_1'] = hltv_align.loc[hltv_index, 'Score_Stream_T1']

                #Map_score
                group.loc[round_index,'Team0_Map_Score'] = hltv_align.loc[hltv_index, 'Map_Score_T0']
                group.loc[round_index,'Team1_Map_Score'] = hltv_align.loc[hltv_index, 'Map_Score_T1']

                #Team0_Side
                group.loc[round_index,'Side_Team0'] = hltv_align.loc[hltv_index, 'Side_T0']
                group.loc[round_index,'Side_Team1'] = hltv_align.loc[hltv_index, 'Side_T1']
            else:
                group.loc[round_index,'Score_0'] = hltv_align.loc[hltv_index, 'Score_Stream_T1']
                group.loc[round_index,'Score_1'] = hltv_align.loc[hltv_index, 'Score_Stream_T0']

                #Map_score
                group.loc[round_index,'Team0_Map_Score'] = hltv_align.loc[hltv_index, 'Map_Score_T1']
                group.loc[round_index,'Team1_Map_Score'] = hltv_align.loc[hltv_index, 'Map_Score_T0']

                #Team0_Side
                group.loc[round_index,'Side_Team0'] = hltv_align.loc[hltv_index, 'Side_T1']
                group.loc[round_index,'Side_Team1'] = hltv_align.loc[hltv_index, 'Side_T0']

        # else:
        #     print('skipping last round: suspecting that the last round is a replay')

    #mark hltv aligned
    group.loc[:,'hltv_aligned?'] = True
    #combine
    final = pd.concat([final, group], axis=0)

#Finally, save the aligned data
final.to_csv('clean\\hltv_aligned.csv', index=False)
