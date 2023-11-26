import pandas as pd
from collections import OrderedDict

#from utils import fix_col_with_replace

path_hltv = "clean\\hltv_scrape.csv"

df_hltv = pd.read_csv(path_hltv)

#import base csv
path_base = 'Brandeis_Twitch_RA\\basic_information.csv'
df_base = pd.read_csv(path_base)


#cleaning of df_hltv
#(1) change date format
#convert date to datetime
df_hltv['Date'] = pd.to_datetime(df_hltv['Date'], dayfirst=True).dt.strftime('%Y-%m-%d')

#(2) add map_id col to df_hltv
map_id_keys = ['T0', 'T1', 'Map']
map_id = ((df_hltv[map_id_keys].shift() != df_hltv[map_id_keys]).any(axis=1)).cumsum()
df_hltv.insert(1, 'Map_ID', map_id)

df_hltv['Score_Stream_T0'] = df_hltv['Score_T0']
df_hltv['Score_Stream_T1'] = df_hltv['Score_T1']

#print unique value from outcome
print('df_hltv[Outcome].unique()', df_hltv['Outcome'].unique())
CT_WINS = ['stopwatch', 'bomb_defused', 'ct_win']
#T_WINS = ['t_win', 'bomb_exploded']
i = 0 
for index in df_hltv.index:
    if df_hltv.loc[index, 'Outcome'] in CT_WINS: 
        bool_t0_ct = df_hltv.loc[index, 'Side_T0'] == 'ct'
        if bool_t0_ct:
            df_hltv.loc[index, 'Score_Stream_T0'] -= 1
        else:
            df_hltv.loc[index, 'Score_Stream_T1'] -= 1
    else: #t wins
        bool_t0_t = df_hltv.loc[index, 'Side_T0'] == 't'
        if bool_t0_t:
            df_hltv.loc[index, 'Score_Stream_T0'] -= 1
        else:
            df_hltv.loc[index, 'Score_Stream_T1'] -= 1


print("df_hltv", df_hltv)

# df_hltv.to_csv('clean\\test_hltv_scrape.csv', index=False, date_format='%Y-%m-%d')

df_hltv.to_csv('clean\\formatted_hltv_scrape.csv', index=False, date_format='%Y-%m-%d')


#convert dict hltiv
# hltv_group_key = ['Date', 'T0', 'T1', 'Map']
# hltv_groupby = df_hltv.groupby(hltv_group_key, sort=False)

# #init dict hltv
# dict_hltv = OrderedDict()
# for n, group in hltv_groupby:
#     dict_hltv[n] = group

# print([x for x in dict_hltv.keys() if 'Natus Vincere' in x[2]])


"""


specific_date = '2023-08-05'
matches_on_date = [x for x in dict_hltv.keys() ]
"""

