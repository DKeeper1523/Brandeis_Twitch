import pandas as pd
from tqdm import tqdm

from collections import OrderedDict

path_hltv = "clean\\formatted_hltv_scrape.csv"
path_all = "clean\\all.csv"

df_hltv = pd.read_csv(path_hltv)
df_all = pd.read_csv(path_all)

#convert dict hltiv
hltv_group_key = ['Date', 'Map_ID', 'T0', 'T1']
hltv_groupby = df_hltv.groupby(hltv_group_key, sort=False)

#init dict hltv
dict_hltv = OrderedDict()
for n, group in hltv_groupby:
    date = n[0]
    id_teams= n[1:]
    dict_hltv[date] = {teams: group}

print('first_level_keys',dict_hltv.keys())
temp = [list(dict_hltv[key].keys()) for key in dict_hltv.keys()]
print('second_level keys', temp)

print('----------------')
example = temp[0][0]
print('example', example)
print(dir(example))
print('Vitality' in example)