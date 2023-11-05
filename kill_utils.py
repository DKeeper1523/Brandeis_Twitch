from utils import printFull

import pandas as pd
import numpy as np

HP_HEADERS = ['Player_HP_'+str(i) for i in range(10)]

HP_TEAM0 = HP_HEADERS[:5]
HP_TEAM1 = HP_HEADERS[5:]

def count_kills(df_round):
    _GetFirst = lambda column, target=0: (column == target).idxmax()

    index_t0_kill = df_round.loc[:, HP_TEAM0].apply(_GetFirst, axis=0)
    index_t1_kill = df_round.loc[:, HP_TEAM1].apply(_GetFirst, axis=0)

    def GetKills(index_kill):
        kills = np.zeros(df_round.shape[0])
        kills.index = df_round.index
        for i in index_kill:
            kills.iloc[i:] += 1
        kills = kills.cumsum()

        return pd.Series(kills, dtype=int)

    NUM_PLAYER = 5
    # t0_kill = GetKills(index_t0_kill) + NUM_PLAYER
    # t1_kill = GetKills(index_t0_kill) + NUM_PLAYER

    t0_alive = NUM_PLAYER - GetKills(index_t1_kill) + NUM_PLAYER
    t1_alive = NUM_PLAYER - GetKills(index_t0_kill)

    df_round.loc[:, 'Alive_0'] = t0_alive
    df_round.loc[:, 'Alive_1'] = t1_alive



