from utils import printFull
import pandas as pd
import numpy as np

HP_HEADERS = ['Player_HP_'+str(i) for i in range(10)]

HP_TEAM0 = HP_HEADERS[:5]
HP_TEAM1 = HP_HEADERS[5:]

def count_kills(df_round):
    def _GetFirst(column, target=0):
        match_target = column[column == target]
        if len(match_target) > 0:
            return match_target.idxmax() 

    #get index where a player's hp first turn into 0
    index_t0_kill = df_round.loc[:, HP_TEAM0].apply(_GetFirst, axis=0)
    index_t1_kill = df_round.loc[:, HP_TEAM1].apply(_GetFirst, axis=0)

    def GetKills(index_kill):
        kills = pd.Series(np.zeros(df_round.shape[0]), dtype=int)
        kills.index = df_round.index
        for i in index_kill:
            if not pd.isna(i):
                kills[i] += 1
        kills = kills.cumsum()
        return kills

    #get number of players alive
    NUM_PLAYER = 5
    t0_alive = GetKills(index_t1_kill)*(-1) + NUM_PLAYER
    t1_alive = GetKills(index_t0_kill)*(-1) + NUM_PLAYER

    df_round.loc[:, 'Alive_0'] = t0_alive
    df_round.loc[:, 'Alive_1'] = t1_alive


#TESTING:
if __name__ == "__main__":
    hp_test = np.tile(np.arange(1,11),(10,1)).T
    hp_test[0,0] = 0
    # hp_test[1,1] = 0
    hp_test[2,2] = 0
    hp_test[2,3] = 0
    hp_test[7,4] = 0

    df_test = pd.DataFrame(hp_test, columns=HP_HEADERS)
    df_test.index = pd.Index(np.arange(30,40))
    count_kills(df_test)

    printFull(df_test[['Alive_0','Alive_1']])
