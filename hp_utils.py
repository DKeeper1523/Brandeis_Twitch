import pandas as pd

#This function likes whoever is higher up in the column
#   e.g is column is [7, 10, 7, 10, 0, 10, 0, 0, 0, 0]
#       - the function match patter ABA and converts it to AAA, thus, 10 at index 1 is eliminated
#           column becomes: [7, 7, 7, 10, 0, 10, 0, 0, 0, 0]
#           window pose:     ^  ^  ^
#       - the function ensures that column is in descending order, by knocking the bigger value
#          to its smaller prev
#           e.g:
#               Here, since function FIRST sees 7, then 10, it replace 10 with 7
#           column becomes: [7, 7, 7, 7, 0, 10, 0, 0, 0, 0]
#           window pos:            ^  ^  ^
#               Since window now is in descresing order, window slides by 2
#           Window moves
#           column becomes: [7, 7, 7, 7, 0, 10, 0, 0, 0, 0]
#           window pos:                  ^   ^  ^
#               as one may have guessed, ABC pattern but window is not in descending order
#               since the smaller value 0 is seen first, 10 is replaced with 0
#           column becomes: [7, 7, 7, 7, 0, 0, 0, 0, 0, 0]
#           window pos:                  ^  ^  ^

#       FYI: The array here shown is just for illustration, in reality
#           this function commpressed consecutive values into groups
#           The actual group array that window uses is
#               NOT: [7, 10, 7, 10, 0, 10, 0, 0, 0, 0]
#      BUT Actually: [7, 10, 7, 10, 0, 10, [0, 0, 0, 0]]
#                                           ^^^^^^^^^^^<- this is one group
def ensureColsOrder(df, ls_hp, order = 'descend'):
    dict_order = {
        'descend' : lambda x1, x2: x1 < x2,
        'ascend' : lambda x1, x2: x1 > x2
    }
    _compare = dict_order[order]
    def _setN2P(index_n, value_n, value_p, window):
        window[index_n].replace(value_n, value_p, inplace = True)

    new = pd.DataFrame()
    for hp in ls_hp:
        # print("HP", hp)
        col = pd.DataFrame()
        window = [0, 0, 0]
        # print("RESET WINDOW")
        i = 0
        groups = df[hp].groupby([(df[hp] != df[hp].shift( )).cumsum()])
        for nth, gp in groups:
            # print("This is ", nth, "th group:")
            i_left = (i-2)%3
            i_center = (i - 1)%3
            i_right = i%3
            #insert to window
            window[i_right] = gp

            #Only enter the cleaning logic if window is filled
            if int not in [type(x) for x in window]:
                left = window[i_left].max()
                center = window[i_center].max()
                right = window[i_right].max()
                #combine
                v_window = [left, center, right]
                # print("left", left,"center", center,"right", right)
                # print("left == right", left == right)
                # print("sorted == v_window?".upper(), sorted(v_window, reverse=True) == v_window)
                #check for ABA patter, if so make B the same as A
                if left == right:
                    _setN2P(i_center, center, left, window)
                #check if window is not in descending order sorted
                #   -preserving from left to right
                elif sorted(v_window, reverse=True) != v_window:
                    #decsending order: left < center
                    # if left < center:
                    if _compare(left, center):
                        _setN2P(i_center, center, left, window)
                        center = left
                    if _compare(center, right):
                        _setN2P(i_right, right, center, window)
                        right = center
                else:
                    # print(v_window, " perfectly in decreasing order!")
                    pass
                together = pd.concat([window[i_left], window[i_center]])
                #update col
                col = pd.concat([col, together])
                #   - Here, .reset_index() is to mitigate an ERROR(dexError: Reindexing only valid with uniquely valued Index objects) 
                # col.reset_index(inplace=True, drop=True)
                #update window: since currently on right, reset center and left.
                window[i_center] = 0
                window[i_left] = 0
            #inc counter
            i+=1
        #cleaning up
        final =  pd.concat([x for x in window if type(x) is not int])
        col = pd.concat([col, final])
        # col.reset_index(inplace=True, drop=True)

        #add to 
        new =  pd.concat([new, col], axis = 1)
        # print("NEW\n", new)
    #add headers
    new.columns = ls_hp
    #update original df
    for hp in ls_hp:
        df[hp] = new[hp]
    # print("Below is updated\n ", new.tail(20), "\n Updated is finished\n ", df.tail(20))

#For testing purposes:
if __name__ == "__main__":
    data = {
        "HP1" : [7, 10, 7, 10, 0, 10, 0, 0, 0, 0],
        "HP2" : [10, 10, 7, 10, 0, 10, 0, 0, 0, 0],
        "HP3" : [10, 10, 7, 10, 0, 10, 0, 0, 0, 0],
        "HP4" : [10, 10, 7, 10, 0, 10, 3, 2, 0, 0],
        "HP5" : [10, 10, 7, 10, 0, 10, 0, 0, 0, 0],
        "HP6" : [10, 10, 7, 10, 0, 10, 0, 0, 0, 0],
    }
    
    data = pd.DataFrame(data)
    
    LS_HP = ["HP" + str(x) for x in range(1, 7)]

    ensureColsOrder(data, LS_HP)

    print(data)