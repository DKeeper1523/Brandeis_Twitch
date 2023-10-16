import pandas as pd
import numpy as np
from math import ceil
import sys

np.set_printoptions(threshold=sys.maxsize)

def truncateAll(df_text, df_audio, df_video):
    print("reached truncate all")
    len_text = len(df_text)
    truncateAudio(df_audio, len_text)
    truncateVideo(df_video, len_text)
    print("text:", len(df_text), "audio", len(df_audio), "video", len(df_video))
    print("\n\n\n\n\n")

def truncateAudio(df_audio, len_text, num_split = 5):
    diff = len(df_audio) - len_text
    #skipping if it is shorter than text csv
    if diff <= 0:
        pass
    else:

        audio_step = (len(df_audio) - diff)//num_split
        overflow = (len(df_audio) - diff) % num_split if audio_step > 0 else 0 #range from [0, num_split-1]
        num_remove_per_step = diff//num_split
        #stepping for inex
        print("Audio drop: ", diff)
        for i in range(audio_step, len(df_audio), audio_step):
            print("i:", i)
            extra =  1 if overflow >= 0 else 0
            overflow -= 1

            start = i - (num_remove_per_step + extra)
            range2drop = np.arange(start, i)

            diff -= len(range2drop)
            if diff <= 0:
                break
            #translate
            print(range2drop)
            # df_audio.drop(range2drop, inplace = True)
            #should be log message here
        print("truncateAudio Finished")

def truncateVideo(df_video, len_text, step = 1000):
    diff = len(df_video) - len_text
    print("reached truncated Video", diff)
    if diff <= 0:
        pass
    else:
        arr_seed = np.arange(0, len(df_video), step)
        num_rep = ceil(diff/len(arr_seed))
        diff_tile = np.tile(np.arange(num_rep), num_rep)
        arr_seed = arr_seed.repeat(num_rep)
        #combine
        indicies2drop = (arr_seed + diff_tile)
        #drop extras
        range_extra =  range(-1, num_rep * (len(arr_seed) - diff),-num_rep)
        indicies2drop = np.delete(indicies2drop, range_extra)
        print("Video Drop:\n", "\n".join(indicies2drop))

        # df_video.drop(indicies2drop, inplace=True)
        #order of elimination can be eliminatedd
        # while (diff >= 0):
        #     for i in np.arange(0, len(df_video), step)[::-1]:
        #         df_video.drop(i, inplace=True)
        #         diff -= 1