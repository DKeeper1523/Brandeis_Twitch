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
        #stepping for inex
        FLAG_INDEX = [1,3] #deleting on 2nd and 4th place
        flag = 0
        print("Audio drop: ", diff) 
        #should be log message here
        for i in range(audio_step, len(df_audio), audio_step):
            if flag in FLAG_INDEX:
                print(flag)
                # df_audio.drop(i, inplace = True)
            flag += 1
        print("truncateAudio Finished")

def truncateVideo(df_video, len_text, step = 1000):
    diff = len(df_video) - len_text
    print("reached truncated Video", diff)
    if diff <= 0:
        pass
    else:
        indicies2drop = range(step, len(df_video), step)
        print("Video Drop:\n", "\n".join(indicies2drop))
