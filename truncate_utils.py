import pandas as pd
import numpy as np
from utils import printFull
import logging

def truncateAndCombineAll(df_text, df_audio, df_video):
    truncateAudio(df_audio)
    # truncateVideo(df_video)
    #get the minimal length
    min_len = min([len(x) for x in [df_video, df_audio, df_text]])

    df_audio = df_audio.iloc[:, 1:]
    df_video = df_video.iloc[:, 1:]
    df_text = df_text.iloc[:, 1:]

    #concatenate
    combined = pd.concat([df_video, df_audio, df_text], axis=1)
    #trucate to the shortest df
    combined = combined.iloc[:min_len, :]
    return combined

def truncateAudio(df_audio, num_split = 5):
    audio_step = len(df_audio)//num_split
    #stepping for inex
    FLAG_INDEX = [1,3] #deleting on 2nd and 4th place
    flag = 0
    #should be log message here
    for i in range(audio_step, len(df_audio), audio_step):
        if flag in FLAG_INDEX:
            logging.info("Audio dropping row#: "+str(i))
            df_audio.drop(i, axis = 0, inplace = True)
        flag += 1

def truncateVideo(df_video, step = 1000):
    indicies2drop = range(step-1, len(df_video), step)
    df_video = df_video.drop(pd.Index(indicies2drop), axis=0, inplace=True)